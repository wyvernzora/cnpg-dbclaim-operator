/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package postgres contains idempotent SQL primitives for managing databases,
// schemas, extensions, roles, and grants.
//
// All operations assume the connection is opened with superuser credentials.
// Every identifier passed in is validated via ValidateIdentifier before being
// spliced (with safe quoting) into the generated SQL.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
)

// SQLSTATE codes we trap to make CREATE statements idempotent under races.
// See https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	sqlstateDuplicateObject   = "42710" // role already exists, etc.
	sqlstateDuplicateDatabase = "42P04"
)

func isAlreadyExists(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

// EnsureDatabase creates the database if it does not exist. CREATE DATABASE
// cannot run in a transaction; the race between SELECT and CREATE is closed by
// trapping SQLSTATE 42P04 (duplicate_database).
func EnsureDatabase(ctx context.Context, conn *pgx.Conn, name string) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	stmt := fmt.Sprintf("CREATE DATABASE %s", Quote(name))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		if isAlreadyExists(err, sqlstateDuplicateDatabase) {
			return nil
		}
		return fmt.Errorf("create database %q: %w", name, err)
	}
	return nil
}

// TerminateBackends terminates all sessions connected to the named database
// other than the caller's. Required before DROP DATABASE.
func TerminateBackends(ctx context.Context, conn *pgx.Conn, name string) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	stmt := "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()"
	if _, err := conn.Exec(ctx, stmt, name); err != nil {
		return fmt.Errorf("terminate backends on %q: %w", name, err)
	}
	return nil
}

// DropDatabase drops the database if it exists. Caller is responsible for
// having terminated active backends first.
func DropDatabase(ctx context.Context, conn *pgx.Conn, name string) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	stmt := fmt.Sprintf("DROP DATABASE IF EXISTS %s", Quote(name))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("drop database %q: %w", name, err)
	}
	return nil
}

// LockDownPublic revokes default public access to the public schema in the
// current database. Run after connecting to the target database.
//
// The REVOKEs update the public schema's ACL and so wait on the pg_namespace
// row a session this operator does not control can be holding — a tenant that
// owns the database and left a transaction open after its own
// GRANT ... ON SCHEMA public does exactly that. This runs on every reconcile of
// every claim and the reconciler runs one worker with no deadline, so the wait
// is bounded by the same lock_timeout the extension install uses: it gives up
// with SQLSTATE 55P03, the claim gets a Warning event, and the requeue retries
// once the other session is gone. Unbounded, one tenant's open transaction
// would park every claim.
func LockDownPublic(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lock down public: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := boundLockWait(ctx, tx); err != nil {
		return err
	}
	stmts := []string{
		"REVOKE ALL ON SCHEMA public FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
	}
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s); err != nil {
			return fmt.Errorf("lock down public (%s): %w", s, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("lock down public: %w", err)
	}
	return nil
}

// EnsureSchema runs CREATE SCHEMA IF NOT EXISTS for the named schema in the
// current database.
func EnsureSchema(ctx context.Context, conn *pgx.Conn, name string) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", Quote(name))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("create schema %q: %w", name, err)
	}
	return nil
}

// ExtensionRelocationError reports that an extension exists in the wrong
// schema and could not be moved. The usual cause is a non-relocatable
// extension, whose control file forbids SET SCHEMA outright.
type ExtensionRelocationError struct {
	Extension string
	From      string
	To        string
	Err       error
}

func (e *ExtensionRelocationError) Error() string {
	return fmt.Sprintf("relocate extension %q from schema %q to schema %q: %v", e.Extension, e.From, e.To, e.Err)
}

func (e *ExtensionRelocationError) Unwrap() error { return e.Err }

// ExtensionSchemaNotExclusiveError reports that a schema the extension's
// install script will run with on its search_path can be written to by a role
// other than the provisioning superuser, so running that script is unsafe.
// Schema is the target schema, or — when Prerequisite is set — the schema
// holding an extension this one requires, which Postgres puts on the script's
// search_path alongside the target.
type ExtensionSchemaNotExclusiveError struct {
	Extension    string
	Schema       string
	Prerequisite string
}

func (e *ExtensionSchemaNotExclusiveError) Error() string {
	if e.Prerequisite != "" {
		return fmt.Sprintf(
			"refusing to install extension %q: it requires extension %q, which lives in schema %q, and CREATE EXTENSION runs the install script as superuser "+
				"with search_path set to the target schema plus every prerequisite's schema. Schema %q is owned by, or grants CREATE to, a role other than the provisioning superuser. "+
				"Relocate %q to a schema no RoleClaim owns or holds CREATE on, or return that ownership or CREATE grant to the superuser for the install",
			e.Extension, e.Prerequisite, e.Schema, e.Schema, e.Prerequisite)
	}
	return fmt.Sprintf(
		"refusing to install extension %q into schema %q: the schema is owned by, or grants CREATE to, a role other than the provisioning superuser, "+
			"and CREATE EXTENSION runs the extension's install script as superuser with search_path set to that schema. "+
			"Install the extension before the schema or its database is handed to a tenant role, or target a schema in spec.schemas "+
			"that no RoleClaim owns or holds CREATE on",
		e.Extension, e.Schema)
}

// EnsureExtension runs CREATE EXTENSION IF NOT EXISTS for the named extension
// in the current database, converging its schema on the requested one. Version
// updates are out of scope.
//
// The schema is required: every Postgres extension installs into exactly one
// schema, so the claim names it. A first install refuses any schema the install
// script will see — the target, or a prerequisite extension's — that a role
// other than this session's can write to; see createExtension.
//
// The schema is a declared desired state, so current placement is read back —
// CREATE EXTENSION IF NOT EXISTS silently ignores its SCHEMA clause when the
// extension already exists — and drift is corrected with ALTER EXTENSION ...
// SET SCHEMA, the same convergence CloudNativePG's own Database CRD performs.
// A non-relocatable extension makes that statement fail loudly rather than
// silently leaving the claim's stated intent unmet.
//
// An empty schema is unreachable through admission — spec.extensions[].schema
// is required — so it can only mean a claim still stored in the v0.3.x
// bare-string shape, decoded through the legacy path. Refuse it with the
// migration pointer before touching the connection.
func EnsureExtension(ctx context.Context, conn *pgx.Conn, name, schema string) error {
	if schema == "" {
		return fmt.Errorf(
			"extension %q has no schema: spec.extensions[].schema is required; "+
				"claims written by operator v0.3.x or earlier must be migrated — "+
				`see "Upgrading from v0.3.x" in the README`, name)
	}
	createStmt, err := ensureExtensionStmt(name, schema)
	if err != nil {
		return err
	}
	actual, installed, err := extensionSchema(ctx, conn, name)
	if err != nil {
		return err
	}
	if !installed {
		return createExtension(ctx, conn, name, schema, createStmt)
	}
	if actual == schema {
		return nil
	}
	if _, err := conn.Exec(ctx, alterExtensionSchemaStmt(name, schema)); err != nil {
		return &ExtensionRelocationError{Extension: name, From: actual, To: schema, Err: err}
	}
	return nil
}

// createExtension installs an extension that is not present yet, refusing any
// schema the install script will see that a role other than the provisioning
// superuser can write to.
//
// CREATE EXTENSION runs the extension's install script as superuser with
// search_path set to the target schema *plus the schema of every extension the
// control file requires*, so an object a tenant planted in any of them can
// capture a name that script resolves. All of them are checked, and the target
// is then pinned for the CREATE — so the schema that was checked is the schema
// the install script creates in, regardless of what session search_path a
// tenant that owns its database has arranged (ALTER DATABASE ... SET
// search_path, or a CloudNativePG per-database/per-role setting). The pin does
// not remove the prerequisite schemas from the script's path, which is why
// checking the target alone is not enough.
//
// Every schema the check covers is locked for the rest of the transaction
// before it is read (lockSchemaOwnership), so a handover cannot slip in between
// the check and the CREATE that trusts it. Which schemas the check covers is
// itself read from the catalog, so the set is frozen first
// (lockExtensionCatalog) — otherwise a prerequisite installed after the read
// puts an unchecked schema on the install script's path.
//
// Every one of those locks conflicts with ordinary tenant DDL, so every one of
// them can be made to wait by a session this operator does not control: a
// tenant that leaves a transaction open after CREATE EXTENSION holds
// RowExclusiveLock on pg_extension, and a tenant that owns its database can
// hold the pg_namespace row of its own schema. The reconciler runs one worker
// with no deadline, so an unbounded wait here stops every other claim from
// being reconciled at all. lock_timeout bounds it: the install fails with a
// message naming the lock, the claim gets a Warning event, and the requeue
// retries once the other session is gone.
func createExtension(ctx context.Context, conn *pgx.Conn, name, schema, createStmt string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin extension install: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := boundLockWait(ctx, tx); err != nil {
		return err
	}
	if err := lockExtensionCatalog(ctx, tx); err != nil {
		return err
	}
	required, requiredSchemas, err := extensionPrerequisites(ctx, tx, name)
	if err != nil {
		return err
	}
	if err := lockSchemaOwnership(ctx, tx, append([]string{schema}, requiredSchemas...)); err != nil {
		return err
	}

	exclusive, err := schemaExclusiveToCurrentUser(ctx, tx, schema)
	if err != nil {
		return err
	}
	if !exclusive {
		return &ExtensionSchemaNotExclusiveError{Extension: name, Schema: schema}
	}

	for i, requiredSchema := range requiredSchemas {
		if requiredSchema == schema {
			continue
		}
		exclusive, err := schemaExclusiveToCurrentUser(ctx, tx, requiredSchema)
		if err != nil {
			return err
		}
		if !exclusive {
			return &ExtensionSchemaNotExclusiveError{Extension: name, Schema: requiredSchema, Prerequisite: required[i]}
		}
	}

	if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+Quote(schema)); err != nil {
		return fmt.Errorf("pin search_path for extension %q: %w", name, err)
	}
	if _, err := tx.Exec(ctx, createStmt); err != nil {
		return fmt.Errorf("create extension %q: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("create extension %q: %w", name, err)
	}
	return nil
}

func ensureExtensionStmt(name, schema string) (string, error) {
	if err := ValidateIdentifier(name); err != nil {
		return "", err
	}
	if err := ValidateIdentifier(schema); err != nil {
		return "", err
	}
	return fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s SCHEMA %s", Quote(name), Quote(schema)), nil
}

// alterExtensionSchemaStmt builds the relocation statement. Both identifiers
// have already been validated by ensureExtensionStmt on the same call path.
func alterExtensionSchemaStmt(name, schema string) string {
	return fmt.Sprintf("ALTER EXTENSION %s SET SCHEMA %s", Quote(name), Quote(schema))
}

// extensionSchema returns the schema an installed extension's objects live in,
// and whether the extension is installed at all.
func extensionSchema(ctx context.Context, conn *pgx.Conn, name string) (string, bool, error) {
	var schema string
	err := readCatalog(ctx, conn,
		`SELECT n.nspname FROM pg_catalog.pg_extension e
		 JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
		 WHERE e.extname = $1`,
		[]any{name}, &schema)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("look up schema of extension %q: %w", name, err)
	}
	return schema, true, nil
}

// extensionPrerequisites returns the extensions the named extension directly
// requires, paired with the schema each one is installed in (parallel slices;
// the pairing is what the refusal message needs).
//
// Postgres runs the install script with search_path set to the target schema
// plus these schemas, so each is exposed to that script exactly as the target
// is. A requirement that is not installed yet has no schema to expose and is
// left out — CREATE EXTENSION then fails on the missing requirement itself,
// loudly. Only direct requirements are listed, matching what Postgres puts on
// the path.
//
// Every version's requirements count, not just the default version's. Reaching
// the default version can mean running a base script and then a chain of update
// scripts, and Postgres builds each script's search_path from the `requires` of
// the version *that script* belongs to (its secondary control file). An
// extension whose default version requires nothing can therefore still be
// installed through a script that sees a prerequisite's schema. Taking the
// union over all available versions can name a version that is not on this
// install's path, which only ever over-refuses — loudly, and with the
// prerequisite in the message.
func extensionPrerequisites(ctx context.Context, tx pgx.Tx, name string) ([]string, []string, error) {
	var names, schemas []string
	err := readCatalogTx(ctx, tx,
		`WITH required AS (
		     SELECT DISTINCT pg_catalog.unnest(av.requires) AS name
		     FROM pg_catalog.pg_available_extension_versions av
		     WHERE av.name = $1)
		 SELECT COALESCE(array_agg(e.extname::text), '{}'::text[]),
		        COALESCE(array_agg(n.nspname::text), '{}'::text[])
		 FROM required
		 JOIN pg_catalog.pg_extension e ON e.extname = required.name
		 JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace`,
		[]any{name}, &names, &schemas)
	if err != nil {
		return nil, nil, fmt.Errorf("look up prerequisites of extension %q: %w", name, err)
	}
	return names, schemas, nil
}

// lockWaitTimeout bounds how long a reconcile waits for any one lock. The
// operator's own transactions hold them for milliseconds, so a wait this long
// means another session is sitting on the catalog; failing is then better than
// parking the single reconcile worker behind it.
const lockWaitTimeout = 10 * time.Second

// boundLockWait caps every lock wait in the transaction, so a session this
// operator does not control cannot hold the reconcile worker indefinitely.
// A wait that hits the cap aborts the statement with SQLSTATE 55P03, which
// surfaces through the wrapping message of whichever lock was waiting.
//
// Open sets the same bound as a session default on every connection the
// operator makes, so this repeats it for the transaction rather than
// establishing it — it keeps the bound with the code that depends on it, and
// covers a connection opened without Open.
func boundLockWait(ctx context.Context, tx pgx.Tx) error {
	stmt := fmt.Sprintf("SET LOCAL lock_timeout = %d", lockWaitTimeout.Milliseconds())
	if _, err := tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("bound lock wait: %w", err)
	}
	return nil
}

// lockExtensionCatalog blocks concurrent extension DDL in this database for the
// rest of the transaction.
//
// Locking the schemas the guard checks is not enough, because *which* schemas
// it checks is itself catalog data: extensionPrerequisites reads which of the
// required extensions are installed and where. An extension that is not
// installed when that read runs has no row to lock, so a concurrent
// CREATE EXTENSION cube SCHEMA tenant landing between the read and this
// transaction's own CREATE adds a schema to the install script's search_path
// that was never checked — and CREATE EXTENSION then runs that script as
// superuser with a tenant's schema on the path. ALTER EXTENSION ... SET SCHEMA
// moves an already-counted prerequisite out from under the check the same way.
//
// Postgres inserts the pg_extension row before executing the install script,
// and that insert takes RowExclusiveLock on the catalog, so holding
// ShareRowExclusiveLock on pg_extension makes every such writer wait until this
// install has committed. The mode conflicts with writers only: readers of
// pg_extension take AccessShareLock and are unaffected, and the lock is on this
// database's copy of the catalog, so other databases are untouched.
func lockExtensionCatalog(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "LOCK TABLE pg_catalog.pg_extension IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		return fmt.Errorf("lock extension catalog: %w", err)
	}
	return nil
}

// lockSchemaOwnership takes row locks on the catalog rows schemaExclusiveToCurrentUser
// reads — the pg_namespace row of each schema, and the current database's
// pg_database row, which is what pg_database_owner resolves through — and holds
// them for the rest of the transaction.
//
// Without them the check and the CREATE that trusts it are two statements at
// Read Committed with nothing between them: ALTER SCHEMA ... OWNER TO, GRANT
// CREATE ON SCHEMA and ALTER DATABASE ... OWNER TO (all of which this operator
// itself issues while applying a RoleClaim) can land in that window, and the
// install script then runs as superuser in a schema a tenant can write to. The
// locks are on the catalog rows those statements update, so they block any
// writer, not only one that agreed to a protocol.
//
// Schemas are locked in sorted order so two concurrent installs sharing schemas
// cannot deadlock. A schema that does not exist locks nothing; the check then
// reports it as not exclusive, which is the answer the caller wants anyway.
func lockSchemaOwnership(ctx context.Context, tx pgx.Tx, schemas []string) error {
	// Same reason readCatalog pins: these statements carry unqualified
	// operators a tenant-planted schema ahead of pg_catalog could capture.
	if _, err := tx.Exec(ctx, "SET LOCAL search_path TO pg_catalog"); err != nil {
		return fmt.Errorf("pin search_path for ownership locks: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"SELECT 1 FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database() FOR UPDATE"); err != nil {
		return fmt.Errorf("lock database owner row: %w", err)
	}
	ordered := slices.Compact(slices.Sorted(slices.Values(schemas)))
	for _, schema := range ordered {
		if _, err := tx.Exec(ctx,
			"SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1 FOR UPDATE", schema); err != nil {
			return fmt.Errorf("lock ownership of schema %q: %w", schema, err)
		}
	}
	return nil
}

// schemaExclusiveToCurrentUser reports whether the named schema is owned by the
// session's own role and grants CREATE to nobody else — that is, whether this
// operator is the only writer. A missing schema counts as not exclusive; the
// caller's error then says so rather than a CREATE failing obscurely.
//
// Ownership by the implicit pg_database_owner role is resolved to the database's
// actual owner. That is how the public schema is owned from PostgreSQL 14 on, so
// comparing the raw OID would refuse "public" on a database this operator still
// owns, and would accept it once ownership had been handed to a tenant — the
// answer wrong in both directions. CREATE grants are resolved the same way.
func schemaExclusiveToCurrentUser(ctx context.Context, tx pgx.Tx, schema string) (bool, error) {
	var exclusive bool
	err := readCatalogTx(ctx, tx,
		`WITH me AS (SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = current_user),
		      db AS (SELECT d.datdba FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()),
		      implicit AS (SELECT pg_catalog.to_regrole('pg_database_owner')::oid AS oid),
		      ns AS (SELECT n.nspowner, n.nspacl FROM pg_catalog.pg_namespace n WHERE n.nspname = $1)
		 SELECT (CASE WHEN ns.nspowner = implicit.oid THEN db.datdba ELSE ns.nspowner END) = me.oid
		    AND NOT EXISTS (
		          SELECT 1
		          FROM pg_catalog.aclexplode(COALESCE(ns.nspacl, pg_catalog.acldefault('n', ns.nspowner))) a
		          WHERE a.privilege_type = 'CREATE'
		            AND (CASE WHEN a.grantee = implicit.oid THEN db.datdba ELSE a.grantee END) <> me.oid)
		 FROM ns, me, db, implicit`,
		[]any{schema}, &exclusive)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("look up ownership of schema %q: %w", schema, err)
	}
	return exclusive, nil
}

// readCatalog runs a single-row catalog query with search_path pinned to
// pg_catalog for its duration.
//
// The operator does not control the session's default search_path: CNPG lets a
// cluster set one per database or per role, and a tenant that owns its database
// can set one too. Without the pin, an unqualified name in a catalog read —
// including operators and functions, which schema qualification alone does not
// cover — can be captured by a relation a tenant planted in a schema listed
// ahead of pg_catalog, and this session resolves it as superuser.
func readCatalog(ctx context.Context, conn *pgx.Conn, sql string, args []any, dest ...any) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin catalog read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return readCatalogTx(ctx, tx, sql, args, dest...)
}

// readCatalogTx is readCatalog for a caller that already holds a transaction.
// The pin lasts until that transaction ends or a later SET LOCAL replaces it.
func readCatalogTx(ctx context.Context, tx pgx.Tx, sql string, args []any, dest ...any) error {
	if _, err := tx.Exec(ctx, "SET LOCAL search_path TO pg_catalog"); err != nil {
		return fmt.Errorf("pin search_path for catalog read: %w", err)
	}
	return tx.QueryRow(ctx, sql, args...).Scan(dest...)
}

// EnsureRole creates the role if missing, then aligns LOGIN, PASSWORD, and
// CONNECTION LIMIT with the supplied values. Idempotent.
//
// connectionLimit of -1 means unlimited.
func EnsureRole(ctx context.Context, conn *pgx.Conn, name, password string, connectionLimit int32) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	createStmt := fmt.Sprintf("CREATE ROLE %s LOGIN", Quote(name))
	if _, err := conn.Exec(ctx, createStmt); err != nil && !isAlreadyExists(err, sqlstateDuplicateObject) {
		return fmt.Errorf("create role %q: %w", name, err)
	}
	alterStmt := fmt.Sprintf(
		"ALTER ROLE %s WITH LOGIN PASSWORD %s CONNECTION LIMIT %d",
		Quote(name), pq.QuoteLiteral(password), connectionLimit,
	)
	if _, err := conn.Exec(ctx, alterStmt); err != nil {
		return fmt.Errorf("alter role %q: %w", name, err)
	}
	return nil
}

// DropRole reassigns/drops owned objects, then drops the role.
func DropRole(ctx context.Context, conn *pgx.Conn, name, reassignTo string) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	if err := ValidateIdentifier(reassignTo); err != nil {
		return err
	}
	stmts := []string{
		fmt.Sprintf("REASSIGN OWNED BY %s TO %s", Quote(name), Quote(reassignTo)),
		fmt.Sprintf("DROP OWNED BY %s", Quote(name)),
		fmt.Sprintf("DROP ROLE IF EXISTS %s", Quote(name)),
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			// REASSIGN/DROP OWNED on a non-existent role fail with
			// undefined_object (42704); swallow so DropRole is idempotent.
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42704" {
				continue
			}
			return fmt.Errorf("drop role %q (%s): %w", name, s, err)
		}
	}
	return nil
}

// GrantConnect grants CONNECT on the database to the role.
func GrantConnect(ctx context.Context, conn *pgx.Conn, dbname, role string) error {
	if err := ValidateIdentifier(dbname); err != nil {
		return err
	}
	if err := ValidateIdentifier(role); err != nil {
		return err
	}
	stmt := fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", Quote(dbname), Quote(role))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("grant connect on %q to %q: %w", dbname, role, err)
	}
	return nil
}

// AlterDatabaseOwner transfers ownership of the database to the role.
func AlterDatabaseOwner(ctx context.Context, conn *pgx.Conn, dbname, owner string) error {
	if err := ValidateIdentifier(dbname); err != nil {
		return err
	}
	if err := ValidateIdentifier(owner); err != nil {
		return err
	}
	stmt := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", Quote(dbname), Quote(owner))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("alter database owner %q -> %q: %w", dbname, owner, err)
	}
	return nil
}

// AlterSchemaOwner transfers ownership of a schema in the current database.
func AlterSchemaOwner(ctx context.Context, conn *pgx.Conn, schema, owner string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(owner); err != nil {
		return err
	}
	stmt := fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", Quote(schema), Quote(owner))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("alter schema owner %q -> %q: %w", schema, owner, err)
	}
	return nil
}

// AccessLevel mirrors v1alpha1.AccessLevel without an import cycle.
type AccessLevel string

const (
	AccessOwner     AccessLevel = "Owner"
	AccessReadWrite AccessLevel = "ReadWrite"
	AccessReadOnly  AccessLevel = "ReadOnly"
)

// ApplySchemaGrants runs the GRANT statements appropriate to the given access
// level for one (role, schema) pair. Idempotent.
//
// Owner is handled separately via AlterSchemaOwner; this function only handles
// the non-ownership grants (USAGE, table/sequence privileges).
func ApplySchemaGrants(ctx context.Context, conn *pgx.Conn, role, schema string, access AccessLevel) error {
	if err := ValidateIdentifier(role); err != nil {
		return err
	}
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	stmts := []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", Quote(schema), Quote(role)),
	}
	switch access {
	case AccessOwner:
		// USAGE only; ownership transfer happens via AlterSchemaOwner.
	case AccessReadWrite:
		stmts[0] = fmt.Sprintf("GRANT USAGE, CREATE ON SCHEMA %s TO %s", Quote(schema), Quote(role))
		stmts = append(stmts,
			fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON ALL TABLES IN SCHEMA %s TO %s", Quote(schema), Quote(role)),
			fmt.Sprintf("GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA %s TO %s", Quote(schema), Quote(role)),
		)
	case AccessReadOnly:
		stmts = append(stmts,
			fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s", Quote(schema), Quote(role)),
			fmt.Sprintf("GRANT SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s", Quote(schema), Quote(role)),
		)
	default:
		return fmt.Errorf("unknown access level %q", access)
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("grant for (%s, %s, %s) [%s]: %w", role, schema, access, s, err)
		}
	}
	return nil
}

// RevokeAllOnSchema removes a role's grants on a schema and its objects. Used
// when a RoleClaim narrows its schemas[] and a previously granted schema must
// be released.
func RevokeAllOnSchema(ctx context.Context, conn *pgx.Conn, role, schema string) error {
	if err := ValidateIdentifier(role); err != nil {
		return err
	}
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	stmts := []string{
		fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s", Quote(schema), Quote(role)),
		fmt.Sprintf("REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s", Quote(schema), Quote(role)),
		fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM %s", Quote(schema), Quote(role)),
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("revoke (%s, %s) [%s]: %w", role, schema, s, err)
		}
	}
	return nil
}

// AlterDefaultPrivilegesGrant registers default privileges so that future
// tables/sequences created by `writer` in `schema` receive the requested access
// for `reader`. Idempotent.
func AlterDefaultPrivilegesGrant(ctx context.Context, conn *pgx.Conn, writer, reader, schema string, access AccessLevel) error {
	if err := ValidateIdentifier(writer); err != nil {
		return err
	}
	if err := ValidateIdentifier(reader); err != nil {
		return err
	}
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	tablePrivs, sequencePrivs, err := defaultPrivileges(access)
	if err != nil {
		return err
	}
	stmts := []string{
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s GRANT %s ON TABLES TO %s",
			Quote(writer), Quote(schema), tablePrivs, Quote(reader)),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s GRANT %s ON SEQUENCES TO %s",
			Quote(writer), Quote(schema), sequencePrivs, Quote(reader)),
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("alter default privileges (%s, %s, %s, %s) [%s]: %w", writer, schema, reader, access, s, err)
		}
	}
	return nil
}

// AlterDefaultPrivilegesRevoke is the inverse of
// AlterDefaultPrivilegesGrant. Idempotent — Postgres tolerates revokes of
// grants that don't exist.
func AlterDefaultPrivilegesRevoke(ctx context.Context, conn *pgx.Conn, writer, reader, schema string, access AccessLevel) error {
	if err := ValidateIdentifier(writer); err != nil {
		return err
	}
	if err := ValidateIdentifier(reader); err != nil {
		return err
	}
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	tablePrivs, sequencePrivs, err := defaultPrivileges(access)
	if err != nil {
		return err
	}
	stmts := []string{
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s REVOKE %s ON TABLES FROM %s",
			Quote(writer), Quote(schema), tablePrivs, Quote(reader)),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s REVOKE %s ON SEQUENCES FROM %s",
			Quote(writer), Quote(schema), sequencePrivs, Quote(reader)),
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("revoke default privileges (%s, %s, %s, %s) [%s]: %w", writer, schema, reader, access, s, err)
		}
	}
	return nil
}

// AlterDefaultPrivilegesGrantSelect is kept for tests and older callers that
// only need ReadOnly default privileges.
func AlterDefaultPrivilegesGrantSelect(ctx context.Context, conn *pgx.Conn, writer, reader, schema string) error {
	return AlterDefaultPrivilegesGrant(ctx, conn, writer, reader, schema, AccessReadOnly)
}

// AlterDefaultPrivilegesRevokeSelect is kept for tests and older callers that
// only need ReadOnly default privileges.
func AlterDefaultPrivilegesRevokeSelect(ctx context.Context, conn *pgx.Conn, writer, reader, schema string) error {
	return AlterDefaultPrivilegesRevoke(ctx, conn, writer, reader, schema, AccessReadOnly)
}

func defaultPrivileges(access AccessLevel) (string, string, error) {
	switch access {
	case AccessReadOnly:
		return "SELECT", "SELECT", nil
	case AccessReadWrite:
		return "SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES", "USAGE, SELECT, UPDATE", nil
	default:
		return "", "", fmt.Errorf("unsupported default privilege access level %q", access)
	}
}
