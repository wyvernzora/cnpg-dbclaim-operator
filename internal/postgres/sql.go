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
func LockDownPublic(ctx context.Context, conn *pgx.Conn) error {
	stmts := []string{
		"REVOKE ALL ON SCHEMA public FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("lock down public (%s): %w", s, err)
		}
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

// EnsureExtension runs CREATE EXTENSION IF NOT EXISTS for the named extension
// in the current database. Version updates are out of scope.
func EnsureExtension(ctx context.Context, conn *pgx.Conn, name string) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	stmt := fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", Quote(name))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("create extension %q: %w", name, err)
	}
	return nil
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
