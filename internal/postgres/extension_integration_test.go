/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
)

// TestEnsureExtensionSchema exercises the schema-targeted extension install
// against a real Postgres. Each subtest runs in its own scratch database
// because an extension can only be installed once per database.
func TestEnsureExtensionSchema(t *testing.T) {
	dsn := os.Getenv("CNPG_DBCLAIM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set CNPG_DBCLAIM_POSTGRES_DSN to run extension schema integration tests against a real Postgres")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer admin.Close(context.Background())

	t.Run("installs the extension into the requested schema", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureSchema(ctx, conn, "releases"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		if err := EnsureExtension(ctx, conn, "pg_trgm", "releases"); err != nil {
			t.Fatalf("ensure extension: %v", err)
		}
		if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "releases" {
			t.Fatalf("pg_trgm installed in schema %q, want %q", got, "releases")
		}
		// The whole point: operator classes resolve under a pinned search_path.
		if _, err := conn.Exec(ctx, "SET search_path TO releases"); err != nil {
			t.Fatalf("set search_path: %v", err)
		}
		if _, err := conn.Exec(ctx, "CREATE TABLE releases.t (title text)"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if _, err := conn.Exec(ctx, "CREATE INDEX t_title_trgm ON releases.t USING gist (title gist_trgm_ops)"); err != nil {
			t.Fatalf("create gist trigram index under pinned search_path: %v", err)
		}
	})

	t.Run("is idempotent when the extension is already in the requested schema", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureSchema(ctx, conn, "releases"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		for i := range 2 {
			if err := EnsureExtension(ctx, conn, "pg_trgm", "releases"); err != nil {
				t.Fatalf("ensure extension (pass %d): %v", i, err)
			}
		}
	})

	t.Run("relocates an extension that already exists in another schema", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureExtension(ctx, conn, "pg_trgm", "public"); err != nil {
			t.Fatalf("seed extension: %v", err)
		}
		if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "public" {
			t.Fatalf("seeded pg_trgm in schema %q, want %q", got, "public")
		}
		if err := EnsureSchema(ctx, conn, "releases"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}

		if err := EnsureExtension(ctx, conn, "pg_trgm", "releases"); err != nil {
			t.Fatalf("ensure extension: %v", err)
		}
		if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "releases" {
			t.Fatalf("pg_trgm in schema %q after convergence, want %q", got, "releases")
		}
		// Relocation is only useful if the objects moved with it.
		if _, err := conn.Exec(ctx, "SET search_path TO releases"); err != nil {
			t.Fatalf("set search_path: %v", err)
		}
		if _, err := conn.Exec(ctx, "CREATE TABLE releases.t (title text)"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if _, err := conn.Exec(ctx, "CREATE INDEX t_title_trgm ON releases.t USING gist (title gist_trgm_ops)"); err != nil {
			t.Fatalf("create gist trigram index after relocation: %v", err)
		}
	})

	// xml2 declares relocatable = false in its control file, so Postgres
	// refuses SET SCHEMA on it. It is the cheapest real non-relocatable
	// fixture: no custom control file, ships with contrib.
	t.Run("surfaces a relocation that Postgres refuses", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureSchema(ctx, conn, "pinned"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		if err := EnsureExtension(ctx, conn, "xml2", "pinned"); err != nil {
			t.Fatalf("seed extension: %v", err)
		}
		if err := EnsureSchema(ctx, conn, "releases"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}

		err := EnsureExtension(ctx, conn, "xml2", "releases")
		var relocation *ExtensionRelocationError
		if !errors.As(err, &relocation) {
			t.Fatalf("EnsureExtension: got err %v, want *ExtensionRelocationError", err)
		}
		if relocation.Extension != "xml2" || relocation.From != "pinned" || relocation.To != "releases" {
			t.Fatalf("relocation = %+v, want {xml2 pinned releases}", *relocation)
		}
		if !strings.Contains(relocation.Error(), "does not support SET SCHEMA") {
			t.Fatalf("relocation error %q does not carry the Postgres reason", relocation.Error())
		}
		if got := installedExtensionSchema(ctx, t, conn, "xml2"); got != "pinned" {
			t.Fatalf("xml2 in schema %q after a refused relocation, want it left at %q", got, "pinned")
		}
	})
}

// TestEnsureExtensionCatalogReadIsNotCapturable plants a lying pg_extension
// relation in a schema and puts that schema ahead of pg_catalog on the session
// search_path — what a tenant with a database-level or role-level search_path
// (CloudNativePG exposes both) can arrange. The read-back that drives
// convergence must resolve the real catalog regardless.
func TestEnsureExtensionCatalogReadIsNotCapturable(t *testing.T) {
	ctx, admin := integrationConn(t)

	conn := scratchDatabase(ctx, t, admin)
	if err := EnsureExtension(ctx, conn, "pg_trgm", "public"); err != nil {
		t.Fatalf("seed extension: %v", err)
	}
	for _, schema := range []string{"evil", "releases"} {
		if err := EnsureSchema(ctx, conn, schema); err != nil {
			t.Fatalf("ensure schema %q: %v", schema, err)
		}
	}
	// Two captures, because schema-qualifying relation names only defeats the
	// first: a relation that reports every extension as already living in
	// "releases", and an equality operator that matches nothing.
	if _, err := conn.Exec(ctx, `CREATE VIEW evil.pg_extension AS
		SELECT e.oid, e.extname, (SELECT n.oid FROM pg_catalog.pg_namespace n WHERE n.nspname = 'releases') AS extnamespace
		FROM pg_catalog.pg_extension e`); err != nil {
		t.Fatalf("plant catalog view: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE FUNCTION evil.never_equal(name, name) RETURNS boolean
		AS 'SELECT false' LANGUAGE sql IMMUTABLE`); err != nil {
		t.Fatalf("plant operator function: %v", err)
	}
	if _, err := conn.Exec(ctx,
		`CREATE OPERATOR evil.= (LEFTARG = name, RIGHTARG = name, FUNCTION = evil.never_equal)`); err != nil {
		t.Fatalf("plant operator: %v", err)
	}
	if _, err := conn.Exec(ctx, "SET search_path TO evil, pg_catalog"); err != nil {
		t.Fatalf("set hostile search_path: %v", err)
	}

	if err := EnsureExtension(ctx, conn, "pg_trgm", "releases"); err != nil {
		t.Fatalf("ensure extension: %v", err)
	}

	// Assert outside the trap: the planted operator would capture this
	// verification query too, and a test cannot check a lie with a lie.
	if _, err := conn.Exec(ctx, "SET search_path TO pg_catalog"); err != nil {
		t.Fatalf("restore search_path: %v", err)
	}
	if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "releases" {
		t.Fatalf("pg_trgm in schema %q; the planted evil.pg_extension was believed over the real catalog", got)
	}
}

// TestEnsureExtensionRefusesSharedSchema covers the other half of the same
// exposure: CREATE EXTENSION runs the extension's install script as superuser
// with search_path set to the target schema, so a schema a tenant role can
// write to is not a safe install target.
func TestEnsureExtensionRefusesSharedSchema(t *testing.T) {
	ctx, admin := integrationConn(t)

	t.Run("refuses a schema another role holds CREATE on", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureSchema(ctx, conn, "releases"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		if err := EnsureRole(ctx, conn, "tenant_rw", "pw", -1); err != nil {
			t.Fatalf("ensure role: %v", err)
		}
		if err := ApplySchemaGrants(ctx, conn, "tenant_rw", "releases", AccessReadWrite); err != nil {
			t.Fatalf("apply grants: %v", err)
		}

		err := EnsureExtension(ctx, conn, "pg_trgm", "releases")
		var refusal *ExtensionSchemaNotExclusiveError
		if !errors.As(err, &refusal) {
			t.Fatalf("EnsureExtension: got err %v, want *ExtensionSchemaNotExclusiveError", err)
		}
		if refusal.Extension != "pg_trgm" || refusal.Schema != "releases" {
			t.Fatalf("refusal = %+v, want {pg_trgm releases}", *refusal)
		}
	})

	t.Run("refuses a schema owned by another role", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureSchema(ctx, conn, "releases"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		if err := EnsureRole(ctx, conn, "tenant_owner", "pw", -1); err != nil {
			t.Fatalf("ensure role: %v", err)
		}
		if err := AlterSchemaOwner(ctx, conn, "releases", "tenant_owner"); err != nil {
			t.Fatalf("alter schema owner: %v", err)
		}

		var refusal *ExtensionSchemaNotExclusiveError
		if err := EnsureExtension(ctx, conn, "pg_trgm", "releases"); !errors.As(err, &refusal) {
			t.Fatalf("EnsureExtension: got err %v, want *ExtensionSchemaNotExclusiveError", err)
		}
	})

	t.Run("allows a schema granted out without CREATE", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureSchema(ctx, conn, "releases"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		if err := EnsureRole(ctx, conn, "tenant_ro", "pw", -1); err != nil {
			t.Fatalf("ensure role: %v", err)
		}
		if err := ApplySchemaGrants(ctx, conn, "tenant_ro", "releases", AccessReadOnly); err != nil {
			t.Fatalf("apply grants: %v", err)
		}

		if err := EnsureExtension(ctx, conn, "pg_trgm", "releases"); err != nil {
			t.Fatalf("ensure extension: %v", err)
		}
		if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "releases" {
			t.Fatalf("pg_trgm installed in schema %q, want %q", got, "releases")
		}
	})

	// The case an explicit `schema: public` claim exercises: public is owned
	// by pg_database_owner from PostgreSQL 14 on, which resolves to this
	// operator while it still owns the database, so nothing about it is shared
	// and the install proceeds. Once an Owner RoleClaim hands the database
	// over, pg_database_owner resolves to the tenant and a new install into
	// public is refused by the same guard the two subtests above exercise.
	t.Run("allows public while the operator still owns the database", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := LockDownPublic(ctx, conn); err != nil {
			t.Fatalf("lock down public: %v", err)
		}

		if err := EnsureExtension(ctx, conn, "pg_trgm", "public"); err != nil {
			t.Fatalf("ensure extension into public: %v", err)
		}
		if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "public" {
			t.Fatalf("pg_trgm installed in schema %q, want %q", got, "public")
		}
	})

	// The next two subtests are a pair. CREATE EXTENSION does not run the
	// install script with only the target schema on search_path: Postgres
	// appends the schema of every extension the control file requires. The
	// first subtest fixes that mechanism in place — earthdistance installs into
	// "safe" while the cube type it needs lives only in "shared", which can
	// only work if Postgres put "shared" on the script's path. The second
	// proves the guard covers that schema too. Without the first, the second
	// would pass just as well against a guard that refuses for the wrong
	// reason.
	t.Run("installs a dependent extension whose prerequisite lives in another schema", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		for _, schema := range []string{"shared", "safe"} {
			if err := EnsureSchema(ctx, conn, schema); err != nil {
				t.Fatalf("ensure schema %q: %v", schema, err)
			}
		}
		if err := EnsureExtension(ctx, conn, "cube", "shared"); err != nil {
			t.Fatalf("seed prerequisite: %v", err)
		}
		// Pin the session away from both schemas, so nothing but Postgres's own
		// prerequisite handling can make the cube type visible to the script.
		if _, err := conn.Exec(ctx, "SET search_path TO pg_catalog"); err != nil {
			t.Fatalf("pin session search_path: %v", err)
		}

		if err := EnsureExtension(ctx, conn, "earthdistance", "safe"); err != nil {
			t.Fatalf("install dependent extension: %v", err)
		}
		if got := installedExtensionSchema(ctx, t, conn, "earthdistance"); got != "safe" {
			t.Fatalf("earthdistance installed in schema %q, want %q", got, "safe")
		}
	})

	t.Run("refuses a prerequisite schema another role owns", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		for _, schema := range []string{"shared", "safe"} {
			if err := EnsureSchema(ctx, conn, schema); err != nil {
				t.Fatalf("ensure schema %q: %v", schema, err)
			}
		}
		if err := EnsureExtension(ctx, conn, "cube", "shared"); err != nil {
			t.Fatalf("seed prerequisite: %v", err)
		}
		// The handover: an Owner RoleClaim takes the schema the prerequisite
		// was installed into. "safe" stays exclusive to the operator, so a
		// guard that only looks at the target schema lets the install through.
		if err := EnsureRole(ctx, conn, "tenant_owner", "pw", -1); err != nil {
			t.Fatalf("ensure role: %v", err)
		}
		if err := AlterSchemaOwner(ctx, conn, "shared", "tenant_owner"); err != nil {
			t.Fatalf("alter schema owner: %v", err)
		}

		err := EnsureExtension(ctx, conn, "earthdistance", "safe")
		var refusal *ExtensionSchemaNotExclusiveError
		if !errors.As(err, &refusal) {
			t.Fatalf("EnsureExtension: got err %v, want *ExtensionSchemaNotExclusiveError", err)
		}
		if refusal.Extension != "earthdistance" || refusal.Schema != "shared" || refusal.Prerequisite != "cube" {
			t.Fatalf("refusal = %+v, want {earthdistance shared cube}", *refusal)
		}
		var installed int
		if err := conn.QueryRow(ctx,
			"SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = 'earthdistance'").Scan(&installed); err != nil {
			t.Fatalf("count earthdistance: %v", err)
		}
		if installed != 0 {
			t.Fatal("earthdistance was installed despite the refusal")
		}
	})

	// The ordinary lifecycle: the DatabaseClaim installs the extension, an
	// Owner RoleClaim then takes the schema. Nothing is installed on later
	// reconciles, so they must stay clean.
	t.Run("keeps reconciling an extension installed before the handover", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureSchema(ctx, conn, "releases"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		if err := EnsureExtension(ctx, conn, "pg_trgm", "releases"); err != nil {
			t.Fatalf("ensure extension: %v", err)
		}
		if err := EnsureRole(ctx, conn, "tenant_owner", "pw", -1); err != nil {
			t.Fatalf("ensure role: %v", err)
		}
		if err := AlterSchemaOwner(ctx, conn, "releases", "tenant_owner"); err != nil {
			t.Fatalf("alter schema owner: %v", err)
		}

		if err := EnsureExtension(ctx, conn, "pg_trgm", "releases"); err != nil {
			t.Fatalf("reconcile after ownership handover: %v", err)
		}
	})
}

// TestEnsureExtensionGuardsPrerequisiteNamedByAnUpdateScript covers the
// prerequisite a default-version-only read misses.
//
// Reaching an extension's default version can mean running a base script and
// then a chain of update scripts, and Postgres builds each script's search_path
// from the `requires` of the version that script belongs to. dbclaim_probe's
// default version 1.1 requires nothing; its 1.0 base script requires cube, and
// CREATE EXTENSION runs that script on the way to 1.1 — so cube's schema is on
// a superuser script's path even though the default version never names it.
func TestEnsureExtensionGuardsPrerequisiteNamedByAnUpdateScript(t *testing.T) {
	ctx, admin := integrationConn(t)

	// The pair, as above: the first subtest fixes the mechanism in place, the
	// second proves the guard covers it. The probe extension's 1.0 script
	// resolves an *unqualified* cube type, so an install that succeeds is proof
	// Postgres put the prerequisite's schema on that script's path — had it
	// not, CREATE EXTENSION would fail with "type cube does not exist".
	t.Run("installs when the intermediate prerequisite's schema is the operator's", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		installProbeExtension(ctx, t, conn)
		for _, schema := range []string{"shared", "safe"} {
			if err := EnsureSchema(ctx, conn, schema); err != nil {
				t.Fatalf("ensure schema %q: %v", schema, err)
			}
		}
		if err := EnsureExtension(ctx, conn, "cube", "shared"); err != nil {
			t.Fatalf("seed prerequisite: %v", err)
		}
		if _, err := conn.Exec(ctx, "SET search_path TO pg_catalog"); err != nil {
			t.Fatalf("pin session search_path: %v", err)
		}

		if err := EnsureExtension(ctx, conn, probeExtension, "safe"); err != nil {
			t.Fatalf("install probe extension: %v", err)
		}
		var version string
		if err := conn.QueryRow(ctx,
			"SELECT extversion FROM pg_catalog.pg_extension WHERE extname = $1", probeExtension).Scan(&version); err != nil {
			t.Fatalf("read probe version: %v", err)
		}
		if version != "1.1" {
			t.Fatalf("probe installed at version %q, want 1.1 reached through the 1.0 script", version)
		}
	})

	t.Run("refuses when the intermediate prerequisite's schema is a tenant's", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		installProbeExtension(ctx, t, conn)
		for _, schema := range []string{"shared", "safe"} {
			if err := EnsureSchema(ctx, conn, schema); err != nil {
				t.Fatalf("ensure schema %q: %v", schema, err)
			}
		}
		if err := EnsureExtension(ctx, conn, "cube", "shared"); err != nil {
			t.Fatalf("seed prerequisite: %v", err)
		}
		if err := EnsureRole(ctx, conn, "tenant_owner", "pw", -1); err != nil {
			t.Fatalf("ensure role: %v", err)
		}
		if err := AlterSchemaOwner(ctx, conn, "shared", "tenant_owner"); err != nil {
			t.Fatalf("alter schema owner: %v", err)
		}

		err := EnsureExtension(ctx, conn, probeExtension, "safe")
		var refusal *ExtensionSchemaNotExclusiveError
		if !errors.As(err, &refusal) {
			t.Fatalf("EnsureExtension: got err %v, want *ExtensionSchemaNotExclusiveError", err)
		}
		if refusal.Schema != "shared" || refusal.Prerequisite != "cube" {
			t.Fatalf("refusal = %+v, want schema \"shared\" and prerequisite \"cube\"", *refusal)
		}
		var installed int
		if err := conn.QueryRow(ctx,
			"SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = $1", probeExtension).Scan(&installed); err != nil {
			t.Fatalf("count probe extension: %v", err)
		}
		if installed != 0 {
			t.Fatal("probe extension was installed despite the refusal")
		}
	})
}

// TestEnsureExtensionHoldsSchemaOwnershipThroughInstall drives the handover
// *into* the window between the exclusivity check and the CREATE that trusts
// it: a second session issues the ALTER SCHEMA ... OWNER TO that applying an
// Owner RoleClaim issues, and holds its transaction open across the install.
//
// The install must not complete against a schema whose handover is already in
// flight. Reading ownership and creating are two statements at Read Committed,
// so nothing but a lock on the row the handover updates can make that hold.
func TestEnsureExtensionHoldsSchemaOwnershipThroughInstall(t *testing.T) {
	ctx, admin := integrationConn(t)

	conn := scratchDatabase(ctx, t, admin)
	tenant := secondConnection(ctx, t, conn)
	if err := EnsureSchema(ctx, conn, "releases"); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if err := EnsureRole(ctx, conn, "tenant_owner", "pw", -1); err != nil {
		t.Fatalf("ensure role: %v", err)
	}

	// The handover, caught mid-flight. AlterSchemaOwner runs the same statement
	// in autocommit, which no test can pause inside; running it by hand in an
	// open transaction holds the window open instead.
	handover, err := tenant.Begin(ctx)
	if err != nil {
		t.Fatalf("begin handover: %v", err)
	}
	defer func() { _ = handover.Rollback(context.Background()) }()
	if _, err := handover.Exec(ctx, `ALTER SCHEMA "releases" OWNER TO "tenant_owner"`); err != nil {
		t.Fatalf("alter schema owner: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- EnsureExtension(ctx, conn, "pg_trgm", "releases") }()

	// Let the installer run until it is waiting on the handover — or until it
	// finishes, which is the failure this test exists for.
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("EnsureExtension returned %v while the handover was still in flight; "+
				"the install ran against ownership it no longer holds", err)
		default:
		}
		var waiting int
		if err := admin.QueryRow(ctx,
			"SELECT count(*) FROM pg_catalog.pg_locks WHERE NOT granted").Scan(&waiting); err != nil {
			t.Fatalf("read pg_locks: %v", err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("installer neither finished nor blocked on the in-flight handover")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := handover.Commit(ctx); err != nil {
		t.Fatalf("commit handover: %v", err)
	}

	err = <-done
	var refusal *ExtensionSchemaNotExclusiveError
	if !errors.As(err, &refusal) {
		t.Fatalf("EnsureExtension: got err %v, want *ExtensionSchemaNotExclusiveError after the handover committed", err)
	}
	var installed int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = 'pg_trgm'").Scan(&installed); err != nil {
		t.Fatalf("count pg_trgm: %v", err)
	}
	if installed != 0 {
		t.Fatal("pg_trgm was installed into a schema handed to the tenant")
	}
}

// TestEnsureExtensionFreezesPrerequisiteSetThroughInstall drives the second race
// the exclusivity guard is exposed to. Not a handover of a schema it checked —
// that is the test above — but a prerequisite arriving after it decided which
// schemas to check.
//
// extensionPrerequisites reads the installed requirements out of pg_extension,
// and a requirement that is not installed when that read runs has no catalog row
// for any lock to cover. A second session that installs one into a tenant's
// schema mid-install would put that schema on the install script's search_path
// unchecked, and CREATE EXTENSION runs that script as superuser. So the install
// must hold the extension catalog against writers for its whole transaction.
func TestEnsureExtensionFreezesPrerequisiteSetThroughInstall(t *testing.T) {
	ctx, admin := integrationConn(t)

	conn := scratchDatabase(ctx, t, admin)
	for _, schema := range []string{"safe", "tenant"} {
		if err := EnsureSchema(ctx, conn, schema); err != nil {
			t.Fatalf("ensure schema %q: %v", schema, err)
		}
	}
	if err := EnsureRole(ctx, conn, "tenant_owner", "pw", -1); err != nil {
		t.Fatalf("ensure role: %v", err)
	}
	if err := AlterSchemaOwner(ctx, conn, "tenant", "tenant_owner"); err != nil {
		t.Fatalf("alter schema owner: %v", err)
	}

	// Park the installer inside its transaction. It blocks where
	// lockSchemaOwnership waits — on the pg_namespace row of its target schema —
	// which is after everything it locks before reading the prerequisite set.
	parker := secondConnection(ctx, t, conn)
	park, err := parker.Begin(ctx)
	if err != nil {
		t.Fatalf("begin parking transaction: %v", err)
	}
	defer func() { _ = park.Rollback(context.Background()) }()
	if _, err := park.Exec(ctx,
		"SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'safe' FOR UPDATE"); err != nil {
		t.Fatalf("hold the target schema's catalog row: %v", err)
	}

	install := make(chan error, 1)
	go func() { install <- EnsureExtension(ctx, conn, "pg_trgm", "safe") }()
	installer := conn.PgConn().PID()
	awaitBlocked(ctx, t, admin, installer, install, "installer")

	var held int
	if err := admin.QueryRow(ctx,
		`SELECT count(*) FROM pg_catalog.pg_locks
		 WHERE pid = $1 AND locktype = 'relation' AND granted
		   AND relation = 'pg_catalog.pg_extension'::pg_catalog.regclass
		   AND mode = 'ShareRowExclusiveLock'`, installer).Scan(&held); err != nil {
		t.Fatalf("read pg_locks: %v", err)
	}
	if held != 1 {
		t.Fatalf("installer holds %d ShareRowExclusiveLock(s) on pg_extension, want 1; "+
			"the prerequisite set it read is not frozen", held)
	}

	// The racer the guard cannot see: a prerequisite installed into a schema the
	// tenant owns, after the prerequisite set was read.
	racerConn := secondConnection(ctx, t, conn)
	race := make(chan error, 1)
	go func() {
		_, err := racerConn.Exec(ctx, `CREATE EXTENSION "cube" SCHEMA "tenant"`)
		race <- err
	}()
	awaitBlocked(ctx, t, admin, racerConn.PgConn().PID(), race, "concurrent CREATE EXTENSION")

	if err := park.Commit(ctx); err != nil {
		t.Fatalf("release the parking transaction: %v", err)
	}
	if err := <-install; err != nil {
		t.Fatalf("EnsureExtension after the park was released: %v", err)
	}
	// The racer must have been merely waiting: a statement that would have
	// failed anyway would make the blocking assertion above prove nothing.
	if err := <-race; err != nil {
		t.Fatalf("concurrent CREATE EXTENSION failed for its own reasons: %v", err)
	}
	if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "safe" {
		t.Fatalf("pg_trgm installed in schema %q, want %q", got, "safe")
	}
}

// TestEnsureExtensionBoundsItsLockWait holds the extension catalog against the
// install the way an ordinary tenant does — a transaction left open after
// CREATE EXTENSION keeps RowExclusiveLock on pg_extension, which conflicts with
// the ShareRowExclusiveLock the install takes. The lock is taken directly here
// because who holds it makes no difference to the wait.
//
// Nothing outside this operator bounds that wait: the reconciler runs a single
// worker with no deadline, so an install that waits forever stops every other
// claim from being reconciled. It must give up instead.
func TestEnsureExtensionBoundsItsLockWait(t *testing.T) {
	ctx, admin := integrationConn(t)

	conn := scratchDatabase(ctx, t, admin)
	if err := EnsureSchema(ctx, conn, "safe"); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	holder := secondConnection(ctx, t, conn)
	hold, err := holder.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holding transaction: %v", err)
	}
	defer func() { _ = hold.Rollback(context.Background()) }()
	if _, err := hold.Exec(ctx, "LOCK TABLE pg_catalog.pg_extension IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatalf("hold the extension catalog: %v", err)
	}

	install := make(chan error, 1)
	start := time.Now()
	go func() { install <- EnsureExtension(ctx, conn, "pg_trgm", "safe") }()
	// The install must actually be waiting on that lock, or the bound below
	// would be asserted over a wait that never happened.
	awaitBlocked(ctx, t, admin, conn.PgConn().PID(), install, "installer")

	select {
	case err := <-install:
		waited := time.Since(start)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("EnsureExtension: got err %v, want a lock_timeout abort (SQLSTATE 55P03)", err)
		}
		if waited < lockWaitTimeout {
			t.Fatalf("EnsureExtension gave up after %v, before the %v it is meant to wait", waited, lockWaitTimeout)
		}
	case <-time.After(lockWaitTimeout + 20*time.Second):
		t.Fatalf("EnsureExtension is still waiting after %v on a lock another session holds; the wait is unbounded",
			time.Since(start))
	}

	var installed int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = 'pg_trgm'").Scan(&installed); err != nil {
		t.Fatalf("count pg_trgm: %v", err)
	}
	if installed != 0 {
		t.Fatal("pg_trgm was installed by a transaction that never took the catalog lock")
	}
}

// TestLockDownPublicBoundsItsLockWait holds the public schema's ACL the way an
// ordinary tenant does — a transaction left open after its own
// GRANT ... ON SCHEMA public keeps AccessExclusiveLock on the schema, which is
// exactly what the REVOKEs need.
//
// This one is worse than the extension install: LockDownPublic runs on every
// reconcile of every claim, whether or not the claim asks for extensions, and
// the reconciler runs a single worker with no deadline. Unbounded, one tenant's
// open transaction stops every claim in the cluster from being reconciled, with
// no event and no status to show for it. It must give up instead.
func TestLockDownPublicBoundsItsLockWait(t *testing.T) {
	ctx, admin := integrationConn(t)
	conn := scratchDatabase(ctx, t, admin)

	holder := secondConnection(ctx, t, conn)
	hold, err := holder.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holding transaction: %v", err)
	}
	defer func() { _ = hold.Rollback(context.Background()) }()
	if _, err := hold.Exec(ctx, "GRANT USAGE ON SCHEMA public TO PUBLIC"); err != nil {
		t.Fatalf("hold the public schema ACL: %v", err)
	}

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- LockDownPublic(ctx, conn) }()
	// The REVOKE must actually be waiting on that lock, or the bound below
	// would be asserted over a wait that never happened.
	awaitBlocked(ctx, t, admin, conn.PgConn().PID(), done, "lock down public")

	select {
	case err := <-done:
		waited := time.Since(start)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("LockDownPublic: got err %v, want a lock_timeout abort (SQLSTATE 55P03)", err)
		}
		if waited < lockWaitTimeout {
			t.Fatalf("LockDownPublic gave up after %v, before the %v it is meant to wait", waited, lockWaitTimeout)
		}
	case <-time.After(lockWaitTimeout + 20*time.Second):
		t.Fatalf("LockDownPublic is still waiting after %v on a lock another session holds; the wait is unbounded",
			time.Since(start))
	}
}

// awaitBlocked waits until the backend with the given pid is waiting on a lock,
// failing if the work it is running finishes first — which is the outcome the
// caller's assertion exists to rule out.
func awaitBlocked(ctx context.Context, t *testing.T, admin *pgx.Conn, pid uint32, done <-chan error, what string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("%s returned %v instead of blocking; it ran against a prerequisite set that was still moving", what, err)
		default:
		}
		var waiting int
		if err := admin.QueryRow(ctx,
			"SELECT count(*) FROM pg_catalog.pg_locks WHERE pid = $1 AND NOT granted", pid).Scan(&waiting); err != nil {
			t.Fatalf("read pg_locks: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s neither finished nor blocked", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// probeExtension is the name of the fixture extension installProbeExtension
// writes into the server's extension directory.
const probeExtension = "dbclaim_probe"

// installProbeExtension writes an extension whose default version is reachable
// only through an update script, and whose *base* version is the one that
// requires cube. No contrib extension has that shape (in a stock PostgreSQL 17
// only earthdistance varies by version at all, and it requires cube at every
// one), so the fixture builds it: COPY ... TO PROGRAM writes the control and
// script files as the server's OS user.
//
// That needs the server local to this session and its extension directory
// writable by the server process — in the postgres:17 image, one
//
//	docker exec <container> chmod 777 "$(pg_config --sharedir)/extension"
//
// away. Without it the fixture cannot exist and the test skips loudly rather
// than passing on a condition it never created.
func installProbeExtension(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()

	var dir string
	if err := conn.QueryRow(ctx,
		"SELECT setting || '/extension' FROM pg_catalog.pg_config WHERE name = 'SHAREDIR'").Scan(&dir); err != nil {
		t.Skipf("cannot locate the server's extension directory: %v", err)
	}
	files := map[string][]string{
		probeExtension + ".control": {
			"default_version = '1.1'",
			"relocatable = true",
			"comment = 'cnpg-dbclaim-operator test fixture'",
		},
		// The divergence: the base version requires cube, the default version
		// does not.
		probeExtension + "--1.0.control": {"requires = 'cube'"},
		probeExtension + "--1.0.sql": {
			// Unqualified cube: resolvable only because Postgres puts the
			// prerequisite's schema on this script's search_path.
			"CREATE FUNCTION probe_dim(cube) RETURNS int AS 'SELECT 0' LANGUAGE sql IMMUTABLE;",
		},
		probeExtension + "--1.0--1.1.sql": {"SELECT 1;"},
	}
	names := make([]string, 0, len(files))
	for name, lines := range files {
		names = append(names, dir+"/"+name)
		quoted := make([]string, 0, len(lines))
		for _, line := range lines {
			quoted = append(quoted, pq.QuoteLiteral(line))
		}
		// COPY takes no bind parameters, hence the quoted literals: one row per
		// line, so the text format writes the file line by line.
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"COPY (SELECT pg_catalog.unnest(ARRAY[%s]::text[])) TO PROGRAM %s",
			strings.Join(quoted, ", "), pq.QuoteLiteral("cat > "+dir+"/"+name))); err != nil {
			t.Skipf("cannot write %s on the server: %v", name, err)
		}
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), fmt.Sprintf("COPY (SELECT 1) TO PROGRAM %s",
			pq.QuoteLiteral("cat > /dev/null; rm -f "+strings.Join(names, " "))))
	})

	var versions int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_catalog.pg_available_extension_versions WHERE name = $1",
		probeExtension).Scan(&versions); err != nil {
		t.Fatalf("read probe versions: %v", err)
	}
	if versions != 2 {
		t.Fatalf("server offers %d versions of %s, want the fixture's 2", versions, probeExtension)
	}
}

// secondConnection opens another connection to the same database, for the
// tests that need two sessions racing.
func secondConnection(ctx context.Context, t *testing.T, like *pgx.Conn) *pgx.Conn {
	t.Helper()
	conn, err := pgx.ConnectConfig(ctx, like.Config().Copy())
	if err != nil {
		t.Fatalf("open second connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// integrationConn returns an admin connection for the DSN-gated tests, or skips.
func integrationConn(t *testing.T) (context.Context, *pgx.Conn) {
	t.Helper()
	dsn := os.Getenv("CNPG_DBCLAIM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set CNPG_DBCLAIM_POSTGRES_DSN to run extension schema integration tests against a real Postgres")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	return ctx, admin
}

// scratchDatabase creates a uniquely-named database over the admin connection
// and returns a connection to it, dropping the database when the test ends.
func scratchDatabase(ctx context.Context, t *testing.T, admin *pgx.Conn) *pgx.Conn {
	t.Helper()
	name := strings.ToLower(fmt.Sprintf("ext_test_%x", time.Now().UnixNano()))
	if err := EnsureDatabase(ctx, admin, name); err != nil {
		t.Fatalf("create scratch database: %v", err)
	}

	cfg := admin.Config().Copy()
	cfg.Database = name
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect scratch database %q: %v", name, err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_ = conn.Close(bg)
		_ = TerminateBackends(bg, admin, name)
		_ = DropDatabase(bg, admin, name)
	})
	return conn
}

func installedExtensionSchema(ctx context.Context, t *testing.T, conn *pgx.Conn, name string) string {
	t.Helper()
	var schema string
	// Qualified so the assertion still reads the real catalog under the hostile
	// search_path TestEnsureExtensionCatalogReadIsNotCapturable installs.
	err := conn.QueryRow(ctx,
		`SELECT n.nspname FROM pg_catalog.pg_extension e
		 JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
		 WHERE e.extname = $1`,
		name).Scan(&schema)
	if err != nil {
		t.Fatalf("query extension schema for %q: %v", name, err)
	}
	return schema
}
