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

	t.Run("omitted schema leaves placement to the server default", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureExtension(ctx, conn, "pg_trgm", ""); err != nil {
			t.Fatalf("ensure extension: %v", err)
		}
		if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "public" {
			t.Fatalf("pg_trgm installed in schema %q, want %q", got, "public")
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
		if err := EnsureExtension(ctx, conn, "pg_trgm", ""); err != nil {
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

	t.Run("omitted schema does not relocate an extension installed elsewhere", func(t *testing.T) {
		conn := scratchDatabase(ctx, t, admin)
		if err := EnsureSchema(ctx, conn, "elsewhere"); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		if err := EnsureExtension(ctx, conn, "pg_trgm", "elsewhere"); err != nil {
			t.Fatalf("seed extension: %v", err)
		}

		if err := EnsureExtension(ctx, conn, "pg_trgm", ""); err != nil {
			t.Fatalf("ensure extension: %v", err)
		}
		if got := installedExtensionSchema(ctx, t, conn, "pg_trgm"); got != "elsewhere" {
			t.Fatalf("pg_trgm moved to %q; no requested schema means no opinion", got)
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
	err := conn.QueryRow(ctx,
		`SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = $1`,
		name).Scan(&schema)
	if err != nil {
		t.Fatalf("query extension schema for %q: %v", name, err)
	}
	return schema
}
