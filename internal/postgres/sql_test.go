/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package postgres

import (
	"context"
	"strings"
	"testing"
)

func TestEnsureExtensionStmt(t *testing.T) {
	cases := []struct {
		name      string
		extension string
		schema    string
		want      string
		wantErr   bool
	}{
		{
			name:      "empty schema is rejected",
			extension: "pgcrypto",
			wantErr:   true,
		},
		{
			name:      "schema targets the extension objects",
			extension: "pg_trgm",
			schema:    "releases",
			want:      `CREATE EXTENSION IF NOT EXISTS "pg_trgm" SCHEMA "releases"`,
		},
		{
			name:      "invalid extension name is rejected",
			extension: "pg_trgm; DROP DATABASE x",
			wantErr:   true,
		},
		{
			name:      "invalid schema is rejected",
			extension: "pg_trgm",
			schema:    "Releases-1",
			wantErr:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ensureExtensionStmt(tc.extension, tc.schema)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ensureExtensionStmt(%q, %q) = %q, want error", tc.extension, tc.schema, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ensureExtensionStmt(%q, %q): unexpected error: %v", tc.extension, tc.schema, err)
			}
			if got != tc.want {
				t.Fatalf("ensureExtensionStmt(%q, %q) = %q, want %q", tc.extension, tc.schema, got, tc.want)
			}
		})
	}
}

// TestEnsureExtensionRequiresSchema pins the migration error for a claim still
// stored in the v0.3.x bare-string shape: it decodes with Schema == "", and the
// convergence path must refuse it before touching the connection (nil here).
func TestEnsureExtensionRequiresSchema(t *testing.T) {
	err := EnsureExtension(context.Background(), nil, "pgcrypto", "")
	if err == nil {
		t.Fatal(`EnsureExtension with an empty schema returned nil, want the migration error`)
	}
	for _, fragment := range []string{`extension "pgcrypto" has no schema`, "Upgrading from v0.3.x"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("EnsureExtension error %q does not contain %q", err, fragment)
		}
	}
}

func TestAlterExtensionSchemaStmt(t *testing.T) {
	got := alterExtensionSchemaStmt("pg_trgm", "releases")
	want := `ALTER EXTENSION "pg_trgm" SET SCHEMA "releases"`
	if got != want {
		t.Fatalf("alterExtensionSchemaStmt() = %q, want %q", got, want)
	}
}
