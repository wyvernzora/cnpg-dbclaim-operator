/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package postgres

import "testing"

func TestEnsureExtensionStmt(t *testing.T) {
	cases := []struct {
		name      string
		extension string
		schema    string
		want      string
		wantErr   bool
	}{
		{
			name:      "schema omitted keeps the server default search_path",
			extension: "pgcrypto",
			want:      `CREATE EXTENSION IF NOT EXISTS "pgcrypto"`,
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

func TestAlterExtensionSchemaStmt(t *testing.T) {
	got := alterExtensionSchemaStmt("pg_trgm", "releases")
	want := `ALTER EXTENSION "pg_trgm" SET SCHEMA "releases"`
	if got != want {
		t.Fatalf("alterExtensionSchemaStmt() = %q, want %q", got, want)
	}
}
