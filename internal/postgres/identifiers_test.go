/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package postgres

import "testing"

func TestValidateIdentifier(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"orders", true},
		{"_underscore_start", true},
		{"with_digits_123", true},
		{"a", true},
		{"Orders", false},        // uppercase rejected
		{"1leading_digit", false}, // digit start rejected
		{"contains-dash", false},
		{"contains space", false},
		{"contains\"quote", false},
		{"contains;semicolon", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateIdentifier(tc.name)
			got := err == nil
			if got != tc.want {
				t.Fatalf("ValidateIdentifier(%q): got err=%v, want valid=%v", tc.name, err, tc.want)
			}
		})
	}
}

func TestQuote(t *testing.T) {
	cases := map[string]string{
		"orders":     `"orders"`,
		`with"quote`: `"with""quote"`,
	}
	for in, want := range cases {
		if got := Quote(in); got != want {
			t.Errorf("Quote(%q) = %q, want %q", in, got, want)
		}
	}
}
