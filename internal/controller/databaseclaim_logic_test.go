/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"errors"
	"fmt"
	"testing"

	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/postgres"
)

func TestApplyErrorReason(t *testing.T) {
	relocation := &postgres.ExtensionRelocationError{
		Extension: "xml2",
		From:      "public",
		To:        "releases",
		Err:       errors.New(`extension "xml2" does not support SET SCHEMA`),
	}
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "extension could not be relocated",
			err:  relocation,
			want: ReasonExtensionRelocationFailed,
		},
		{
			name: "wrapped relocation failure",
			err:  fmt.Errorf("apply database: %w", relocation),
			want: ReasonExtensionRelocationFailed,
		},
		{
			name: "generic apply failure",
			err:  errors.New("boom"),
			want: ReasonReconcileFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := applyErrorReason(tt.err); got != tt.want {
				t.Fatalf("applyErrorReason() = %q, want %q", got, tt.want)
			}
		})
	}
}
