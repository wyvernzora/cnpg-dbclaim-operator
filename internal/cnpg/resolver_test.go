/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package cnpg

import (
	"errors"
	"testing"
)

func TestCheckClaimAllowed(t *testing.T) {
	tests := []struct {
		name           string
		clusterNs      string
		annotations    map[string]string
		claimNamespace string
		wantErr        bool
	}{
		{
			name:           "same namespace always allowed",
			clusterNs:      "infra",
			annotations:    nil,
			claimNamespace: "infra",
			wantErr:        false,
		},
		{
			name:           "same namespace allowed even with annotation excluding it",
			clusterNs:      "infra",
			annotations:    map[string]string{ClaimAllowlistAnnotation: "team-a,team-b"},
			claimNamespace: "infra",
			wantErr:        false,
		},
		{
			name:           "cross-namespace denied without annotation",
			clusterNs:      "infra",
			annotations:    nil,
			claimNamespace: "team-a",
			wantErr:        true,
		},
		{
			name:           "cross-namespace denied with empty annotation",
			clusterNs:      "infra",
			annotations:    map[string]string{ClaimAllowlistAnnotation: ""},
			claimNamespace: "team-a",
			wantErr:        true,
		},
		{
			name:           "cross-namespace allowed with exact match",
			clusterNs:      "infra",
			annotations:    map[string]string{ClaimAllowlistAnnotation: "team-a,team-b"},
			claimNamespace: "team-a",
			wantErr:        false,
		},
		{
			name:           "cross-namespace allowed with whitespace around entries",
			clusterNs:      "infra",
			annotations:    map[string]string{ClaimAllowlistAnnotation: " team-a , team-b "},
			claimNamespace: "team-b",
			wantErr:        false,
		},
		{
			name:           "cross-namespace denied when not in list",
			clusterNs:      "infra",
			annotations:    map[string]string{ClaimAllowlistAnnotation: "team-a,team-b"},
			claimNamespace: "team-c",
			wantErr:        true,
		},
		{
			name:           "annotation value with only commas denies all",
			clusterNs:      "infra",
			annotations:    map[string]string{ClaimAllowlistAnnotation: ",,"},
			claimNamespace: "team-a",
			wantErr:        true,
		},
		{
			name:           "unrelated annotation key has no effect",
			clusterNs:      "infra",
			annotations:    map[string]string{"other.example.com/foo": "team-a"},
			claimNamespace: "team-a",
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := &ClusterTarget{
				ClusterNamespace:   tc.clusterNs,
				ClusterAnnotations: tc.annotations,
			}
			err := CheckClaimAllowed(target, tc.claimNamespace)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !errors.Is(err, ErrClaimNotAllowed) {
					t.Fatalf("expected ErrClaimNotAllowed, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
