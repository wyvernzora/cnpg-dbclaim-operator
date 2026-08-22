/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package v1alpha1

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	runtimejson "k8s.io/apimachinery/pkg/runtime/serializer/json"
)

// TestDecodeExtensionsLegacyShape decodes through the same codec client-go
// builds for typed clients and informers, because that is the path an upgrade
// breaks: spec.extensions was a []string in v0.3.x and objects written then are
// still stored in that shape after the CRD is upgraded in place. A decode
// failure there is not local — it fails the controller's LIST and every Get,
// halting reconciliation for every claim in the cluster.
func TestDecodeExtensionsLegacyShape(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}
	serializer := runtimejson.NewSerializerWithOptions(
		runtimejson.DefaultMetaFactory, scheme, scheme, runtimejson.SerializerOptions{},
	)

	cases := []struct {
		name       string
		extensions string
		want       []ExtensionSpec
	}{
		{
			name:       "legacy bare strings",
			extensions: `["pgcrypto","pg_trgm"]`,
			want:       []ExtensionSpec{{Name: "pgcrypto"}, {Name: "pg_trgm"}},
		},
		{
			name:       "current object form",
			extensions: `[{"name":"pg_trgm","schema":"app"}]`,
			want:       []ExtensionSpec{{Name: "pg_trgm", Schema: "app"}},
		},
		{
			name:       "mixed, as a partially migrated object would be",
			extensions: `["pgcrypto",{"name":"pg_trgm","schema":"app"}]`,
			want:       []ExtensionSpec{{Name: "pgcrypto"}, {Name: "pg_trgm", Schema: "app"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := `{"apiVersion":"cnpg.wyvernzora.io/v1alpha1","kind":"DatabaseClaim",` +
				`"metadata":{"name":"orders","namespace":"app"},` +
				`"spec":{"databaseName":"orders","clusterRef":{"name":"pg","namespace":"cnpg"},` +
				`"extensions":` + tc.extensions + `}}`

			obj, _, err := serializer.Decode([]byte(doc), nil, &DatabaseClaim{})
			if err != nil {
				t.Fatalf("decode stored object: %v", err)
			}
			claim, ok := obj.(*DatabaseClaim)
			if !ok {
				t.Fatalf("decoded %T, want *DatabaseClaim", obj)
			}
			got := claim.Spec.Extensions
			if len(got) != len(tc.want) {
				t.Fatalf("extensions = %+v, want %+v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("extensions[%d] = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestEncodeExtensionsAlwaysObjectForm pins the write direction: a claim read in
// the legacy shape is written back in the current one, so the controller's own
// finalizer update migrates the stored object instead of re-persisting a shape
// the CRD schema rejects.
func TestEncodeExtensionsAlwaysObjectForm(t *testing.T) {
	var spec DatabaseClaimSpec
	if err := json.Unmarshal([]byte(`{"databaseName":"orders","extensions":["pgcrypto"]}`), &spec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	out, err := json.Marshal(spec.Extensions)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(out) != `[{"name":"pgcrypto"}]` {
		t.Fatalf("re-encoded extensions = %s, want [{\"name\":\"pgcrypto\"}]", out)
	}
}

// TestDecodeExtensionsRejectsOtherScalars keeps the legacy escape hatch narrow:
// only a JSON string stands in for an extension name.
func TestDecodeExtensionsRejectsOtherScalars(t *testing.T) {
	var spec DatabaseClaimSpec
	if err := json.Unmarshal([]byte(`{"extensions":[12]}`), &spec); err == nil {
		t.Fatalf("decoded extensions %+v from a number, want an error", spec.Extensions)
	}
}
