/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	cnpgresolver "github.com/wyvernzora/cnpg-dbclaim-operator/internal/cnpg"
)

func mkDB(name string, schemas ...string) *cnpgclaimv1alpha1.DatabaseClaim {
	return &cnpgclaimv1alpha1.DatabaseClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
			DatabaseName: name,
			Schemas:      schemas,
		},
	}
}

func mkSugarRC(name, roleName, db string, access cnpgclaimv1alpha1.AccessLevel) *cnpgclaimv1alpha1.RoleClaim {
	a := access
	return &cnpgclaimv1alpha1.RoleClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: cnpgclaimv1alpha1.RoleClaimSpec{
			DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: db},
			Access:           &a,
			RoleName:         roleName,
		},
	}
}

func mkExplicitRC(name, roleName, db string, grants ...cnpgclaimv1alpha1.SchemaGrant) *cnpgclaimv1alpha1.RoleClaim {
	return &cnpgclaimv1alpha1.RoleClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: cnpgclaimv1alpha1.RoleClaimSpec{
			DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: db},
			Schemas:          grants,
			RoleName:         roleName,
		},
	}
}

// sortByName ignores ordering when comparing grant slices.
var sortByName = cmpopts.SortSlices(func(a, b cnpgclaimv1alpha1.SchemaGrant) bool { return a.Name < b.Name })

func TestResolveSchemas_Sugar(t *testing.T) {
	db := mkDB("orders", "public", "app")
	rc := mkSugarRC("svc", "svc_role", "orders", cnpgclaimv1alpha1.AccessReadWrite)

	got, err := resolveSchemas(rc, db)
	if err != nil {
		t.Fatalf("resolveSchemas: %v", err)
	}
	want := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "app", Access: cnpgclaimv1alpha1.AccessReadWrite},
		{Name: "public", Access: cnpgclaimv1alpha1.AccessReadWrite},
	}
	if diff := cmp.Diff(want, got, sortByName); diff != "" {
		t.Errorf("resolveSchemas mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSchemas_Explicit(t *testing.T) {
	db := mkDB("orders-domain", "ordering", "shipping", "shared")
	rc := mkExplicitRC("ordering-svc", "ordering_svc_role", "orders-domain",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
		cnpgclaimv1alpha1.SchemaGrant{Name: "shared", Access: cnpgclaimv1alpha1.AccessReadWrite},
		cnpgclaimv1alpha1.SchemaGrant{Name: "shipping", Access: cnpgclaimv1alpha1.AccessReadOnly},
	)
	got, err := resolveSchemas(rc, db)
	if err != nil {
		t.Fatalf("resolveSchemas: %v", err)
	}
	want := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
		{Name: "shared", Access: cnpgclaimv1alpha1.AccessReadWrite},
		{Name: "shipping", Access: cnpgclaimv1alpha1.AccessReadOnly},
	}
	if diff := cmp.Diff(want, got, sortByName); diff != "" {
		t.Errorf("resolveSchemas mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSchemas_UnknownSchema(t *testing.T) {
	db := mkDB("orders", "app")
	rc := mkExplicitRC("svc", "svc_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "missing", Access: cnpgclaimv1alpha1.AccessReadOnly},
	)
	if _, err := resolveSchemas(rc, db); err == nil {
		t.Fatal("expected UnknownSchema error, got nil")
	}
}

func TestSchemasNeedingRevoke_RemovedSchema(t *testing.T) {
	prev := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "a", Access: cnpgclaimv1alpha1.AccessReadOnly},
		{Name: "b", Access: cnpgclaimv1alpha1.AccessReadWrite},
		{Name: "c", Access: cnpgclaimv1alpha1.AccessOwner},
	}
	cur := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "a", Access: cnpgclaimv1alpha1.AccessReadOnly},
		{Name: "c", Access: cnpgclaimv1alpha1.AccessOwner},
	}
	got := schemasNeedingRevoke(prev, cur)
	want := []string{"b"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("schemasNeedingRevoke mismatch (-want +got):\n%s", diff)
	}
}

func TestSchemasNeedingRevoke_AccessChanged(t *testing.T) {
	prev := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "app", Access: cnpgclaimv1alpha1.AccessReadWrite},
		{Name: "shared", Access: cnpgclaimv1alpha1.AccessReadOnly},
	}
	cur := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "app", Access: cnpgclaimv1alpha1.AccessReadOnly},
		{Name: "shared", Access: cnpgclaimv1alpha1.AccessReadOnly},
	}
	got := schemasNeedingRevoke(prev, cur)
	want := []string{"app"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("schemasNeedingRevoke mismatch (-want +got):\n%s", diff)
	}
}

func TestSchemasNeedingRevoke_Unchanged(t *testing.T) {
	prev := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "app", Access: cnpgclaimv1alpha1.AccessReadWrite},
	}
	cur := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "app", Access: cnpgclaimv1alpha1.AccessReadWrite},
	}
	if got := schemasNeedingRevoke(prev, cur); len(got) != 0 {
		t.Errorf("expected no revokes, got %v", got)
	}
}

func TestFindOwnerConflict_NoConflict(t *testing.T) {
	db := mkDB("orders", "ordering", "shipping")
	resolved := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	}
	sibling := mkExplicitRC("shipping-svc", "shipping_svc_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "shipping", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	sibling.UID = "uid-shipping"
	sibling.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	me := mkExplicitRC("ordering-svc", "ordering_svc_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	me.UID = "uid-ordering"
	me.CreationTimestamp = metav1.NewTime(time.Now())
	if got := findOwnerConflict(me, resolved, []cnpgclaimv1alpha1.RoleClaim{*sibling}, db); got != "" {
		t.Errorf("expected no conflict, got %q", got)
	}
}

// Status-empty conflict: the older sibling has never reconciled (status empty)
// but its spec already asserts Owner on the same schema. The fix makes the
// projection from spec, so the newcomer must lose.
func TestFindOwnerConflict_SpecProjection_StatusEmpty(t *testing.T) {
	db := mkDB("orders", "ordering")
	resolved := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	}
	earlier := time.Now().Add(-time.Hour)
	sibling := mkExplicitRC("original-owner", "original_owner_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	sibling.UID = "uid-orig"
	sibling.CreationTimestamp = metav1.NewTime(earlier)
	// Crucially: empty status. Old code would miss this.
	sibling.Status = cnpgclaimv1alpha1.RoleClaimStatus{}
	me := mkExplicitRC("newcomer", "newcomer_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	me.UID = "uid-new"
	me.CreationTimestamp = metav1.NewTime(time.Now())
	if got := findOwnerConflict(me, resolved, []cnpgclaimv1alpha1.RoleClaim{*sibling}, db); got == "" {
		t.Error("expected newcomer to lose against status-empty sibling with Owner in spec, got empty conflict string")
	}
}

func TestFindOwnerConflict_NewerLoses(t *testing.T) {
	db := mkDB("orders", "ordering")
	earlier := time.Now().Add(-time.Hour)
	resolved := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	}
	sibling := mkExplicitRC("original-owner", "original_owner_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	sibling.UID = "uid-orig"
	sibling.CreationTimestamp = metav1.NewTime(earlier)
	me := mkExplicitRC("newcomer", "newcomer_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	me.UID = "uid-new"
	me.CreationTimestamp = metav1.NewTime(time.Now())
	if got := findOwnerConflict(me, resolved, []cnpgclaimv1alpha1.RoleClaim{*sibling}, db); got == "" {
		t.Error("expected newer claim to lose, got empty conflict string")
	}
}

// Same-second timestamp tie should be broken deterministically by UID. Without
// the UID fallback, both claims would see the other as "newer" and lose.
func TestFindOwnerConflict_SameSecondUIDTiebreak(t *testing.T) {
	db := mkDB("orders", "ordering")
	now := metav1.NewTime(time.Now())
	resolved := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	}
	sibling := mkExplicitRC("lower-uid", "lower_uid_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	sibling.UID = "aaa"
	sibling.CreationTimestamp = now
	me := mkExplicitRC("higher-uid", "higher_uid_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	me.UID = "zzz"
	me.CreationTimestamp = now
	if got := findOwnerConflict(me, resolved, []cnpgclaimv1alpha1.RoleClaim{*sibling}, db); got == "" {
		t.Error("higher-UID claim should lose the tiebreak, got empty conflict string")
	}
}

// Siblings marked for deletion should not block ownership transfer.
func TestFindOwnerConflict_DeletingSiblingIgnored(t *testing.T) {
	db := mkDB("orders", "ordering")
	now := metav1.NewTime(time.Now())
	resolved := []cnpgclaimv1alpha1.SchemaGrant{
		{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	}
	deleting := metav1.NewTime(now.Add(-time.Minute))
	sibling := mkExplicitRC("retiring", "retiring_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	sibling.UID = "uid-old"
	sibling.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	sibling.DeletionTimestamp = &deleting
	me := mkExplicitRC("incoming", "incoming_role", "orders",
		cnpgclaimv1alpha1.SchemaGrant{Name: "ordering", Access: cnpgclaimv1alpha1.AccessOwner},
	)
	me.UID = "uid-new"
	me.CreationTimestamp = now
	if got := findOwnerConflict(me, resolved, []cnpgclaimv1alpha1.RoleClaim{*sibling}, db); got != "" {
		t.Errorf("deleting sibling should not block ownership, got conflict %q", got)
	}
}

func TestResolvedRoleName_UsesSpecVerbatim(t *testing.T) {
	rc := mkSugarRC("any-k8s-name", "explicit_role", "orders", cnpgclaimv1alpha1.AccessReadWrite)
	if got := resolvedRoleName(rc); got != "explicit_role" {
		t.Errorf("resolvedRoleName: got %q want %q", got, "explicit_role")
	}
}

func TestSortDefaultPrivilegeGrants(t *testing.T) {
	in := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{
		{Writer: "b_writer", Schema: "shared"},
		{Writer: "a_writer", Schema: "z_schema"},
		{Writer: "a_writer", Schema: "a_schema"},
	}
	sortDefaultPrivilegeGrants(in)
	want := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{
		{Writer: "a_writer", Schema: "a_schema"},
		{Writer: "a_writer", Schema: "z_schema"},
		{Writer: "b_writer", Schema: "shared"},
	}
	if diff := cmp.Diff(want, in); diff != "" {
		t.Errorf("sortDefaultPrivilegeGrants mismatch (-want +got):\n%s", diff)
	}
}

func TestDefaultPrivilegesToRevoke_WriterReplacement(t *testing.T) {
	previous := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer_a", Schema: "s"}}
	current := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer_b", Schema: "s"}}

	got := defaultPrivilegesToRevoke(previous, current)
	want := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer_a", Schema: "s"}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("defaultPrivilegesToRevoke mismatch (-want +got):\n%s", diff)
	}
}

func TestDefaultPrivilegesAsReader_ReaderNarrowingDropsWriterSchemaPair(t *testing.T) {
	tuples := []defaultPrivilegeTuple{
		{role: "writer", schema: "s", access: cnpgclaimv1alpha1.AccessReadWrite},
		{role: "reader", schema: "s", access: cnpgclaimv1alpha1.AccessReadWrite},
	}

	got := defaultPrivilegesAsReader(tuples, "reader")
	if len(got) != 0 {
		t.Fatalf("reader narrowed away from ReadOnly should have no default privileges, got %v", got)
	}
	toRevoke := defaultPrivilegesToRevoke(
		[]cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer", Schema: "s"}},
		got,
	)
	wantRevoke := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer", Schema: "s"}}
	if diff := cmp.Diff(wantRevoke, toRevoke); diff != "" {
		t.Errorf("defaultPrivilegesToRevoke mismatch (-want +got):\n%s", diff)
	}
}

func TestDefaultPrivilegesAsReader_WriterNarrowingDropsSiblingReaderDesiredSet(t *testing.T) {
	tuples := []defaultPrivilegeTuple{
		{role: "writer", schema: "s", access: cnpgclaimv1alpha1.AccessReadOnly},
		{role: "reader", schema: "s", access: cnpgclaimv1alpha1.AccessReadOnly},
	}

	got := defaultPrivilegesAsReader(tuples, "reader")
	if len(got) != 0 {
		t.Fatalf("writer narrowed away from ReadWrite should not remain in desired set, got %v", got)
	}
	toRevoke := defaultPrivilegesToRevoke(
		[]cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer", Schema: "s"}},
		got,
	)
	wantRevoke := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer", Schema: "s"}}
	if diff := cmp.Diff(wantRevoke, toRevoke); diff != "" {
		t.Errorf("defaultPrivilegesToRevoke mismatch (-want +got):\n%s", diff)
	}
}

func TestDefaultPrivilegeHelpersSortDefensively(t *testing.T) {
	previous := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{
		{Writer: "z_writer", Schema: "z"},
		{Writer: "a_writer", Schema: "b"},
		{Writer: "a_writer", Schema: "a"},
	}
	gotCopy := sortedDefaultPrivilegeGrantsCopy(previous)
	wantCopy := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{
		{Writer: "a_writer", Schema: "a"},
		{Writer: "a_writer", Schema: "b"},
		{Writer: "z_writer", Schema: "z"},
	}
	if diff := cmp.Diff(wantCopy, gotCopy); diff != "" {
		t.Errorf("sortedDefaultPrivilegeGrantsCopy mismatch (-want +got):\n%s", diff)
	}
	if previous[0].Writer != "z_writer" {
		t.Fatalf("sortedDefaultPrivilegeGrantsCopy mutated input: %v", previous)
	}

	tuples := []defaultPrivilegeTuple{
		{role: "z_writer", schema: "z", access: cnpgclaimv1alpha1.AccessReadWrite},
		{role: "reader", schema: "z", access: cnpgclaimv1alpha1.AccessReadOnly},
		{role: "a_writer", schema: "b", access: cnpgclaimv1alpha1.AccessOwner},
		{role: "reader", schema: "b", access: cnpgclaimv1alpha1.AccessReadOnly},
		{role: "a_writer", schema: "a", access: cnpgclaimv1alpha1.AccessReadWrite},
		{role: "reader", schema: "a", access: cnpgclaimv1alpha1.AccessReadOnly},
	}
	gotDesired := defaultPrivilegesAsReader(tuples, "reader")
	if diff := cmp.Diff(wantCopy, gotDesired); diff != "" {
		t.Errorf("defaultPrivilegesAsReader should sort deterministically (-want +got):\n%s", diff)
	}
}

func TestBuildDefaultPrivilegeTuplesSkipsSelfAndUnprovisionedSiblings(t *testing.T) {
	selfUID := types.UID("self")
	selfGrants := []cnpgclaimv1alpha1.SchemaGrant{{Name: "app", Access: cnpgclaimv1alpha1.AccessReadOnly}}
	selfSibling := mkSugarRC("self", "ignored", "orders", cnpgclaimv1alpha1.AccessReadWrite)
	selfSibling.UID = selfUID
	selfSibling.Status.RoleName = "ignored"
	selfSibling.Status.ResolvedSchemas = []cnpgclaimv1alpha1.SchemaGrant{{Name: "app", Access: cnpgclaimv1alpha1.AccessReadWrite}}
	unprovisioned := mkSugarRC("new", "new_role", "orders", cnpgclaimv1alpha1.AccessReadWrite)
	provisioned := mkSugarRC("writer", "writer_role", "orders", cnpgclaimv1alpha1.AccessReadWrite)
	provisioned.Status.RoleName = "writer_role"
	provisioned.Status.ResolvedSchemas = []cnpgclaimv1alpha1.SchemaGrant{{Name: "app", Access: cnpgclaimv1alpha1.AccessReadWrite}}

	got := buildDefaultPrivilegeTuples(selfUID, "reader_role", selfGrants, []cnpgclaimv1alpha1.RoleClaim{
		*selfSibling,
		*unprovisioned,
		*provisioned,
	})
	want := []defaultPrivilegeTuple{
		{role: "reader_role", schema: "app", access: cnpgclaimv1alpha1.AccessReadOnly},
		{role: "writer_role", schema: "app", access: cnpgclaimv1alpha1.AccessReadWrite},
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(defaultPrivilegeTuple{})); diff != "" {
		t.Errorf("buildDefaultPrivilegeTuples mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveErrorReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "cluster missing",
			err:  cnpgresolver.ErrClusterNotFound,
			want: ReasonClusterMissing,
		},
		{
			name: "cluster not ready",
			err:  cnpgresolver.ErrClusterNotReady,
			want: ReasonClusterNotReady,
		},
		{
			name: "superuser secret failure",
			err:  cnpgresolver.ErrSuperUserSecretMissing,
			want: ReasonResolveFailed,
		},
		{
			name: "generic resolve failure",
			err:  errors.New("boom"),
			want: ReasonResolveFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveErrorReason(tt.err); got != tt.want {
				t.Fatalf("resolveErrorReason() = %q, want %q", got, tt.want)
			}
		})
	}
}
