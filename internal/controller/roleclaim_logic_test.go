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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func TestFindDatabaseNameConflict_OldestWins(t *testing.T) {
	now := metav1.NewTime(time.Now())
	older := mkDB("older", "app")
	older.Namespace = "team-a"
	older.UID = "aaa"
	older.CreationTimestamp = now
	older.Spec.DatabaseName = "shared_app"
	older.Spec.ClusterRef = cnpgclaimv1alpha1.ClusterReference{Name: "pg", Namespace: "cnpg-system"}

	newer := mkDB("newer", "app")
	newer.Namespace = "team-b"
	newer.UID = "bbb"
	newer.CreationTimestamp = now
	newer.Spec.DatabaseName = "shared_app"
	newer.Spec.ClusterRef = older.Spec.ClusterRef

	if got := findDatabaseNameConflict(newer, []cnpgclaimv1alpha1.DatabaseClaim{*older}); got == "" {
		t.Fatal("expected newer DatabaseClaim to lose duplicate database ownership")
	}
	if got := findDatabaseNameConflict(older, []cnpgclaimv1alpha1.DatabaseClaim{*newer}); got != "" {
		t.Fatalf("older DatabaseClaim should win duplicate database ownership, got %q", got)
	}
}

func TestFindDatabaseNameConflict_IgnoresDeletingAndDifferentClusters(t *testing.T) {
	now := metav1.NewTime(time.Now())
	claim := mkDB("claim", "app")
	claim.Namespace = "team-a"
	claim.UID = "claim"
	claim.CreationTimestamp = now
	claim.Spec.DatabaseName = "app"
	claim.Spec.ClusterRef = cnpgclaimv1alpha1.ClusterReference{Name: "pg", Namespace: "cnpg-system"}

	deletingTime := metav1.NewTime(now.Add(time.Minute))
	deleting := claim.DeepCopy()
	deleting.Name = "deleting"
	deleting.UID = "aaa"
	deleting.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	deleting.DeletionTimestamp = &deletingTime

	otherCluster := claim.DeepCopy()
	otherCluster.Name = "other-cluster"
	otherCluster.UID = "bbb"
	otherCluster.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	otherCluster.Spec.ClusterRef.Name = "pg2"

	if got := findDatabaseNameConflict(claim, []cnpgclaimv1alpha1.DatabaseClaim{*deleting, *otherCluster}); got != "" {
		t.Fatalf("expected no database conflict, got %q", got)
	}
	if otherDatabaseClaimOwnsPhysicalDatabase(claim, []cnpgclaimv1alpha1.DatabaseClaim{*deleting, *otherCluster}) {
		t.Fatal("deleting or different-cluster claims should not protect this physical database")
	}
}

func TestFindRoleNameConflict_CrossNamespaceAndDatabaseClaim(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := cnpgclaimv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	now := metav1.NewTime(time.Now())
	cluster := cnpgclaimv1alpha1.ClusterReference{Name: "pg", Namespace: "cnpg-system"}

	dbA := mkDB("orders", "app")
	dbA.Namespace = "team-a"
	dbA.Spec.ClusterRef = cluster
	dbB := mkDB("billing", "app")
	dbB.Namespace = "team-b"
	dbB.Spec.ClusterRef = cluster

	older := mkSugarRC("svc-a", "shared_role", "orders", cnpgclaimv1alpha1.AccessReadWrite)
	older.Namespace = "team-a"
	older.UID = "aaa"
	older.CreationTimestamp = now
	newer := mkSugarRC("svc-b", "shared_role", "billing", cnpgclaimv1alpha1.AccessReadOnly)
	newer.Namespace = "team-b"
	newer.UID = "bbb"
	newer.CreationTimestamp = now

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dbA, dbB, older, newer).Build()
	r := &RoleClaimReconciler{Client: c}
	var storedOlder, storedNewer cnpgclaimv1alpha1.RoleClaim
	if err := c.Get(t.Context(), types.NamespacedName{Name: older.Name, Namespace: older.Namespace}, &storedOlder); err != nil {
		t.Fatalf("get older RoleClaim: %v", err)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: newer.Name, Namespace: newer.Namespace}, &storedNewer); err != nil {
		t.Fatalf("get newer RoleClaim: %v", err)
	}

	got, err := r.findRoleNameConflict(t.Context(), &storedNewer, dbB, storedNewer.Spec.RoleName)
	if err != nil {
		t.Fatalf("findRoleNameConflict: %v", err)
	}
	if got == "" {
		t.Fatal("expected newer RoleClaim to lose duplicate role ownership across namespaces")
	}

	got, err = r.findRoleNameConflict(t.Context(), &storedOlder, dbA, storedOlder.Spec.RoleName)
	if err != nil {
		t.Fatalf("findRoleNameConflict winner: %v", err)
	}
	if got != "" {
		t.Fatalf("older RoleClaim should win duplicate role ownership, got %q", got)
	}
}

func TestOtherRoleClaimOwnsPhysicalRole_IgnoresDeleting(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := cnpgclaimv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	now := metav1.NewTime(time.Now())
	cluster := cnpgclaimv1alpha1.ClusterReference{Name: "pg", Namespace: "cnpg-system"}
	db := mkDB("orders", "app")
	db.Namespace = "team-a"
	db.Spec.ClusterRef = cluster

	claim := mkSugarRC("svc", "shared_role", "orders", cnpgclaimv1alpha1.AccessReadWrite)
	claim.Namespace = "team-a"
	claim.UID = "claim"
	claim.CreationTimestamp = now
	deleting := mkSugarRC("deleting", "shared_role", "orders", cnpgclaimv1alpha1.AccessReadOnly)
	deleting.Namespace = "team-a"
	deleting.UID = "deleting"
	deleting.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	deleting.Finalizers = []string{RoleClaimFinalizer}
	deletingTime := metav1.NewTime(now.Add(time.Minute))
	deleting.DeletionTimestamp = &deletingTime

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db, claim, deleting).Build()
	r := &RoleClaimReconciler{Client: c}
	owned, err := r.otherRoleClaimOwnsPhysicalRole(t.Context(), claim, db, claim.Spec.RoleName)
	if err != nil {
		t.Fatalf("otherRoleClaimOwnsPhysicalRole: %v", err)
	}
	if owned {
		t.Fatal("deleting RoleClaim should not protect physical role")
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
	want := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer_a", Schema: "s", Access: cnpgclaimv1alpha1.AccessReadOnly}}
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
	want := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer", Schema: "s", Access: cnpgclaimv1alpha1.AccessReadWrite}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("ReadWrite reader should receive default privileges (-want +got):\n%s", diff)
	}
	toRevoke := defaultPrivilegesToRevoke(
		[]cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer", Schema: "s"}},
		got,
	)
	wantRevoke := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer", Schema: "s", Access: cnpgclaimv1alpha1.AccessReadOnly}}
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
	wantRevoke := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{{Writer: "writer", Schema: "s", Access: cnpgclaimv1alpha1.AccessReadOnly}}
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
		{Writer: "a_writer", Schema: "a", Access: cnpgclaimv1alpha1.AccessReadOnly},
		{Writer: "a_writer", Schema: "b", Access: cnpgclaimv1alpha1.AccessReadOnly},
		{Writer: "z_writer", Schema: "z", Access: cnpgclaimv1alpha1.AccessReadOnly},
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

func TestDefaultPrivilegesToRevoke_AccessChange(t *testing.T) {
	previous := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{
		{Writer: "writer", Schema: "s", Access: cnpgclaimv1alpha1.AccessReadOnly},
	}
	current := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{
		{Writer: "writer", Schema: "s", Access: cnpgclaimv1alpha1.AccessReadWrite},
	}

	got := defaultPrivilegesToRevoke(previous, current)
	if diff := cmp.Diff(previous, got); diff != "" {
		t.Errorf("access change should revoke previous tuple (-want +got):\n%s", diff)
	}
}

func TestDefaultPrivilegeAccess_DefaultsToReadOnly(t *testing.T) {
	g := cnpgclaimv1alpha1.DefaultPrivilegeGrant{Writer: "writer", Schema: "s"}
	if got := defaultPrivilegeAccess(g); got != cnpgclaimv1alpha1.AccessReadOnly {
		t.Fatalf("defaultPrivilegeAccess() = %q, want %q", got, cnpgclaimv1alpha1.AccessReadOnly)
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
