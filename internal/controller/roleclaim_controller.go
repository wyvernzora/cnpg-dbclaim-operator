/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	cnpgresolver "github.com/wyvernzora/cnpg-dbclaim-operator/internal/cnpg"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/postgres"
	secretpkg "github.com/wyvernzora/cnpg-dbclaim-operator/internal/secret"
)

// roleClaimSecretLabel marks Secrets owned by a RoleClaim with the claim name.
const roleClaimSecretLabel = "cnpg.wyvernzora.io/roleclaim" // #nosec G101 -- Kubernetes label key, not a credential.

// RoleClaimReconciler reconciles RoleClaim resources.
type RoleClaimReconciler struct {
	client.Client
	// APIReader bypasses the controller-runtime cache and reads directly from
	// the apiserver. Used only for the owner-conflict check so that a sibling
	// freshly Create()-d cannot escape detection via cache lag.
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=roleclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=roleclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=roleclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile implements the RoleClaim reconcile loop.
func (r *RoleClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var claim cnpgclaimv1alpha1.RoleClaim
	if err := r.Get(ctx, req.NamespacedName, &claim); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !claim.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &claim)
	}

	if !controllerutil.ContainsFinalizer(&claim, RoleClaimFinalizer) {
		controllerutil.AddFinalizer(&claim, RoleClaimFinalizer)
		if err := r.Update(ctx, &claim); err != nil {
			return ctrl.Result{}, err
		}
	}

	return r.reconcileNormal(ctx, &claim)
}

func (r *RoleClaimReconciler) reconcileNormal(ctx context.Context, claim *cnpgclaimv1alpha1.RoleClaim) (ctrl.Result, error) {
	claim.Status.Phase = cnpgclaimv1alpha1.RoleClaimPhaseProvisioning

	// Resolve parent DBClaim.
	var dbClaim cnpgclaimv1alpha1.DatabaseClaim
	dbClaimKey := types.NamespacedName{Name: claim.Spec.DatabaseClaimRef.Name, Namespace: claim.Namespace}
	if err := r.Get(ctx, dbClaimKey, &dbClaim); err != nil {
		if apierrors.IsNotFound(err) {
			setCondition(&claim.Status.Conditions, claim.Generation, ConditionDatabaseClaimResolved, metav1.ConditionFalse, ReasonDatabaseClaimMissing,
				fmt.Sprintf("DatabaseClaim %q not found in namespace", claim.Spec.DatabaseClaimRef.Name))
			return r.failPending(ctx, claim)
		}
		return ctrl.Result{}, err
	}
	if !isDBClaimReady(&dbClaim) {
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionDatabaseClaimResolved, metav1.ConditionFalse, ReasonDatabaseNotReady,
			"DatabaseClaim is not Ready")
		return r.failPending(ctx, claim)
	}
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionDatabaseClaimResolved, metav1.ConditionTrue, ReasonProvisioned, "")

	// Resolve target schemas from sugar or explicit form.
	resolved, err := resolveSchemas(claim, &dbClaim)
	if err != nil {
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionRoleReady, metav1.ConditionFalse, ReasonUnknownSchema, err.Error())
		return r.failPending(ctx, claim)
	}

	// Owner-conflict check across sibling RoleClaims. Use APIReader (not the
	// cache) so a sibling Create()-d milliseconds ago is still visible. The
	// cache field index is not available here because APIReader talks to the
	// apiserver, which doesn't know about controller-runtime indexes — list
	// by namespace and filter in memory.
	siblings, err := r.siblingRoleClaims(ctx, claim.Namespace, dbClaim.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if conflict := findOwnerConflict(claim, resolved, siblings, &dbClaim); conflict != "" {
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionRoleReady, metav1.ConditionFalse, ReasonOwnerConflict, conflict)
		return r.failPending(ctx, claim)
	}

	// Resolve cluster connection.
	target, err := cnpgresolver.Resolve(ctx, r.Client, dbClaim.Spec.ClusterRef.Name, dbClaim.Spec.ClusterRef.Namespace)
	if err != nil {
		reason := resolveErrorReason(err)
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, reason, err.Error())
		return r.failPending(ctx, claim)
	}

	// Materialize Secret (password is generated once, on first reconcile).
	roleName := resolvedRoleName(claim)
	secretName := claim.Name + "-credentials"
	password, err := r.ensureSecret(ctx, claim, secretName, secretpkg.Credentials{
		Host:   target.Host,
		Port:   target.Port,
		DBName: dbClaim.Spec.DatabaseName,
		User:   roleName,
	})
	if err != nil {
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionSecretReady, metav1.ConditionFalse, ReasonReconcileFailed, err.Error())
		return r.failPending(ctx, claim)
	}
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionSecretReady, metav1.ConditionTrue, ReasonProvisioned, "")

	// Drift-revoke BEFORE applying desired grants so permission narrowing
	// (e.g. ReadWrite -> ReadOnly on a schema) converges. revokeDrift catches
	// both dropped schemas and access-level changes.
	if err := r.revokeDrift(ctx, &dbClaim, target, roleName, claim.Status.ResolvedSchemas, resolved); err != nil {
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionRoleReady, metav1.ConditionFalse, ReasonReconcileFailed, err.Error())
		return r.failPending(ctx, claim)
	}

	// Apply role and grants. applyRole computes the new default-priv universe
	// and reconciles it against claim.Status.AppliedDefaultPrivileges,
	// returning the new list (or nil on no-op) so we can persist it below.
	newAppliedDefaultPrivs, err := r.applyRole(ctx, &dbClaim, target, claim, roleName, password, resolved)
	if err != nil {
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionRoleReady, metav1.ConditionFalse, ReasonReconcileFailed, err.Error())
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonReconcileFailed, err.Error())
		claim.Status.Phase = cnpgclaimv1alpha1.RoleClaimPhaseFailed
		if statusErr := r.Status().Update(ctx, claim); statusErr != nil {
			return ctrl.Result{}, errors.Join(err, fmt.Errorf("status update after apply error: %w", statusErr))
		}
		return ctrl.Result{}, err
	}

	claim.Status.RoleName = roleName
	claim.Status.CredentialsSecretName = secretName
	claim.Status.ResolvedSchemas = resolved
	claim.Status.AppliedDefaultPrivileges = newAppliedDefaultPrivs
	claim.Status.ObservedGeneration = claim.Generation
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionRoleReady, metav1.ConditionTrue, ReasonProvisioned, "")
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionTrue, ReasonProvisioned, "")
	claim.Status.Phase = cnpgclaimv1alpha1.RoleClaimPhaseReady

	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// applyRole opens a DB connection and runs the role provisioning + grant
// statements, including the reflexive default-privileges fan-out across
// siblings. Returns the new AppliedDefaultPrivileges set the caller should
// persist to status after this function returns nil.
func (r *RoleClaimReconciler) applyRole(
	ctx context.Context,
	dbClaim *cnpgclaimv1alpha1.DatabaseClaim,
	target *cnpgresolver.ClusterTarget,
	claim *cnpgclaimv1alpha1.RoleClaim,
	roleName, password string,
	resolved []cnpgclaimv1alpha1.SchemaGrant,
) ([]cnpgclaimv1alpha1.DefaultPrivilegeGrant, error) {
	// Roles live cluster-wide; we connect to the target DB so that
	// schema/table grants resolve in the right namespace.
	dbConn, err := postgres.Open(ctx, target.ConnOpts(dbClaim.Spec.DatabaseName))
	if err != nil {
		return nil, err
	}
	defer dbConn.Close(ctx)

	connLimit := int32(-1)
	if claim.Spec.ConnectionLimit != nil {
		connLimit = *claim.Spec.ConnectionLimit
	}
	if err := postgres.EnsureRole(ctx, dbConn, roleName, password, connLimit); err != nil {
		return nil, err
	}
	if err := postgres.GrantConnect(ctx, dbConn, dbClaim.Spec.DatabaseName, roleName); err != nil {
		return nil, err
	}

	// Sugar form with Access=Owner also transfers DB ownership.
	if claim.Spec.Access != nil && *claim.Spec.Access == cnpgclaimv1alpha1.AccessOwner {
		if err := postgres.AlterDatabaseOwner(ctx, dbConn, dbClaim.Spec.DatabaseName, roleName); err != nil {
			return nil, err
		}
	}

	for _, grant := range resolved {
		if grant.Access == cnpgclaimv1alpha1.AccessOwner {
			if err := postgres.AlterSchemaOwner(ctx, dbConn, grant.Name, roleName); err != nil {
				return nil, err
			}
		}
		if err := postgres.ApplySchemaGrants(ctx, dbConn, roleName, grant.Name, postgres.AccessLevel(grant.Access)); err != nil {
			return nil, err
		}
	}

	return r.applyReflex(ctx, dbConn, claim, dbClaim, roleName, resolved)
}

// applyReflex implements the per-schema default-privileges fan-out across all
// sibling RoleClaims of the same DBClaim, and reconciles
// claim.Status.AppliedDefaultPrivileges by revoking pairs that are no longer
// in the intended universe.
//
// Returns the new "as-reader" set the caller should persist to status after
// this function returns nil. Status is NOT updated here — that happens only
// after every revoke and grant succeeds, so a mid-flight failure leaves the
// previous list intact for the next reconcile to retry.
func (r *RoleClaimReconciler) applyReflex(
	ctx context.Context,
	dbConn *pgx.Conn,
	claim *cnpgclaimv1alpha1.RoleClaim,
	dbClaim *cnpgclaimv1alpha1.DatabaseClaim,
	thisRole string,
	thisGrants []cnpgclaimv1alpha1.SchemaGrant,
) ([]cnpgclaimv1alpha1.DefaultPrivilegeGrant, error) {
	siblings, err := roleClaimsReferencingDBClaim(ctx, r.Client, claim.Namespace, dbClaim.Name)
	if err != nil {
		return nil, err
	}

	tuples := buildDefaultPrivilegeTuples(claim.UID, thisRole, thisGrants, siblings)

	// Compute the universe of (writer, reader, schema) pairs and the subset
	// where THIS claim is the reader — the part we record in status.
	newAsReader := defaultPrivilegesAsReader(tuples, thisRole)

	// Diff against the previously-applied as-reader set (sorted defensively
	// in case status was written by an older binary). Revoke any pair that
	// dropped out. Failures abort without updating status; the next reconcile
	// retries the same diff from the unchanged status.
	for _, g := range defaultPrivilegesToRevoke(claim.Status.AppliedDefaultPrivileges, newAsReader) {
		if err := postgres.AlterDefaultPrivilegesRevokeSelect(ctx, dbConn, g.Writer, thisRole, g.Schema); err != nil {
			return nil, err
		}
	}

	// Apply current grants (idempotent against pg_default_acl). The full
	// universe is re-emitted on every reconcile, including pairs that don't
	// involve this claim — that's how a sibling reconcile reseeds entries
	// after a sibling-side downgrade.
	for _, writer := range tuples {
		if writer.access != cnpgclaimv1alpha1.AccessOwner && writer.access != cnpgclaimv1alpha1.AccessReadWrite {
			continue
		}
		for _, reader := range tuples {
			if reader.access != cnpgclaimv1alpha1.AccessReadOnly {
				continue
			}
			if reader.schema != writer.schema || reader.role == writer.role {
				continue
			}
			if err := postgres.AlterDefaultPrivilegesGrantSelect(ctx, dbConn, writer.role, reader.role, writer.schema); err != nil {
				return nil, err
			}
		}
	}
	return newAsReader, nil
}

type defaultPrivilegeTuple struct {
	role   string
	schema string
	access cnpgclaimv1alpha1.AccessLevel
}

func buildDefaultPrivilegeTuples(
	claimUID types.UID,
	thisRole string,
	thisGrants []cnpgclaimv1alpha1.SchemaGrant,
	siblings []cnpgclaimv1alpha1.RoleClaim,
) []defaultPrivilegeTuple {
	var tuples []defaultPrivilegeTuple
	addClaim := func(name string, grants []cnpgclaimv1alpha1.SchemaGrant) {
		for _, g := range grants {
			tuples = append(tuples, defaultPrivilegeTuple{role: name, schema: g.Name, access: g.Access})
		}
	}
	addClaim(thisRole, thisGrants)
	for i := range siblings {
		s := &siblings[i]
		if s.UID == claimUID {
			continue
		}
		if s.Status.RoleName == "" || len(s.Status.ResolvedSchemas) == 0 {
			continue // not yet provisioned; reflex will trigger again on its reconcile
		}
		addClaim(s.Status.RoleName, s.Status.ResolvedSchemas)
	}
	return tuples
}

func defaultPrivilegesAsReader(tuples []defaultPrivilegeTuple, thisRole string) []cnpgclaimv1alpha1.DefaultPrivilegeGrant {
	out := []cnpgclaimv1alpha1.DefaultPrivilegeGrant{}
	for _, writer := range tuples {
		if writer.access != cnpgclaimv1alpha1.AccessOwner && writer.access != cnpgclaimv1alpha1.AccessReadWrite {
			continue
		}
		for _, reader := range tuples {
			if reader.access != cnpgclaimv1alpha1.AccessReadOnly {
				continue
			}
			if reader.schema != writer.schema || reader.role == writer.role {
				continue
			}
			if reader.role == thisRole {
				out = append(out, cnpgclaimv1alpha1.DefaultPrivilegeGrant{
					Writer: writer.role,
					Schema: writer.schema,
				})
			}
		}
	}
	sortDefaultPrivilegeGrants(out)
	return out
}

func sortedDefaultPrivilegeGrantsCopy(in []cnpgclaimv1alpha1.DefaultPrivilegeGrant) []cnpgclaimv1alpha1.DefaultPrivilegeGrant {
	out := append([]cnpgclaimv1alpha1.DefaultPrivilegeGrant{}, in...)
	sortDefaultPrivilegeGrants(out)
	return out
}

func defaultPrivilegesToRevoke(
	previous,
	current []cnpgclaimv1alpha1.DefaultPrivilegeGrant,
) []cnpgclaimv1alpha1.DefaultPrivilegeGrant {
	previous = sortedDefaultPrivilegeGrantsCopy(previous)
	current = sortedDefaultPrivilegeGrantsCopy(current)
	currentSet := sets.New[string]()
	for _, g := range current {
		currentSet.Insert(defaultPrivKey(g))
	}
	out := make([]cnpgclaimv1alpha1.DefaultPrivilegeGrant, 0, len(previous))
	for _, g := range previous {
		if !currentSet.Has(defaultPrivKey(g)) {
			out = append(out, g)
		}
	}
	return out
}

// defaultPrivKey is the composite key used to diff DefaultPrivilegeGrant lists.
func defaultPrivKey(g cnpgclaimv1alpha1.DefaultPrivilegeGrant) string {
	return g.Writer + "\x00" + g.Schema
}

func sortDefaultPrivilegeGrants(g []cnpgclaimv1alpha1.DefaultPrivilegeGrant) {
	slices.SortFunc(g, func(a, b cnpgclaimv1alpha1.DefaultPrivilegeGrant) int {
		if c := cmp.Compare(a.Writer, b.Writer); c != 0 {
			return c
		}
		return cmp.Compare(a.Schema, b.Schema)
	})
}

// revokeDrift compares previously-applied grants (from status.resolvedSchemas)
// to the newly resolved set and revokes grants for removed schemas or schemas
// whose access level changed. Ownership transfers are not reversed here.
func (r *RoleClaimReconciler) revokeDrift(
	ctx context.Context,
	dbClaim *cnpgclaimv1alpha1.DatabaseClaim,
	target *cnpgresolver.ClusterTarget,
	roleName string,
	previous, current []cnpgclaimv1alpha1.SchemaGrant,
) error {
	toRevoke := schemasNeedingRevoke(previous, current)
	if len(toRevoke) == 0 {
		return nil
	}
	dbConn, err := postgres.Open(ctx, target.ConnOpts(dbClaim.Spec.DatabaseName))
	if err != nil {
		return err
	}
	defer dbConn.Close(ctx)
	for _, schema := range toRevoke {
		if err := postgres.RevokeAllOnSchema(ctx, dbConn, roleName, schema); err != nil {
			return err
		}
	}
	return nil
}

// ensureSecret writes or reuses the credentials Secret using CreateOrUpdate.
// The password in the existing Secret is authoritative: if present it is
// reused; otherwise a fresh one is generated. The Secret is always written
// before the Postgres role is created so that, on partial failure, the next
// reconcile reuses the same password.
//
// Only Secret.Data is treated as authoritative for reconcile decisions
// (Labels and OwnerReferences are reasserted but not compared); a human or
// another controller editing those is tolerated.
func (r *RoleClaimReconciler) ensureSecret(
	ctx context.Context,
	claim *cnpgclaimv1alpha1.RoleClaim,
	name string,
	creds secretpkg.Credentials,
) (string, error) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: claim.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if existing := secret.Data["password"]; len(existing) > 0 {
			creds.Password = string(existing)
		}
		if creds.Password == "" {
			pw, err := secretpkg.GeneratePassword()
			if err != nil {
				return err
			}
			creds.Password = pw
		}
		if secret.Labels == nil {
			secret.Labels = map[string]string{}
		}
		secret.Labels[roleClaimSecretLabel] = claim.Name
		secret.Type = corev1.SecretTypeOpaque
		desired := creds.Data()
		if maps.EqualFunc(secret.Data, desired, func(a, b []byte) bool { return string(a) == string(b) }) {
			// No-op: leaves resourceVersion unchanged and skips the update.
			return controllerutil.SetControllerReference(claim, secret, r.Scheme)
		}
		secret.Data = desired
		return controllerutil.SetControllerReference(claim, secret, r.Scheme)
	}); err != nil {
		return "", err
	}
	return creds.Password, nil
}

// resolveSchemas turns spec.access (sugar) or spec.schemas (explicit form)
// into a canonical, sorted list of (schema, access) grants. Returns an error
// if the explicit form references a schema not declared on the DBClaim.
func resolveSchemas(claim *cnpgclaimv1alpha1.RoleClaim, dbClaim *cnpgclaimv1alpha1.DatabaseClaim) ([]cnpgclaimv1alpha1.SchemaGrant, error) {
	declared := sets.New[string](dbClaim.Spec.Schemas...)
	if claim.Spec.Access != nil {
		out := make([]cnpgclaimv1alpha1.SchemaGrant, 0, len(dbClaim.Spec.Schemas))
		for _, s := range dbClaim.Spec.Schemas {
			out = append(out, cnpgclaimv1alpha1.SchemaGrant{Name: s, Access: *claim.Spec.Access})
		}
		sortGrants(out)
		return out, nil
	}
	out := make([]cnpgclaimv1alpha1.SchemaGrant, 0, len(claim.Spec.Schemas))
	for _, g := range claim.Spec.Schemas {
		if !declared.Has(g.Name) {
			return nil, fmt.Errorf("schema %q is not declared in DatabaseClaim %q", g.Name, dbClaim.Name)
		}
		out = append(out, g)
	}
	sortGrants(out)
	return out, nil
}

func sortGrants(g []cnpgclaimv1alpha1.SchemaGrant) {
	slices.SortFunc(g, func(a, b cnpgclaimv1alpha1.SchemaGrant) int {
		return cmp.Compare(a.Name, b.Name)
	})
}

// schemasNeedingRevoke returns schema names whose previously-applied grants
// must be cleared before applying the current desired state: schemas removed
// from the claim, plus schemas whose access level changed.
func schemasNeedingRevoke(previous, current []cnpgclaimv1alpha1.SchemaGrant) []string {
	desired := map[string]cnpgclaimv1alpha1.AccessLevel{}
	for _, g := range current {
		desired[g.Name] = g.Access
	}
	out := make([]string, 0, len(previous))
	for _, p := range previous {
		access, ok := desired[p.Name]
		if !ok || access != p.Access {
			out = append(out, p.Name)
		}
	}
	return out
}

// findOwnerConflict returns a non-empty message if another RoleClaim already
// owns one of the schemas this claim is trying to own. Siblings are evaluated
// against their Spec (projected to grants via resolveSchemas), not their
// Status, so a freshly-created sibling whose status is still empty is still
// considered. Tiebreak is deterministic: earlier CreationTimestamp wins, with
// lower UID as the tie-breaker.
func findOwnerConflict(
	claim *cnpgclaimv1alpha1.RoleClaim,
	resolved []cnpgclaimv1alpha1.SchemaGrant,
	siblings []cnpgclaimv1alpha1.RoleClaim,
	dbClaim *cnpgclaimv1alpha1.DatabaseClaim,
) string {
	ownsHere := sets.New[string]()
	for _, g := range resolved {
		if g.Access == cnpgclaimv1alpha1.AccessOwner {
			ownsHere.Insert(g.Name)
		}
	}
	if ownsHere.Len() == 0 {
		return ""
	}
	for i := range siblings {
		s := &siblings[i]
		if s.UID == claim.UID {
			continue
		}
		if !s.DeletionTimestamp.IsZero() {
			continue
		}
		if !siblingWins(s, claim) {
			continue
		}
		siblingGrants, err := resolveSchemas(s, dbClaim)
		if err != nil {
			// Sibling spec is itself invalid; its own reconcile will surface
			// the problem. Don't let it block us.
			continue
		}
		for _, sg := range siblingGrants {
			if sg.Access == cnpgclaimv1alpha1.AccessOwner && ownsHere.Has(sg.Name) {
				return fmt.Sprintf("RoleClaim %q already claims ownership of schema %q", s.Name, sg.Name)
			}
		}
	}
	return ""
}

// siblingWins reports whether s wins ownership against claim under the strict
// tiebreak: earlier CreationTimestamp first, lower UID on tie.
func siblingWins(s, claim *cnpgclaimv1alpha1.RoleClaim) bool {
	if s.CreationTimestamp.Before(&claim.CreationTimestamp) {
		return true
	}
	if claim.CreationTimestamp.Before(&s.CreationTimestamp) {
		return false
	}
	return string(s.UID) < string(claim.UID)
}

// resolvedRoleName returns the Postgres role name for the claim. spec.roleName
// is required and pattern-validated by the CRD, so there is nothing to derive.
func resolvedRoleName(claim *cnpgclaimv1alpha1.RoleClaim) string {
	return claim.Spec.RoleName
}

// isDBClaimReady reports whether the parent DBClaim's Ready condition is True
// for its current generation. Trusting a stale True (observedGeneration <
// generation) would let the RoleClaim race ahead of an in-flight spec change.
func isDBClaimReady(dc *cnpgclaimv1alpha1.DatabaseClaim) bool {
	cond := meta.FindStatusCondition(dc.Status.Conditions, ConditionReady)
	return cond != nil && cond.Status == metav1.ConditionTrue && cond.ObservedGeneration == dc.Generation
}

// resolveErrorReason maps a cluster-resolve error to the Condition Reason that
// should accompany it on the Ready (and ClusterResolved) condition, so the
// top-level Ready reason is not unconditionally ClusterMissing.
func resolveErrorReason(err error) string {
	switch {
	case errors.Is(err, cnpgresolver.ErrClusterNotFound):
		return ReasonClusterMissing
	case errors.Is(err, cnpgresolver.ErrClusterNotReady):
		return ReasonClusterNotReady
	default:
		return ReasonResolveFailed
	}
}

// siblingRoleClaims lists RoleClaims in the namespace via APIReader (uncached)
// and filters in memory. APIReader does not support cache field indexes, so
// we accept the linear filter on a typically-small per-namespace cardinality.
func (r *RoleClaimReconciler) siblingRoleClaims(ctx context.Context, namespace, dbClaimName string) ([]cnpgclaimv1alpha1.RoleClaim, error) {
	if r.APIReader == nil {
		// Fallback for older constructions (e.g. some tests) — should not
		// happen under SetupWithManager. Use the cache.
		return roleClaimsReferencingDBClaim(ctx, r.Client, namespace, dbClaimName)
	}
	var list cnpgclaimv1alpha1.RoleClaimList
	if err := r.APIReader.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	out := make([]cnpgclaimv1alpha1.RoleClaim, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Spec.DatabaseClaimRef.Name == dbClaimName {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// reconcileDelete handles role drop + finalizer release.
func (r *RoleClaimReconciler) reconcileDelete(ctx context.Context, claim *cnpgclaimv1alpha1.RoleClaim) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(claim, RoleClaimFinalizer) {
		return ctrl.Result{}, nil
	}
	claim.Status.Phase = cnpgclaimv1alpha1.RoleClaimPhaseTerminating

	// Look up parent for connection + reassign target.
	var dbClaim cnpgclaimv1alpha1.DatabaseClaim
	dbClaimKey := types.NamespacedName{Name: claim.Spec.DatabaseClaimRef.Name, Namespace: claim.Namespace}
	dbErr := r.Get(ctx, dbClaimKey, &dbClaim)

	if dbErr == nil {
		target, err := cnpgresolver.Resolve(ctx, r.Client, dbClaim.Spec.ClusterRef.Name, dbClaim.Spec.ClusterRef.Namespace)
		if err == nil {
			if dropErr := r.dropRole(ctx, &dbClaim, target, claim); dropErr != nil {
				log.FromContext(ctx).Error(dropErr, "drop role failed")
				return ctrl.Result{}, dropErr
			}
		} else if !isClusterGoneGracePeriodPassed(claim, err) {
			return ctrl.Result{}, err
		}
	} else if !apierrors.IsNotFound(dbErr) {
		return ctrl.Result{}, dbErr
	}
	// If dbClaim is gone or cluster gone past grace, just release the finalizer.

	controllerutil.RemoveFinalizer(claim, RoleClaimFinalizer)
	if err := r.Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func isClusterGoneGracePeriodPassed(claim *cnpgclaimv1alpha1.RoleClaim, err error) bool {
	if !errors.Is(err, cnpgresolver.ErrClusterNotFound) &&
		!errors.Is(err, cnpgresolver.ErrSuperUserSecretMissing) {
		return false
	}
	if claim.DeletionTimestamp == nil {
		return false
	}
	return time.Since(claim.DeletionTimestamp.Time) > clusterGoneGracePeriod
}

// dropRole tears the Postgres role down. DROP OWNED BY removes default-priv
// entries where the role is grantee, so AppliedDefaultPrivileges is implicitly
// cleaned up by the same SQL — no separate REVOKE loop is needed here.
func (r *RoleClaimReconciler) dropRole(
	ctx context.Context,
	dbClaim *cnpgclaimv1alpha1.DatabaseClaim,
	target *cnpgresolver.ClusterTarget,
	claim *cnpgclaimv1alpha1.RoleClaim,
) error {
	roleName := claim.Status.RoleName
	if roleName == "" {
		roleName = resolvedRoleName(claim)
	}
	if err := postgres.ValidateIdentifier(roleName); err != nil {
		return nil // never provisioned with a valid name; nothing to drop
	}
	dbConn, err := postgres.Open(ctx, target.ConnOpts(dbClaim.Spec.DatabaseName))
	if err != nil {
		return err
	}
	defer dbConn.Close(ctx)
	return postgres.DropRole(ctx, dbConn, roleName, target.SuperUser)
}

// failPending writes the current conditions, marks the claim Pending, and
// asks for a requeue.
func (r *RoleClaimReconciler) failPending(ctx context.Context, claim *cnpgclaimv1alpha1.RoleClaim) (ctrl.Result, error) {
	claim.Status.Phase = cnpgclaimv1alpha1.RoleClaimPhasePending
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// SetupWithManager wires up the controller. Field indexes must be installed
// once per manager via SetupFieldIndexes before this is called.
func (r *RoleClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&cnpgclaimv1alpha1.RoleClaim{}).
		Owns(&corev1.Secret{}).
		Watches(
			&cnpgclaimv1alpha1.DatabaseClaim{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForDBClaim),
		).
		Watches(
			&cnpgclaimv1alpha1.RoleClaim{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForSiblings),
		).
		Complete(r)
}

func (r *RoleClaimReconciler) requestsForDBClaim(ctx context.Context, obj client.Object) []reconcile.Request {
	dc, ok := obj.(*cnpgclaimv1alpha1.DatabaseClaim)
	if !ok {
		return nil
	}
	return requestsForDBClaimChildren(ctx, r.Client, dc)
}

func (r *RoleClaimReconciler) requestsForSiblings(ctx context.Context, obj client.Object) []reconcile.Request {
	rc, ok := obj.(*cnpgclaimv1alpha1.RoleClaim)
	if !ok {
		return nil
	}
	return requestsForRoleClaimSiblings(ctx, r.Client, rc)
}
