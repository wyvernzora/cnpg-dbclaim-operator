/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package controller hosts the DatabaseClaim and RoleClaim reconcilers.
package controller

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

// Finalizers used by the two reconcilers.
const (
	DatabaseClaimFinalizer = "cnpg.wyvernzora.io/databaseclaim"
	RoleClaimFinalizer     = "cnpg.wyvernzora.io/roleclaim"
)

// Standard condition types.
const (
	ConditionReady                 = "Ready"
	ConditionClusterResolved       = "ClusterResolved"
	ConditionDatabaseReady         = "DatabaseReady"
	ConditionDatabaseClaimResolved = "DatabaseClaimResolved"
	ConditionRoleReady             = "RoleReady"
	ConditionSecretReady           = "SecretReady"
)

// Standard reasons.
const (
	ReasonProvisioned            = "Provisioned"
	ReasonReconciling            = "Reconciling"
	ReasonReconcileFailed        = "ReconcileFailed"
	ReasonResolveFailed          = "ResolveFailed"
	ReasonClusterMissing         = "ClusterMissing"
	ReasonClusterNotReady        = "ClusterNotReady"
	ReasonSuperuserSecretMissing = "SuperuserSecretMissing"
	ReasonDatabaseClaimMissing   = "DatabaseClaimMissing"
	ReasonDatabaseNotReady       = "DatabaseNotReady"
	ReasonUnknownSchema          = "UnknownSchema"
	ReasonOwnerConflict          = "OwnerConflict"
	ReasonDatabaseNameConflict   = "DatabaseNameConflict"
	ReasonRoleNameConflict       = "RoleNameConflict"
	ReasonBlockedByRoleClaims    = "BlockedByRoleClaims"
	ReasonClaimNotAllowed        = "ClaimNotAllowed"
	// ReasonExtensionRelocationFailed marks a claim whose extension exists in
	// the wrong schema and could not be moved there — in practice a
	// non-relocatable extension, which no amount of retrying will fix.
	ReasonExtensionRelocationFailed = "ExtensionRelocationFailed"
)

// Event-only reasons for lifecycle paths that do not map cleanly to status
// conditions.
const (
	ReasonDatabaseDropped  = "DatabaseDropped"
	ReasonDatabaseRetained = "DatabaseRetained"
	ReasonRoleDropped      = "RoleDropped"
	ReasonTeardownSkipped  = "TeardownSkipped"
)

// clusterGoneGracePeriod is how long we keep retrying a DROP/REASSIGN against
// a missing CNPG cluster before fail-open: releasing the finalizer without
// completing the SQL teardown. Short enough that stuck deletions don't linger,
// long enough to ride out a brief cluster outage.
const clusterGoneGracePeriod = 5 * time.Minute

// updateFinalizers persists metadata.finalizers changes.
//
// CRDs do not expose a /finalizers REST subresource. Use a normal object
// update here; client.SubResource("finalizers").Update targets a non-existent
// CRD endpoint and returns NotFound in envtest and real clusters. This normal
// update path requires primary-resource update RBAC in addition to the standard
// <resource>/finalizers rule emitted by Kubebuilder markers.
//
// A whole-object update re-encodes spec.extensions from the stored v0.3.x
// bare-string shape into the object form, and those entries carry no schema —
// a field the CRD now requires — so on an unmigrated legacy claim the update
// is rejected as Invalid. CRD validation ratcheting does not rescue any write
// here: the stored strings fail the item schema outright, so a metadata-only
// patch is rejected too (TestFinalizerReleaseOnLegacyStoredClaim). For a claim
// being deleted, fall back to a JSON merge patch that writes the remaining
// finalizers and clears spec.extensions — acceptable only because
// deprovisioning reads databaseName and deletionPolicy, never extensions. The
// add-finalizer branch deliberately gets no fallback: a live claim keeps its
// spec, fails reconciliation loudly, and is migrated instead (see "Upgrading
// from v0.3.x" in the README). The fallback needs the patch RBAC verb on
// databaseclaims in addition to update.
func updateFinalizers(ctx context.Context, c client.Client, obj client.Object) error {
	err := c.Update(ctx, obj)
	if err == nil || !apierrors.IsInvalid(err) || obj.GetDeletionTimestamp().IsZero() {
		return err
	}
	body, marshalErr := json.Marshal(map[string]any{
		"metadata": map[string]any{"finalizers": obj.GetFinalizers()},
		"spec":     map[string]any{"extensions": nil},
	})
	if marshalErr != nil {
		return errors.Join(err, marshalErr)
	}
	if patchErr := c.Patch(ctx, obj, client.RawPatch(types.MergePatchType, body)); patchErr != nil {
		return errors.Join(err, patchErr)
	}
	return nil
}

// Field-index keys used to register and look up cross-resource references via
// controller-runtime's FieldIndexer, replacing in-process linear scans.
const (
	indexDBClaimByClusterRef   = ".spec.clusterRef.name"
	indexRoleClaimByDBClaimRef = ".spec.databaseClaimRef.name"
)

// SetupFieldIndexes installs the FieldIndexer entries used by both reconcilers.
// Call once per manager before registering controllers — controller-runtime
// rejects duplicate IndexField registrations on the same manager, so this
// cannot live inside per-reconciler SetupWithManager.
func SetupFieldIndexes(ctx context.Context, mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(ctx, &cnpgclaimv1alpha1.DatabaseClaim{}, indexDBClaimByClusterRef,
		func(obj client.Object) []string {
			dc, ok := obj.(*cnpgclaimv1alpha1.DatabaseClaim)
			if !ok {
				return nil
			}
			return []string{dc.Spec.ClusterRef.Name}
		},
	); err != nil {
		return err
	}
	return mgr.GetFieldIndexer().IndexField(ctx, &cnpgclaimv1alpha1.RoleClaim{}, indexRoleClaimByDBClaimRef,
		func(obj client.Object) []string {
			rc, ok := obj.(*cnpgclaimv1alpha1.RoleClaim)
			if !ok {
				return nil
			}
			return []string{rc.Spec.DatabaseClaimRef.Name}
		},
	)
}

// setCondition sets a condition on the given slice, threading observedGeneration.
// Centralises the boilerplate so both reconcilers use the same shape.
func setCondition(conds *[]metav1.Condition, generation int64, t string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               t,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

func shouldEmitConditionEvent(conds []metav1.Condition, generation int64, t string, status metav1.ConditionStatus, reason string) bool {
	cond := meta.FindStatusCondition(conds, t)
	return cond == nil ||
		cond.ObservedGeneration != generation ||
		cond.Status != status ||
		cond.Reason != reason
}

func shouldEmitDeleteFailureEvent(conds []metav1.Condition, generation int64, wasTerminating bool) bool {
	return !wasTerminating ||
		shouldEmitConditionEvent(conds, generation, ConditionReady, metav1.ConditionFalse, ReasonReconcileFailed)
}

// eventNoteLimit is the longest note the API server accepts on an
// events.k8s.io/v1 Event (1024 bytes; measured against envtest 1.31.0, accepted
// at 1024 and rejected at 1025). An over-long note is not a truncated event but
// no event at all: the API server rejects it as Invalid and client-go's
// broadcaster drops it without retrying.
//
// The messages that overrun it are the ones that matter most. updateFinalizers
// reports an API-server Invalid error carrying one cause per stored extension —
// past ~22 of them the note is over the limit — and that error is reported on
// the paths where status cannot be written, so the event is the only signal
// left.
const eventNoteLimit = 1024

const eventNoteTruncationSuffix = " [truncated]"

// truncateEventNote cuts a note down to what the API server will accept, on a
// rune boundary so the result is still valid UTF-8.
func truncateEventNote(note string) string {
	if len(note) <= eventNoteLimit {
		return note
	}
	cut := eventNoteLimit - len(eventNoteTruncationSuffix)
	for cut > 0 && !utf8.RuneStart(note[cut]) {
		cut--
	}
	return note[:cut] + eventNoteTruncationSuffix
}

func emitEvent(recorder events.EventRecorder, obj runtime.Object, eventType, reason, message string) {
	if recorder == nil || obj == nil {
		return
	}
	if message == "" {
		message = reason
	}
	recorder.Eventf(obj, nil, eventType, reason, reason, "%s", truncateEventNote(message))
}

// objectWins reports whether a wins deterministic ownership against b:
// earlier CreationTimestamp first, lower UID on tie. All claim ownership
// arbitration uses this same rule so conflicts converge predictably.
func objectWins(a, b client.Object) bool {
	aCreated := a.GetCreationTimestamp()
	bCreated := b.GetCreationTimestamp()
	if aCreated.Before(&bCreated) {
		return true
	}
	if bCreated.Before(&aCreated) {
		return false
	}
	return cmp.Compare(string(a.GetUID()), string(b.GetUID())) < 0
}

func sameObject(a, b client.Object) bool {
	if a.GetUID() != "" && b.GetUID() != "" {
		return a.GetUID() == b.GetUID()
	}
	return a.GetNamespace() == b.GetNamespace() && a.GetName() == b.GetName()
}

func physicalDatabaseKey(dc *cnpgclaimv1alpha1.DatabaseClaim) string {
	return dc.Spec.ClusterRef.Namespace + "\x00" + dc.Spec.ClusterRef.Name + "\x00" + dc.Spec.DatabaseName
}

func samePhysicalDatabase(a, b *cnpgclaimv1alpha1.DatabaseClaim) bool {
	return physicalDatabaseKey(a) == physicalDatabaseKey(b)
}

// findDatabaseNameConflict returns a message if an older non-deleting
// DatabaseClaim already owns the same physical (cluster, databaseName).
func findDatabaseNameConflict(claim *cnpgclaimv1alpha1.DatabaseClaim, claims []cnpgclaimv1alpha1.DatabaseClaim) string {
	for i := range claims {
		other := &claims[i]
		if sameObject(other, claim) {
			continue
		}
		if !other.DeletionTimestamp.IsZero() {
			continue
		}
		if !samePhysicalDatabase(other, claim) {
			continue
		}
		if !objectWins(other, claim) {
			continue
		}
		return fmt.Sprintf("DatabaseClaim %q in namespace %q already owns database %q on Cluster %s/%s",
			other.Name, other.Namespace, claim.Spec.DatabaseName, claim.Spec.ClusterRef.Namespace, claim.Spec.ClusterRef.Name)
	}
	return ""
}

// otherDatabaseClaimOwnsPhysicalDatabase reports whether a non-deleting claim
// other than claim still points at the same physical database. Used to avoid
// dropping shared physical state when deleting stale duplicate claims.
func otherDatabaseClaimOwnsPhysicalDatabase(claim *cnpgclaimv1alpha1.DatabaseClaim, claims []cnpgclaimv1alpha1.DatabaseClaim) bool {
	for i := range claims {
		other := &claims[i]
		if sameObject(other, claim) {
			continue
		}
		if !other.DeletionTimestamp.IsZero() {
			continue
		}
		if samePhysicalDatabase(other, claim) {
			return true
		}
	}
	return false
}

func listDatabaseClaims(ctx context.Context, r client.Reader) ([]cnpgclaimv1alpha1.DatabaseClaim, error) {
	var list cnpgclaimv1alpha1.DatabaseClaimList
	if err := r.List(ctx, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func listRoleClaims(ctx context.Context, r client.Reader) ([]cnpgclaimv1alpha1.RoleClaim, error) {
	var list cnpgclaimv1alpha1.RoleClaimList
	if err := r.List(ctx, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// roleClaimsReferencingDBClaim returns the RoleClaims in the given namespace
// whose spec.databaseClaimRef.name matches dbClaim. Used by the DatabaseClaim
// controller for refuse-to-orphan deletion and by the RoleClaim controller
// for owner-conflict checks and reflex fan-out.
func roleClaimsReferencingDBClaim(ctx context.Context, c client.Client, namespace, dbClaim string) ([]cnpgclaimv1alpha1.RoleClaim, error) {
	var list cnpgclaimv1alpha1.RoleClaimList
	if err := c.List(ctx, &list,
		client.InNamespace(namespace),
		client.MatchingFields{indexRoleClaimByDBClaimRef: dbClaim},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// dbClaimsReferencingCluster returns the DatabaseClaims referencing a Cluster
// (namespace+name). Backed by the FieldIndexer.
func dbClaimsReferencingCluster(ctx context.Context, c client.Client, clusterNamespace, clusterName string) ([]cnpgclaimv1alpha1.DatabaseClaim, error) {
	var list cnpgclaimv1alpha1.DatabaseClaimList
	if err := c.List(ctx, &list, client.MatchingFields{indexDBClaimByClusterRef: clusterName}); err != nil {
		return nil, err
	}
	out := make([]cnpgclaimv1alpha1.DatabaseClaim, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Spec.ClusterRef.Namespace == clusterNamespace {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// requestsForRoleClaimSiblings returns reconcile requests for all RoleClaims
// that reference the same DatabaseClaim as the given RoleClaim. Useful as a
// watch handler that triggers the default-privileges reflex fan-out when a
// sibling RoleClaim changes.
func requestsForRoleClaimSiblings(ctx context.Context, c client.Client, source *cnpgclaimv1alpha1.RoleClaim) []reconcile.Request {
	siblings, err := roleClaimsReferencingDBClaim(ctx, c, source.Namespace, source.Spec.DatabaseClaimRef.Name)
	if err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(siblings))
	for _, s := range siblings {
		if s.Name == source.Name {
			continue
		}
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: s.Name, Namespace: s.Namespace}})
	}
	return out
}

// requestsForDBClaimChildren returns reconcile requests for all RoleClaims
// referencing the given DatabaseClaim. Used as a watch handler on
// DatabaseClaim → wake up child RoleClaims when the parent becomes Ready.
func requestsForDBClaimChildren(ctx context.Context, c client.Client, dbClaim *cnpgclaimv1alpha1.DatabaseClaim) []reconcile.Request {
	children, err := roleClaimsReferencingDBClaim(ctx, c, dbClaim.Namespace, dbClaim.Name)
	if err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(children))
	for _, child := range children {
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: child.Name, Namespace: child.Namespace}})
	}
	return out
}
