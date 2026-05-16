/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	cnpgresolver "github.com/wyvernzora/cnpg-dbclaim-operator/internal/cnpg"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/postgres"
)

// DatabaseClaimReconciler reconciles DatabaseClaim resources.
type DatabaseClaimReconciler struct {
	client.Client
	APIReader client.Reader
	Recorder  events.EventRecorder
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=databaseclaims,verbs=get;list;watch;update
// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=databaseclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=databaseclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=roleclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile implements the DatabaseClaim reconcile loop.
func (r *DatabaseClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var claim cnpgclaimv1alpha1.DatabaseClaim
	if err := r.Get(ctx, req.NamespacedName, &claim); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !claim.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &claim)
	}

	if !controllerutil.ContainsFinalizer(&claim, DatabaseClaimFinalizer) {
		controllerutil.AddFinalizer(&claim, DatabaseClaimFinalizer)
		if err := updateFinalizers(ctx, r.Client, &claim); err != nil {
			return ctrl.Result{}, err
		}
	}

	return r.reconcileNormal(ctx, &claim)
}

func (r *DatabaseClaimReconciler) reconcileNormal(ctx context.Context, claim *cnpgclaimv1alpha1.DatabaseClaim) (ctrl.Result, error) {
	claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseProvisioning

	claims, err := r.allDatabaseClaims(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if conflict := findDatabaseNameConflict(claim, claims); conflict != "" {
		eventNeeded := shouldEmitConditionEvent(claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonDatabaseNameConflict)
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionDatabaseReady, metav1.ConditionFalse, ReasonDatabaseNameConflict, conflict)
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonDatabaseNameConflict, conflict)
		claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhasePending
		if err := r.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		if eventNeeded {
			emitEvent(r.Recorder, claim, corev1.EventTypeWarning, ReasonDatabaseNameConflict, conflict)
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	target, err := cnpgresolver.Resolve(ctx, r.Client, claim.Spec.ClusterRef.Name, claim.Spec.ClusterRef.Namespace)
	if err != nil {
		return r.handleResolveError(ctx, claim, err)
	}
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionClusterResolved, metav1.ConditionTrue, ReasonProvisioned, "cluster resolved")

	if err := r.applyDatabase(ctx, claim, target); err != nil {
		eventNeeded := shouldEmitConditionEvent(claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonReconcileFailed)
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionDatabaseReady, metav1.ConditionFalse, ReasonReconcileFailed, err.Error())
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonReconcileFailed, err.Error())
		claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseFailed
		if statusErr := r.Status().Update(ctx, claim); statusErr != nil {
			return ctrl.Result{}, errors.Join(err, fmt.Errorf("status update after apply error: %w", statusErr))
		}
		if eventNeeded {
			emitEvent(r.Recorder, claim, corev1.EventTypeWarning, ReasonReconcileFailed, err.Error())
		}
		return ctrl.Result{}, err
	}

	claim.Status.DatabaseInfo = &cnpgclaimv1alpha1.DatabaseInfo{
		Host:   target.Host,
		Port:   int32(target.Port),
		DBName: claim.Spec.DatabaseName,
	}
	claim.Status.ObservedGeneration = claim.Generation
	provisionedEvent := shouldEmitConditionEvent(claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionTrue, ReasonProvisioned)
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionDatabaseReady, metav1.ConditionTrue, ReasonProvisioned, "database provisioned")
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionTrue, ReasonProvisioned, "")
	claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseReady

	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	if provisionedEvent {
		emitEvent(r.Recorder, claim, corev1.EventTypeNormal, ReasonProvisioned, fmt.Sprintf("database %q provisioned", claim.Spec.DatabaseName))
	}
	return ctrl.Result{}, nil
}

func (r *DatabaseClaimReconciler) handleResolveError(ctx context.Context, claim *cnpgclaimv1alpha1.DatabaseClaim, err error) (ctrl.Result, error) {
	var reason string
	switch {
	case errors.Is(err, cnpgresolver.ErrClusterNotFound):
		reason = ReasonClusterMissing
	case errors.Is(err, cnpgresolver.ErrClusterNotReady):
		reason = ReasonClusterNotReady
	default:
		reason = ReasonResolveFailed
	}
	eventNeeded := shouldEmitConditionEvent(claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, reason)
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionClusterResolved, metav1.ConditionFalse, reason, err.Error())
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, reason, err.Error())
	claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhasePending
	if statusErr := r.Status().Update(ctx, claim); statusErr != nil {
		return ctrl.Result{}, statusErr
	}
	if eventNeeded {
		emitEvent(r.Recorder, claim, corev1.EventTypeWarning, reason, err.Error())
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// applyDatabase opens admin and per-database SQL sessions and runs the
// idempotent setup: CREATE DATABASE, lock down public, CREATE SCHEMA, CREATE
// EXTENSION.
func (r *DatabaseClaimReconciler) applyDatabase(ctx context.Context, claim *cnpgclaimv1alpha1.DatabaseClaim, target *cnpgresolver.ClusterTarget) error {
	adminConn, err := postgres.Open(ctx, target.ConnOpts(postgres.AdminDatabase))
	if err != nil {
		return fmt.Errorf("open admin connection: %w", err)
	}
	defer adminConn.Close(ctx)

	if err := postgres.EnsureDatabase(ctx, adminConn, claim.Spec.DatabaseName); err != nil {
		return err
	}

	dbConn, err := postgres.Open(ctx, target.ConnOpts(claim.Spec.DatabaseName))
	if err != nil {
		return fmt.Errorf("open connection to %s: %w", claim.Spec.DatabaseName, err)
	}
	defer dbConn.Close(ctx)

	if err := postgres.LockDownPublic(ctx, dbConn); err != nil {
		return err
	}
	for _, schema := range claim.Spec.Schemas {
		if err := postgres.EnsureSchema(ctx, dbConn, schema); err != nil {
			return err
		}
	}
	for _, ext := range claim.Spec.Extensions {
		if err := postgres.EnsureExtension(ctx, dbConn, ext); err != nil {
			return err
		}
	}
	return nil
}

// reconcileDelete enforces the refuse-to-orphan / cascade semantics on
// DatabaseClaim deletion, then drops the database when nothing references it.
func (r *DatabaseClaimReconciler) reconcileDelete(ctx context.Context, claim *cnpgclaimv1alpha1.DatabaseClaim) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(claim, DatabaseClaimFinalizer) {
		return ctrl.Result{}, nil
	}

	claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseTerminating

	referrers, err := roleClaimsReferencingDBClaim(ctx, r.Client, claim.Namespace, claim.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	teardownReason := ""
	teardownMessage := ""

	switch claim.Spec.DeletionPolicy {
	case cnpgclaimv1alpha1.DeletionPolicyDelete:
		// Cascade: trigger deletion of all referring RoleClaims; requeue until
		// they have drained their finalizers.
		if len(referrers) > 0 {
			for i := range referrers {
				if referrers[i].DeletionTimestamp.IsZero() {
					if err := r.Delete(ctx, &referrers[i]); err != nil && !apierrors.IsNotFound(err) {
						return ctrl.Result{}, err
					}
				}
			}
			message := fmt.Sprintf("cascading deletion of %d RoleClaim(s)", len(referrers))
			eventNeeded := shouldEmitConditionEvent(claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonReconciling)
			setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonReconciling, message)
			if err := r.Status().Update(ctx, claim); err != nil {
				return ctrl.Result{}, err
			}
			if eventNeeded {
				emitEvent(r.Recorder, claim, corev1.EventTypeNormal, ReasonReconciling, message)
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		// No more referrers — drop the database, then release the finalizer.
		claims, err := r.allDatabaseClaims(ctx)
		if err != nil {
			return ctrl.Result{}, err
		}
		if otherDatabaseClaimOwnsPhysicalDatabase(claim, claims) {
			log.FromContext(ctx).Info("another DatabaseClaim still owns physical database; releasing finalizer without DROP",
				"database", claim.Spec.DatabaseName, "cluster", claim.Spec.ClusterRef)
			teardownReason = ReasonTeardownSkipped
			teardownMessage = fmt.Sprintf("database %q left in place because another DatabaseClaim still owns it", claim.Spec.DatabaseName)
			break
		}
		dropped, err := r.dropDatabase(ctx, claim)
		if err != nil {
			return r.failDelete(ctx, claim, err)
		}
		if dropped {
			teardownReason = ReasonDatabaseDropped
			teardownMessage = fmt.Sprintf("database %q dropped", claim.Spec.DatabaseName)
		} else {
			teardownReason = ReasonTeardownSkipped
			teardownMessage = fmt.Sprintf("database %q teardown skipped after cluster access was unavailable past the grace period", claim.Spec.DatabaseName)
		}
	default:
		// Retain (default): refuse-to-orphan.
		if len(referrers) > 0 {
			names := make([]string, 0, len(referrers))
			for _, rc := range referrers {
				names = append(names, rc.Name)
			}
			message := fmt.Sprintf("RoleClaims still reference this DatabaseClaim: %v", names)
			eventNeeded := shouldEmitConditionEvent(claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonBlockedByRoleClaims)
			setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonBlockedByRoleClaims, message)
			if err := r.Status().Update(ctx, claim); err != nil {
				return ctrl.Result{}, err
			}
			if eventNeeded {
				emitEvent(r.Recorder, claim, corev1.EventTypeWarning, ReasonBlockedByRoleClaims, message)
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// Retain + no referrers: just release the finalizer; do NOT drop the DB.
		teardownReason = ReasonDatabaseRetained
		teardownMessage = fmt.Sprintf("database %q retained", claim.Spec.DatabaseName)
	}

	controllerutil.RemoveFinalizer(claim, DatabaseClaimFinalizer)
	if err := updateFinalizers(ctx, r.Client, claim); err != nil {
		return ctrl.Result{}, err
	}
	emitEvent(r.Recorder, claim, corev1.EventTypeNormal, teardownReason, teardownMessage)
	return ctrl.Result{}, nil
}

func (r *DatabaseClaimReconciler) failDelete(ctx context.Context, claim *cnpgclaimv1alpha1.DatabaseClaim, err error) (ctrl.Result, error) {
	message := err.Error()
	eventNeeded := shouldEmitDeleteFailureEvent(claim.Status.Conditions, claim.Generation, claim.Status.Phase == cnpgclaimv1alpha1.DatabaseClaimPhaseTerminating)
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonReconcileFailed, message)
	claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseTerminating
	if statusErr := r.Status().Update(ctx, claim); statusErr != nil {
		return ctrl.Result{}, errors.Join(err, fmt.Errorf("status update after delete error: %w", statusErr))
	}
	if eventNeeded {
		emitEvent(r.Recorder, claim, corev1.EventTypeWarning, ReasonReconcileFailed, message)
	}
	return ctrl.Result{}, err
}

func (r *DatabaseClaimReconciler) dropDatabase(ctx context.Context, claim *cnpgclaimv1alpha1.DatabaseClaim) (bool, error) {
	target, err := cnpgresolver.Resolve(ctx, r.Client, claim.Spec.ClusterRef.Name, claim.Spec.ClusterRef.Namespace)
	if err != nil {
		if errors.Is(err, cnpgresolver.ErrClusterNotFound) || errors.Is(err, cnpgresolver.ErrSuperUserSecretMissing) {
			// Cluster gone: fail open after grace period.
			if claim.DeletionTimestamp != nil && time.Since(claim.DeletionTimestamp.Time) > clusterGoneGracePeriod {
				log.FromContext(ctx).Info("cluster gone past grace period; releasing finalizer without DROP",
					"cluster", claim.Spec.ClusterRef)
				return false, nil
			}
		}
		return false, err
	}
	adminConn, err := postgres.Open(ctx, target.ConnOpts(postgres.AdminDatabase))
	if err != nil {
		return false, err
	}
	defer adminConn.Close(ctx)
	if err := postgres.TerminateBackends(ctx, adminConn, claim.Spec.DatabaseName); err != nil {
		return false, err
	}
	if err := postgres.DropDatabase(ctx, adminConn, claim.Spec.DatabaseName); err != nil {
		return false, err
	}
	return true, nil
}

// SetupWithManager wires up the controller. Field indexes must be installed
// once per manager via SetupFieldIndexes before this is called.
func (r *DatabaseClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("databaseclaim-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&cnpgclaimv1alpha1.DatabaseClaim{}).
		Watches(
			&cnpgv1.Cluster{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForCluster),
		).
		Watches(
			&cnpgclaimv1alpha1.RoleClaim{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForRoleClaim),
		).
		Complete(r)
}

func (r *DatabaseClaimReconciler) allDatabaseClaims(ctx context.Context) ([]cnpgclaimv1alpha1.DatabaseClaim, error) {
	if r.APIReader != nil {
		return listDatabaseClaims(ctx, r.APIReader)
	}
	return listDatabaseClaims(ctx, r.Client)
}

// requestsForCluster maps a Cluster change into reconciles of all
// DatabaseClaims that reference it. Backed by the FieldIndexer.
func (r *DatabaseClaimReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	items, err := dbClaimsReferencingCluster(ctx, r.Client, obj.GetNamespace(), obj.GetName())
	if err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(items))
	for _, item := range items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKey{Name: item.Name, Namespace: item.Namespace}})
	}
	return out
}

// requestsForRoleClaim maps a RoleClaim change into a reconcile of its parent
// DatabaseClaim — used so that deletion-blocking decisions stay current as
// RoleClaims come and go.
func (r *DatabaseClaimReconciler) requestsForRoleClaim(_ context.Context, obj client.Object) []reconcile.Request {
	rc, ok := obj.(*cnpgclaimv1alpha1.RoleClaim)
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: rc.Spec.DatabaseClaimRef.Name, Namespace: rc.Namespace}}}
}
