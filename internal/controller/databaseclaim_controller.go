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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=databaseclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=databaseclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=databaseclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups=cnpg.wyvernzora.io,resources=roleclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

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
		if err := r.Update(ctx, &claim); err != nil {
			return ctrl.Result{}, err
		}
	}

	return r.reconcileNormal(ctx, &claim)
}

func (r *DatabaseClaimReconciler) reconcileNormal(ctx context.Context, claim *cnpgclaimv1alpha1.DatabaseClaim) (ctrl.Result, error) {
	claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseProvisioning

	target, err := cnpgresolver.Resolve(ctx, r.Client, claim.Spec.ClusterRef.Name, claim.Spec.ClusterRef.Namespace)
	if err != nil {
		return r.handleResolveError(ctx, claim, err)
	}
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionClusterResolved, metav1.ConditionTrue, ReasonProvisioned, "cluster resolved")

	if err := r.applyDatabase(ctx, claim, target); err != nil {
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionDatabaseReady, metav1.ConditionFalse, ReasonReconcileFailed, err.Error())
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonReconcileFailed, err.Error())
		claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseFailed
		if statusErr := r.Status().Update(ctx, claim); statusErr != nil {
			return ctrl.Result{}, errors.Join(err, fmt.Errorf("status update after apply error: %w", statusErr))
		}
		return ctrl.Result{}, err
	}

	claim.Status.DatabaseInfo = &cnpgclaimv1alpha1.DatabaseInfo{
		Host:   target.Host,
		Port:   int32(target.Port),
		DBName: claim.Spec.DatabaseName,
	}
	claim.Status.ObservedGeneration = claim.Generation
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionDatabaseReady, metav1.ConditionTrue, ReasonProvisioned, "database provisioned")
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionTrue, ReasonProvisioned, "")
	claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseReady

	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
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
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionClusterResolved, metav1.ConditionFalse, reason, err.Error())
	setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, reason, err.Error())
	claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhasePending
	if statusErr := r.Status().Update(ctx, claim); statusErr != nil {
		return ctrl.Result{}, statusErr
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
			setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonReconciling,
				fmt.Sprintf("cascading deletion of %d RoleClaim(s)", len(referrers)))
			if err := r.Status().Update(ctx, claim); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		// No more referrers — drop the database, then release the finalizer.
		if err := r.dropDatabase(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
	default:
		// Retain (default): refuse-to-orphan.
		if len(referrers) > 0 {
			names := make([]string, 0, len(referrers))
			for _, rc := range referrers {
				names = append(names, rc.Name)
			}
			setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionFalse, ReasonBlockedByRoleClaims,
				fmt.Sprintf("RoleClaims still reference this DatabaseClaim: %v", names))
			if err := r.Status().Update(ctx, claim); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// Retain + no referrers: just release the finalizer; do NOT drop the DB.
	}

	controllerutil.RemoveFinalizer(claim, DatabaseClaimFinalizer)
	if err := r.Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *DatabaseClaimReconciler) dropDatabase(ctx context.Context, claim *cnpgclaimv1alpha1.DatabaseClaim) error {
	target, err := cnpgresolver.Resolve(ctx, r.Client, claim.Spec.ClusterRef.Name, claim.Spec.ClusterRef.Namespace)
	if err != nil {
		if errors.Is(err, cnpgresolver.ErrClusterNotFound) || errors.Is(err, cnpgresolver.ErrSuperUserSecretMissing) {
			// Cluster gone: fail open after grace period.
			if claim.DeletionTimestamp != nil && time.Since(claim.DeletionTimestamp.Time) > clusterGoneGracePeriod {
				log.FromContext(ctx).Info("cluster gone past grace period; releasing finalizer without DROP",
					"cluster", claim.Spec.ClusterRef)
				return nil
			}
		}
		return err
	}
	adminConn, err := postgres.Open(ctx, target.ConnOpts(postgres.AdminDatabase))
	if err != nil {
		return err
	}
	defer adminConn.Close(ctx)
	if err := postgres.TerminateBackends(ctx, adminConn, claim.Spec.DatabaseName); err != nil {
		return err
	}
	return postgres.DropDatabase(ctx, adminConn, claim.Spec.DatabaseName)
}

// SetupWithManager wires up the controller. Field indexes must be installed
// once per manager via SetupFieldIndexes before this is called.
func (r *DatabaseClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
