/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

// errStatusWriteRejected stands in for what a real API server returns on every
// status write to an unmigrated legacy claim: the spec-level CEL rule is
// evaluated against the stored bare strings and fails
// (TestFinalizerReleaseOnLegacyStoredClaim measures it).
var errStatusWriteRejected = errors.New("status write rejected")

// TestReconcileEmitsEventWhenStatusWriteIsRejected covers every DatabaseClaim
// path that reports a decision by writing status and then emitting an event.
// On a claim whose status writes are all rejected, the event is the only signal
// that can land, so it must be emitted whether or not the write succeeded —
// otherwise the claim goes quiet exactly when an operator needs to be told why
// it is stuck.
func TestReconcileEmitsEventWhenStatusWriteIsRejected(t *testing.T) {
	ctx := context.Background()
	deleting := metav1.NewTime(time.Now())

	newClaim := func(name string, mutate func(*cnpgclaimv1alpha1.DatabaseClaim)) *cnpgclaimv1alpha1.DatabaseClaim {
		claim := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         "default",
				Finalizers:        []string{DatabaseClaimFinalizer},
				CreationTimestamp: metav1.NewTime(time.Now()),
			},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "orders",
				ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: "pg", Namespace: "default"},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		if mutate != nil {
			mutate(claim)
		}
		return claim
	}
	referrer := &cnpgclaimv1alpha1.RoleClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-rw", Namespace: "default"},
		Spec: cnpgclaimv1alpha1.RoleClaimSpec{
			DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: "claim"},
			RoleName:         "orders_rw",
		},
	}

	cases := []struct {
		name    string
		objects []client.Object
		want    string
	}{
		{
			name: "another claim already owns the database",
			objects: []client.Object{
				newClaim("older", func(c *cnpgclaimv1alpha1.DatabaseClaim) {
					c.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
				}),
				newClaim("claim", nil),
			},
			want: "Warning " + ReasonDatabaseNameConflict,
		},
		{
			name:    "the referenced cluster does not exist",
			objects: []client.Object{newClaim("claim", nil)},
			want:    "Warning " + ReasonClusterMissing,
		},
		{
			name: "deletion is blocked by a RoleClaim",
			objects: []client.Object{
				newClaim("claim", func(c *cnpgclaimv1alpha1.DatabaseClaim) {
					c.DeletionTimestamp = &deleting
				}),
				referrer,
			},
			want: "Warning " + ReasonBlockedByRoleClaims,
		},
		{
			// The de-duplication trap: the claim was already reporting this
			// exact condition before the upgrade, and the write that would
			// refresh it is the write being rejected. Suppressing the event
			// here leaves the claim with no signal at all, forever.
			name: "deletion is blocked and the stored condition already says so",
			objects: []client.Object{
				newClaim("claim", func(c *cnpgclaimv1alpha1.DatabaseClaim) {
					c.DeletionTimestamp = &deleting
					c.Generation = 1
					setCondition(&c.Status.Conditions, c.Generation, ConditionReady, metav1.ConditionFalse,
						ReasonBlockedByRoleClaims, "RoleClaims still reference this DatabaseClaim: [orders-rw]")
				}),
				referrer,
			},
			want: "Warning " + ReasonBlockedByRoleClaims,
		},
		{
			name: "deletion cascades through a RoleClaim",
			objects: []client.Object{
				newClaim("claim", func(c *cnpgclaimv1alpha1.DatabaseClaim) {
					c.DeletionTimestamp = &deleting
					c.Spec.DeletionPolicy = cnpgclaimv1alpha1.DeletionPolicyDelete
				}),
				referrer,
			},
			want: "Normal " + ReasonReconciling,
		},
		{
			name: "teardown cannot reach the cluster",
			objects: []client.Object{
				newClaim("claim", func(c *cnpgclaimv1alpha1.DatabaseClaim) {
					c.DeletionTimestamp = &deleting
					c.Spec.DeletionPolicy = cnpgclaimv1alpha1.DeletionPolicyDelete
				}),
			},
			want: "Warning " + ReasonReconcileFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, recorder := reconcilerWithFailingStatusWrites(t, errStatusWriteRejected, tc.objects...)

			_, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: client.ObjectKey{Namespace: "default", Name: "claim"},
			})
			if err == nil {
				t.Fatal("Reconcile returned no error, so the rejected status write never happened")
			}

			select {
			case got := <-recorder.Events:
				if !strings.HasPrefix(got, tc.want) {
					t.Fatalf("event %q, want one starting %q", got, tc.want)
				}
			default:
				t.Fatalf("no event emitted after the status write was rejected (%v); want one starting %q", err, tc.want)
			}
		})
	}

	// The other direction. A conflict is retried against a fresh read, and that
	// reconcile records the condition and emits the event, so emitting here too
	// would report the same decision twice — which is what the event-count
	// assertions in the envtest suite catch.
	t.Run("a conflicting status write is left to the retry", func(t *testing.T) {
		conflict := apierrors.NewConflict(
			schema.GroupResource{Group: "cnpg.wyvernzora.io", Resource: "databaseclaims"},
			"claim", errors.New("the object has been modified"))
		r, recorder := reconcilerWithFailingStatusWrites(t, conflict, newClaim("claim", nil))

		if _, err := r.Reconcile(ctx, ctrl.Request{
			NamespacedName: client.ObjectKey{Namespace: "default", Name: "claim"},
		}); !apierrors.IsConflict(err) {
			t.Fatalf("Reconcile returned %v, want the conflict", err)
		}
		select {
		case got := <-recorder.Events:
			t.Fatalf("event %q emitted for a status write that will be retried", got)
		default:
		}
	})
}

// TestDeleteEmitsEventWhenFinalizerReleaseFails covers the last write of a
// deletion. Once the teardown decision is made the claim is one finalizer write
// away from gone; if that write keeps failing — here the way the finding
// describes, an upgrade that never re-applied the ClusterRole, so the merge
// patch updateFinalizers falls back to is forbidden — the claim sits
// Terminating with nothing on it to say why.
//
// The Invalid error is built the way the API server builds it on an unmigrated
// legacy claim: one cause per stored extension. That is what makes the note
// long enough to be rejected outright, so an event that is emitted but does not
// fit is no better than one that was never emitted.
func TestDeleteEmitsEventWhenFinalizerReleaseFails(t *testing.T) {
	deleting := metav1.NewTime(time.Now())
	claim := &cnpgclaimv1alpha1.DatabaseClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "claim",
			Namespace:         "default",
			Finalizers:        []string{DatabaseClaimFinalizer},
			DeletionTimestamp: &deleting,
		},
		Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
			DatabaseName:   "orders",
			ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: "pg", Namespace: "default"},
			DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
		},
	}
	gr := schema.GroupResource{Group: "cnpg.wyvernzora.io", Resource: "databaseclaims"}
	causes := make(field.ErrorList, 0, 40)
	for i := range 40 {
		causes = append(causes, field.Invalid(
			field.NewPath("spec", "extensions").Index(i), "pg_trgm",
			"spec.extensions[*].schema in body is required"))
	}
	r, recorder := newFakeReconciler(t, interceptor.Funcs{
		// What an unmigrated legacy claim does to the whole-object write...
		Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
			return apierrors.NewInvalid(schema.GroupKind{Group: gr.Group, Kind: "DatabaseClaim"}, "claim", causes)
		},
		// ...and what a missing `patch` verb does to the fallback.
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return apierrors.NewForbidden(gr, "claim", errors.New("cannot patch resource"))
		},
	}, claim)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: "default", Name: "claim"},
	})
	if err == nil {
		t.Fatal("Reconcile returned no error, so the finalizer release never failed")
	}
	// Without this the note-length assertion below would pass on a message that
	// was never over the limit, and prove nothing.
	if len(err.Error()) <= eventNoteLimit {
		t.Fatalf("the failure is only %d bytes, so this fixture never reaches the %d-byte note limit",
			len(err.Error()), eventNoteLimit)
	}
	prefix := "Warning " + ReasonReconcileFailed + " "
	select {
	case got := <-recorder.Events:
		if !strings.HasPrefix(got, prefix) {
			t.Fatalf("event %q, want one starting %q", got, prefix)
		}
		if note := strings.TrimPrefix(got, prefix); len(note) > eventNoteLimit {
			t.Fatalf("event note is %d bytes, over the %d the API server accepts: it is rejected as Invalid "+
				"and dropped without retry, so the stuck deletion is left with no signal at all",
				len(note), eventNoteLimit)
		}
	default:
		t.Fatalf("no event emitted after the finalizer release failed (%v)", err)
	}
}

// reconcilerWithFailingStatusWrites builds a DatabaseClaim reconciler whose
// every status write fails with statusErr, the way they all fail on an
// unmigrated legacy claim.
func reconcilerWithFailingStatusWrites(t *testing.T, statusErr error, objs ...client.Object) (*DatabaseClaimReconciler, *events.FakeRecorder) {
	t.Helper()
	return newFakeReconciler(t, interceptor.Funcs{
		SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			return statusErr
		},
	}, objs...)
}

// newFakeReconciler builds a DatabaseClaim reconciler over a fake client with
// the given writes intercepted, plus the FakeRecorder its events land in.
func newFakeReconciler(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) (*DatabaseClaimReconciler, *events.FakeRecorder) {
	t.Helper()

	s := k8sruntime.NewScheme()
	if err := cnpgclaimv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add claim scheme: %v", err)
	}
	if err := cnpgv1.AddToScheme(s); err != nil {
		t.Fatalf("add cnpg scheme: %v", err)
	}

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&cnpgclaimv1alpha1.DatabaseClaim{}, &cnpgclaimv1alpha1.RoleClaim{}).
		WithIndex(&cnpgclaimv1alpha1.RoleClaim{}, indexRoleClaimByDBClaimRef, func(obj client.Object) []string {
			rc, ok := obj.(*cnpgclaimv1alpha1.RoleClaim)
			if !ok {
				return nil
			}
			return []string{rc.Spec.DatabaseClaimRef.Name}
		}).
		WithInterceptorFuncs(funcs).
		Build()

	recorder := events.NewFakeRecorder(10)
	return &DatabaseClaimReconciler{Client: c, APIReader: c, Recorder: recorder, Scheme: s}, recorder
}
