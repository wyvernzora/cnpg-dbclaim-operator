/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

func TestShouldEmitConditionEvent(t *testing.T) {
	conds := []metav1.Condition{{
		Type:               ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonClusterMissing,
		Message:            "old message",
		ObservedGeneration: 7,
	}}

	tests := []struct {
		name       string
		conds      []metav1.Condition
		generation int64
		status     metav1.ConditionStatus
		reason     string
		want       bool
	}{
		{
			name:       "absent condition emits",
			conds:      nil,
			generation: 7,
			status:     metav1.ConditionFalse,
			reason:     ReasonClusterMissing,
			want:       true,
		},
		{
			name:       "same status reason and generation suppresses",
			conds:      conds,
			generation: 7,
			status:     metav1.ConditionFalse,
			reason:     ReasonClusterMissing,
			want:       false,
		},
		{
			name:       "generation change emits",
			conds:      conds,
			generation: 8,
			status:     metav1.ConditionFalse,
			reason:     ReasonClusterMissing,
			want:       true,
		},
		{
			name:       "status change emits",
			conds:      conds,
			generation: 7,
			status:     metav1.ConditionTrue,
			reason:     ReasonClusterMissing,
			want:       true,
		},
		{
			name:       "reason change emits",
			conds:      conds,
			generation: 7,
			status:     metav1.ConditionFalse,
			reason:     ReasonClusterNotReady,
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldEmitConditionEvent(tt.conds, tt.generation, ConditionReady, tt.status, tt.reason)
			if got != tt.want {
				t.Fatalf("shouldEmitConditionEvent() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestShouldEmitConditionEventIgnoresMessageOnlyChanges(t *testing.T) {
	conds := []metav1.Condition{{
		Type:               ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonClusterMissing,
		Message:            "old message",
		ObservedGeneration: 7,
	}}

	if shouldEmitConditionEvent(conds, 7, ConditionReady, metav1.ConditionFalse, ReasonClusterMissing) {
		t.Fatal("message-only changes should not cause event emission")
	}
}

func TestShouldEmitDeleteFailureEvent(t *testing.T) {
	conds := []metav1.Condition{{
		Type:               ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonReconcileFailed,
		Message:            "old failure",
		ObservedGeneration: 7,
	}}

	tests := []struct {
		name           string
		wasTerminating bool
		reason         string
		want           bool
	}{
		{
			name:           "non terminating claim emits even when Ready already has ReconcileFailed",
			wasTerminating: false,
			reason:         ReasonReconcileFailed,
			want:           true,
		},
		{
			name:           "terminating retry suppresses unchanged ReconcileFailed",
			wasTerminating: true,
			reason:         ReasonReconcileFailed,
			want:           false,
		},
		{
			name:           "terminating claim emits when reason changes to ReconcileFailed",
			wasTerminating: true,
			reason:         ReasonBlockedByRoleClaims,
			want:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testConds := conds
			testConds[0].Reason = tt.reason
			got := shouldEmitDeleteFailureEvent(testConds, 7, tt.wasTerminating)
			if got != tt.want {
				t.Fatalf("shouldEmitDeleteFailureEvent() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestEmitEvent(t *testing.T) {
	claim := &cnpgclaimv1alpha1.DatabaseClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	recorder := record.NewFakeRecorder(1)

	emitEvent(recorder, claim, corev1.EventTypeWarning, ReasonClusterMissing, "cluster missing")

	got := <-recorder.Events
	if !strings.Contains(got, corev1.EventTypeWarning) ||
		!strings.Contains(got, ReasonClusterMissing) ||
		!strings.Contains(got, "cluster missing") {
		t.Fatalf("event = %q, want warning event with reason and message", got)
	}
}

func TestEmitEventNilRecorderIsNoop(t *testing.T) {
	claim := &cnpgclaimv1alpha1.DatabaseClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}

	emitEvent(nil, claim, corev1.EventTypeNormal, ReasonProvisioned, "ready")
}
