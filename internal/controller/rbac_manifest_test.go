/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// helmAction matches a Helm template action, so a chart template can be read
// as the YAML it is otherwise. Every action in the RBAC template stands in for
// a scalar or a whole line, so dropping them leaves the document parseable.
var helmAction = regexp.MustCompile(`(?s){{.*?}}`)

// TestShippedClusterRolesGrantBothEventAPIs pins the RBAC the operator actually
// needs to emit events. The claim controllers record through
// mgr.GetEventRecorder, whose recorder posts to events.k8s.io/v1, while
// controller-runtime's leader election still records through the core v1 API —
// so a ClusterRole that grants only one of the two silently drops half the
// events, Forbidden, with nothing on the claim to show for it.
//
// Both shipped ClusterRoles are checked: config/rbac/role.yaml is generated
// from the kubebuilder markers, and the chart's is maintained by hand, which is
// the one that drifts.
func TestShippedClusterRolesGrantBothEventAPIs(t *testing.T) {
	for _, source := range []struct{ name, path string }{
		{"generated", "../../config/rbac/role.yaml"},
		{"helm chart", "../../charts/dbclaim-operator/templates/rbac.yaml"},
	} {
		t.Run(source.name, func(t *testing.T) {
			rules := clusterRoleRules(t, source.path)
			for _, group := range []string{"", "events.k8s.io"} {
				if !grants(rules, group, "events", "create") {
					t.Errorf("%s ClusterRole does not grant create on events in API group %q", source.path, group)
				}
			}
		})
	}
}

// TestKustomizeGrantsTheLeaseItElectsOn pins the leader election RBAC to the
// flag that needs it. config/manager/manager.yaml passes --leader-elect, and
// controller-runtime starts no controller until the lease is acquired; a
// Forbidden Get on that lease is retried forever rather than fatal, while the
// healthz.Ping probes stay green. A kustomize deploy missing this grant is a
// pod that looks healthy and reconciles nothing.
//
// The chart is not checked here: it gates the flag and the Role on the same
// .Values.leaderElection, so they cannot drift apart.
func TestKustomizeGrantsTheLeaseItElectsOn(t *testing.T) {
	manager, err := os.ReadFile("../../config/manager/manager.yaml")
	if err != nil {
		t.Fatalf("read config/manager/manager.yaml: %v", err)
	}
	if !strings.Contains(string(manager), "--leader-elect") {
		t.Skip("config/manager/manager.yaml no longer passes --leader-elect, so no lease is needed")
	}

	rules := kustomizedRBACRules(t, "../../config/rbac")
	for _, verb := range []string{"get", "create", "update"} {
		if !grants(rules, "coordination.k8s.io", "leases", verb) {
			t.Errorf("config/rbac grants no %q on coordination.k8s.io leases, but the manager runs with --leader-elect", verb)
		}
	}
}

// kustomizedRBACRules returns every rule of every Role and ClusterRole a
// kustomization actually pulls in. It walks the resources list rather than the
// directory, because a manifest kustomize never builds grants nothing.
func kustomizedRBACRules(t *testing.T, dir string) []rbacv1.PolicyRule {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "kustomization.yaml"))
	if err != nil {
		t.Fatalf("read %s/kustomization.yaml: %v", dir, err)
	}
	var kustomization struct {
		Resources []string `json:"resources"`
	}
	if err := yaml.Unmarshal(raw, &kustomization); err != nil {
		t.Fatalf("parse %s/kustomization.yaml: %v", dir, err)
	}

	var rules []rbacv1.PolicyRule
	for _, resource := range kustomization.Resources {
		manifest, err := os.ReadFile(filepath.Join(dir, resource))
		if err != nil {
			t.Fatalf("read %s listed by %s/kustomization.yaml: %v", resource, dir, err)
		}
		// Role and ClusterRole carry the same Rules field, so one type reads both.
		for _, doc := range strings.Split(string(manifest), "\n---") {
			var role rbacv1.Role
			if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
				t.Fatalf("parse %s: %v", resource, err)
			}
			if role.Kind != "Role" && role.Kind != "ClusterRole" {
				continue
			}
			rules = append(rules, role.Rules...)
		}
	}
	return rules
}

// clusterRoleRules returns every rule of every ClusterRole in a manifest,
// which may be a Helm template.
func clusterRoleRules(t *testing.T, path string) []rbacv1.PolicyRule {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var rules []rbacv1.PolicyRule
	found := false
	for _, doc := range strings.Split(helmAction.ReplaceAllString(string(raw), ""), "\n---") {
		var role rbacv1.ClusterRole
		if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if role.Kind != "ClusterRole" {
			continue
		}
		found = true
		rules = append(rules, role.Rules...)
	}
	if !found {
		t.Fatalf("no ClusterRole document in %s", path)
	}
	return rules
}

func grants(rules []rbacv1.PolicyRule, group, resource, verb string) bool {
	for _, rule := range rules {
		if slices.Contains(rule.APIGroups, group) &&
			slices.Contains(rule.Resources, resource) &&
			slices.Contains(rule.Verbs, verb) {
			return true
		}
	}
	return false
}
