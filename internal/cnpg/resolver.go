/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package cnpg contains helpers for resolving CloudNativePG Cluster
// resources: looking them up, determining readiness, and extracting the
// connection parameters our operator needs (RW service FQDN, superuser
// credentials).
package cnpg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/postgres"
)

// serviceReadWritePort is the port the CNPG read-write Service exposes. CNPG
// always maps the Service to this port regardless of the in-pod postgres
// listen port (cf. cloudnative-pg/pkg/postgres.ServerPort).
const serviceReadWritePort = 5432

// ClaimAllowlistAnnotation, when set on a CNPG Cluster, names the namespaces
// whose DatabaseClaims (and, transitively, RoleClaims) may target the cluster.
// Value is a comma-separated list; whitespace around entries is trimmed.
// Same-namespace claims are always allowed regardless of the annotation; if
// the annotation is absent or empty, cross-namespace claims are denied.
const ClaimAllowlistAnnotation = "cnpg.wyvernzora.io/allowed-claim-namespaces"

// ClusterTarget is the resolved connection target for a CNPG cluster.
type ClusterTarget struct {
	Host               string // FQDN of the RW service
	Port               int
	SuperUser          string
	SuperPass          string
	SecretName         string
	ClusterName        string
	ClusterNamespace   string
	ClusterAnnotations map[string]string
	ClusterReady       bool
	ClusterPhase       string
}

// ErrClusterNotFound indicates the Cluster CR was not found.
var ErrClusterNotFound = errors.New("cnpg cluster not found")

// ErrClusterNotReady indicates the Cluster CR exists but isn't Ready.
var ErrClusterNotReady = errors.New("cnpg cluster not ready")

// ErrSuperUserSecretMissing indicates the superuser Secret could not be read.
var ErrSuperUserSecretMissing = errors.New("cnpg superuser secret missing")

// ErrClaimNotAllowed indicates the claim's namespace is not permitted to
// target the referenced Cluster by its allowlist annotation.
var ErrClaimNotAllowed = errors.New("claim namespace not allowed by cluster allowlist")

// Resolve looks up the Cluster, verifies readiness, and reads the superuser
// secret to populate connection options.
func Resolve(ctx context.Context, c client.Client, name, namespace string) (*ClusterTarget, error) {
	target, err := ResolveCluster(ctx, c, name, namespace)
	if err != nil {
		return nil, err
	}
	if err := CheckClusterReady(target); err != nil {
		return nil, err
	}
	if err := ResolveSuperuserCredentials(ctx, c, target); err != nil {
		return nil, err
	}
	return target, nil
}

// ResolveCluster looks up the Cluster, returning only non-secret connection
// metadata. Callers that need to enforce cluster policy should do it after this
// step and before CheckClusterReady or ResolveSuperuserCredentials.
func ResolveCluster(ctx context.Context, c client.Client, name, namespace string) (*ClusterTarget, error) {
	var cluster cnpgv1.Cluster
	err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cluster)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: %s/%s", ErrClusterNotFound, namespace, name)
	}
	if err != nil {
		return nil, fmt.Errorf("get cluster %s/%s: %w", namespace, name, err)
	}

	secretName := cluster.GetSuperuserSecretName()
	host := fmt.Sprintf("%s.%s.svc", cluster.GetServiceReadWriteName(), namespace)
	annotations := make(map[string]string, len(cluster.Annotations))
	for k, v := range cluster.Annotations {
		annotations[k] = v
	}
	return &ClusterTarget{
		Host:               host,
		Port:               serviceReadWritePort,
		SecretName:         secretName,
		ClusterName:        name,
		ClusterNamespace:   namespace,
		ClusterAnnotations: annotations,
		ClusterReady:       IsClusterReady(&cluster),
		ClusterPhase:       string(cluster.Status.Phase),
	}, nil
}

// CheckClusterReady reports ErrClusterNotReady if the resolved Cluster is not
// ready for SQL connections.
func CheckClusterReady(target *ClusterTarget) error {
	if target.ClusterReady {
		return nil
	}
	return fmt.Errorf("%w: %s/%s phase=%q", ErrClusterNotReady, target.ClusterNamespace, target.ClusterName, target.ClusterPhase)
}

// ResolveSuperuserCredentials reads the resolved Cluster's superuser Secret and
// fills the credential fields on target.
func ResolveSuperuserCredentials(ctx context.Context, c client.Client, target *ClusterTarget) error {
	if target.SecretName == "" {
		return fmt.Errorf("%w: cluster %s/%s has no superuser secret configured", ErrSuperUserSecretMissing, target.ClusterNamespace, target.ClusterName)
	}

	var secret corev1.Secret
	err := c.Get(ctx, types.NamespacedName{Name: target.SecretName, Namespace: target.ClusterNamespace}, &secret)
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: secret %s/%s", ErrSuperUserSecretMissing, target.ClusterNamespace, target.SecretName)
	}
	if err != nil {
		return fmt.Errorf("get superuser secret %s/%s: %w", target.ClusterNamespace, target.SecretName, err)
	}

	user, pass, err := credentialsFromSecret(&secret)
	if err != nil {
		return err
	}
	target.SuperUser = user
	target.SuperPass = pass
	return nil
}

// CheckClaimAllowed reports whether a claim in claimNamespace may target the
// resolved cluster. Same-namespace claims are always allowed; otherwise the
// namespace must appear in the cluster's ClaimAllowlistAnnotation value
// (comma-separated, whitespace-trimmed). Missing or empty annotation denies
// all cross-namespace claims.
func CheckClaimAllowed(target *ClusterTarget, claimNamespace string) error {
	if target.ClusterNamespace == claimNamespace {
		return nil
	}
	raw := target.ClusterAnnotations[ClaimAllowlistAnnotation]
	for _, ns := range strings.Split(raw, ",") {
		if strings.TrimSpace(ns) == claimNamespace {
			return nil
		}
	}
	return fmt.Errorf("%w: namespace %q not listed on cluster %s", ErrClaimNotAllowed, claimNamespace, target.ClusterNamespace)
}

// IsClusterReady returns true if the cluster has the Ready condition true OR
// is in the steady-state Healthy phase. CNPG conditions can lag the phase
// during normal operation, so we accept either signal.
func IsClusterReady(cluster *cnpgv1.Cluster) bool {
	if cluster.Status.Phase == cnpgv1.PhaseHealthy {
		return true
	}
	cond := meta.FindStatusCondition(cluster.Status.Conditions, string(cnpgv1.ConditionClusterReady))
	return cond != nil && cond.Status == metav1.ConditionTrue
}

// ConnOpts returns the postgres.ConnOpts for an admin connection against the
// given database name.
func (t *ClusterTarget) ConnOpts(database string) postgres.ConnOpts {
	return postgres.ConnOpts{
		Host:     t.Host,
		Port:     t.Port,
		Database: database,
		User:     t.SuperUser,
		Password: t.SuperPass,
	}
}

// credentialsFromSecret extracts username/password from a CNPG superuser
// secret. CNPG uses keys "username" and "password" in a kubernetes.io/basic-auth
// secret.
func credentialsFromSecret(s *corev1.Secret) (string, string, error) {
	user, ok := s.Data["username"]
	if !ok || len(user) == 0 {
		return "", "", fmt.Errorf("%w: secret %s/%s has no username", ErrSuperUserSecretMissing, s.Namespace, s.Name)
	}
	pass, ok := s.Data["password"]
	if !ok || len(pass) == 0 {
		return "", "", fmt.Errorf("%w: secret %s/%s has no password", ErrSuperUserSecretMissing, s.Namespace, s.Name)
	}
	return string(user), string(pass), nil
}
