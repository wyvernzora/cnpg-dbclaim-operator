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

// ClusterTarget is the resolved connection target for a CNPG cluster.
type ClusterTarget struct {
	Host       string // FQDN of the RW service
	Port       int
	SuperUser  string
	SuperPass  string
	SecretName string
}

// ErrClusterNotFound indicates the Cluster CR was not found.
var ErrClusterNotFound = errors.New("cnpg cluster not found")

// ErrClusterNotReady indicates the Cluster CR exists but isn't Ready.
var ErrClusterNotReady = errors.New("cnpg cluster not ready")

// ErrSuperUserSecretMissing indicates the superuser Secret could not be read.
var ErrSuperUserSecretMissing = errors.New("cnpg superuser secret missing")

// Resolve looks up the Cluster, verifies readiness, and reads the superuser
// secret to populate connection options.
func Resolve(ctx context.Context, c client.Client, name, namespace string) (*ClusterTarget, error) {
	var cluster cnpgv1.Cluster
	err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cluster)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: %s/%s", ErrClusterNotFound, namespace, name)
	}
	if err != nil {
		return nil, fmt.Errorf("get cluster %s/%s: %w", namespace, name, err)
	}

	if !IsClusterReady(&cluster) {
		return nil, fmt.Errorf("%w: %s/%s phase=%q", ErrClusterNotReady, namespace, name, cluster.Status.Phase)
	}

	secretName := cluster.GetSuperuserSecretName()
	if secretName == "" {
		return nil, fmt.Errorf("%w: cluster %s/%s has no superuser secret configured", ErrSuperUserSecretMissing, namespace, name)
	}

	var secret corev1.Secret
	err = c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, &secret)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: secret %s/%s", ErrSuperUserSecretMissing, namespace, secretName)
	}
	if err != nil {
		return nil, fmt.Errorf("get superuser secret %s/%s: %w", namespace, secretName, err)
	}

	user, pass, err := credentialsFromSecret(&secret)
	if err != nil {
		return nil, err
	}

	host := fmt.Sprintf("%s.%s.svc", cluster.GetServiceReadWriteName(), namespace)
	return &ClusterTarget{
		Host:       host,
		Port:       serviceReadWritePort,
		SuperUser:  user,
		SuperPass:  pass,
		SecretName: secretName,
	}, nil
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
