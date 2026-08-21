/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterReference identifies a CloudNativePG Cluster.
type ClusterReference struct {
	// Name of the CNPG Cluster resource.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the CNPG Cluster resource. May differ from the namespace of
	// the DatabaseClaim itself.
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
}

// ExtensionSpec names a Postgres extension to install and, optionally, the
// schema its objects are created in.
type ExtensionSpec struct {
	// Name of the Postgres extension (e.g., "pgcrypto").
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_]{0,62}$`
	Name string `json:"name"`

	// Schema to install the extension's objects into. Must be one of
	// spec.schemas — this is enforced at admission, and spec.schemas is
	// ensured before extensions are installed, so a schema-targeted extension
	// can never name a schema that does not exist. Consumers that pin their
	// runtime search_path to that schema can then resolve the extension's
	// operator classes and functions.
	//
	// Leave unset for the "global by convention" case: Postgres has no truly
	// global extension, so an unset schema simply means CREATE EXTENSION
	// without a SCHEMA clause, landing the objects wherever the provisioning
	// superuser's default search_path puts them (in practice, "public").
	// There is no separate spelling for global.
	//
	// A set schema is a declared desired state, so an extension that already
	// exists in a different schema is relocated with ALTER EXTENSION ... SET
	// SCHEMA, matching CloudNativePG's own Database CRD. Extensions that
	// declare themselves non-relocatable cannot be moved; that failure
	// surfaces as Reason=ExtensionRelocationFailed for an operator to resolve.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_]{0,62}$`
	Schema string `json:"schema,omitempty"`
}

// DeletionPolicy controls what happens to the underlying Postgres database when
// the DatabaseClaim is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type DeletionPolicy string

const (
	// DeletionPolicyRetain keeps the database. Deletion is refused while any
	// RoleClaim still references the DatabaseClaim.
	DeletionPolicyRetain DeletionPolicy = "Retain"

	// DeletionPolicyDelete drops the database. Referencing RoleClaims are
	// cascade-deleted first.
	DeletionPolicyDelete DeletionPolicy = "Delete"
)

// DatabaseClaimSpec defines the desired state of DatabaseClaim.
// +kubebuilder:validation:XValidation:rule="self.databaseName == oldSelf.databaseName",message="databaseName is immutable"
// +kubebuilder:validation:XValidation:rule="self.clusterRef == oldSelf.clusterRef",message="clusterRef is immutable"
// +kubebuilder:validation:XValidation:rule="!has(self.extensions) || self.extensions.all(e, !has(e.schema) || (has(self.schemas) && e.schema in self.schemas))",message="each extension schema must be declared in spec.schemas"
type DatabaseClaimSpec struct {
	// DatabaseName is the Postgres database name to provision. Immutable.
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_]{0,62}$`
	// +kubebuilder:validation:XValidation:rule="self != 'postgres' && self != 'template0' && self != 'template1'",message="reserved database name"
	DatabaseName string `json:"databaseName"`

	// ClusterRef points at the CNPG Cluster that will host this database.
	// Immutable.
	ClusterRef ClusterReference `json:"clusterRef"`

	// Extensions to install in the database. Installed at the default version
	// via CREATE EXTENSION IF NOT EXISTS. Version updates are out of scope
	// in v1. Both this list and spec.schemas are capped at 32 entries so the
	// admission rule cross-checking them stays inside the API server's CEL
	// cost budget.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=32
	Extensions []ExtensionSpec `json:"extensions,omitempty"`

	// Schemas to ensure in the database. Each entry is created with
	// CREATE SCHEMA IF NOT EXISTS, initially owned by the superuser.
	// Ownership transfers when a matching Owner RoleClaim reconciles.
	// Removing an entry from this list is a no-op in v1; schemas must be
	// dropped manually.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:validation:items:Pattern=`^[a-z_][a-z0-9_]{0,62}$`
	Schemas []string `json:"schemas,omitempty"`

	// DeletionPolicy controls the behaviour of DatabaseClaim deletion. Retain
	// (default) blocks deletion while RoleClaims reference this claim; Delete
	// cascades through referencing RoleClaims and then drops the database.
	// +kubebuilder:default=Retain
	// +optional
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
}

// DatabaseInfo captures the connection coordinates of a provisioned database.
// Not a Secret — contains no credentials.
type DatabaseInfo struct {
	Host   string `json:"host,omitempty"`
	Port   int32  `json:"port,omitempty"`
	DBName string `json:"dbname,omitempty"`
}

// DatabaseClaimPhase is a coarse-grained lifecycle indicator derived from
// status.conditions. The authoritative state lives in conditions.
// +kubebuilder:validation:Enum=Pending;Provisioning;Ready;Failed;Terminating
type DatabaseClaimPhase string

const (
	DatabaseClaimPhasePending      DatabaseClaimPhase = "Pending"
	DatabaseClaimPhaseProvisioning DatabaseClaimPhase = "Provisioning"
	DatabaseClaimPhaseReady        DatabaseClaimPhase = "Ready"
	DatabaseClaimPhaseFailed       DatabaseClaimPhase = "Failed"
	DatabaseClaimPhaseTerminating  DatabaseClaimPhase = "Terminating"
)

// DatabaseClaimStatus defines the observed state of DatabaseClaim.
type DatabaseClaimStatus struct {
	// Phase is a derived, human-readable summary of conditions.
	// +optional
	Phase DatabaseClaimPhase `json:"phase,omitempty"`

	// Conditions is the authoritative status surface. Standard types include
	// Ready, ClusterResolved, and DatabaseReady.
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// DatabaseInfo carries connection coordinates once the database is Ready.
	// +optional
	DatabaseInfo *DatabaseInfo `json:"databaseInfo,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=dbclaim;dbclaims,categories=cnpg
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.databaseName`
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DatabaseClaim provisions a Postgres database in a CNPG cluster on behalf of
// an application namespace.
type DatabaseClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatabaseClaimSpec   `json:"spec,omitempty"`
	Status DatabaseClaimStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatabaseClaimList contains a list of DatabaseClaim.
type DatabaseClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabaseClaim `json:"items"`
}
