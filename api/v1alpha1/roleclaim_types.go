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

// DatabaseClaimRef references a DatabaseClaim in the same namespace as the
// RoleClaim. Cross-namespace refs are deliberately not allowed; otherwise a
// RoleClaim in any namespace could request roles on someone else's database.
type DatabaseClaimRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// AccessLevel is the permission profile applied to a role on a (set of)
// schema(s).
// +kubebuilder:validation:Enum=Owner;ReadWrite;ReadOnly
type AccessLevel string

const (
	// AccessOwner grants ALTER SCHEMA OWNER TO and (when used as sugar)
	// ALTER DATABASE OWNER TO.
	AccessOwner AccessLevel = "Owner"

	// AccessReadWrite grants USAGE on the schema plus full DML on tables and
	// sequences.
	AccessReadWrite AccessLevel = "ReadWrite"

	// AccessReadOnly grants USAGE on the schema plus SELECT on tables and
	// sequences. Default-privileges reflex extends this to future objects
	// created by sibling writer roles.
	AccessReadOnly AccessLevel = "ReadOnly"
)

// DefaultPrivilegeGrant is one (writer-role, schema) pair on which this
// RoleClaim's role has been granted default SELECT privileges via
// ALTER DEFAULT PRIVILEGES. Tracked in status so a narrowing reconcile can
// revoke the diff.
type DefaultPrivilegeGrant struct {
	// Writer is the Postgres role whose future objects this claim's role
	// receives SELECT on.
	Writer string `json:"writer"`
	// Schema is the schema scope of the default-privilege grant.
	Schema string `json:"schema"`
}

// SchemaGrant pairs a schema name with an access level.
type SchemaGrant struct {
	// Name of the schema. Must be declared in the referenced DatabaseClaim's
	// spec.schemas (otherwise reconcile fails with Reason=UnknownSchema).
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_]{0,62}$`
	Name string `json:"name"`

	// Access level applied to this schema.
	Access AccessLevel `json:"access"`
}

// RoleClaimSpec defines the desired state of RoleClaim.
//
// Exactly one of spec.access (sugar — applies to all schemas declared on the
// DatabaseClaim) or spec.schemas (per-schema scoping) MUST be set.
//
// +kubebuilder:validation:XValidation:rule="(has(self.access) ? 1 : 0) + (has(self.schemas) ? 1 : 0) == 1",message="exactly one of spec.access or spec.schemas must be set"
// +kubebuilder:validation:XValidation:rule="self.databaseClaimRef == oldSelf.databaseClaimRef",message="databaseClaimRef is immutable"
// +kubebuilder:validation:XValidation:rule="(has(oldSelf.access) ? has(self.access) && self.access == oldSelf.access : !has(self.access))",message="access is immutable once set and cannot be added after creation"
// +kubebuilder:validation:XValidation:rule="self.roleName == oldSelf.roleName",message="roleName is immutable"
type RoleClaimSpec struct {
	// DatabaseClaimRef points at the DatabaseClaim this role belongs to.
	// Same-namespace only. Immutable.
	DatabaseClaimRef DatabaseClaimRef `json:"databaseClaimRef"`

	// Access is the simple form: a single access level applied to every schema
	// declared on the referenced DatabaseClaim. Mutually exclusive with
	// spec.schemas. When set to Owner, the role is also granted ownership of
	// the database itself (ALTER DATABASE OWNER TO).
	// +optional
	Access *AccessLevel `json:"access,omitempty"`

	// Schemas is the per-schema form. A list of (schema, access) entries.
	// Mutually exclusive with spec.access. Use this for bounded-context
	// scenarios where multiple services share a database with different
	// permissions on different schemas. Access changes revoke and reapply
	// grants, but ownership transfers are not automatically reversed.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	Schemas []SchemaGrant `json:"schemas,omitempty"`

	// RoleName is the Postgres role name this claim manages. Required and
	// immutable: K8s resource names commonly contain '-', which is invalid
	// in Postgres identifiers, so a default derived from the K8s name is
	// unsafe. Set this explicitly to a pattern-safe identifier.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_]{0,62}$`
	RoleName string `json:"roleName"`

	// ConnectionLimit sets the Postgres role's CONNECTION LIMIT. If unset,
	// the role's connection count is unlimited (CONNECTION LIMIT -1).
	// +optional
	// +kubebuilder:validation:Minimum=-1
	ConnectionLimit *int32 `json:"connectionLimit,omitempty"`
}

// RoleClaimPhase is a coarse-grained lifecycle indicator.
// +kubebuilder:validation:Enum=Pending;Provisioning;Ready;Failed;Terminating
type RoleClaimPhase string

const (
	RoleClaimPhasePending      RoleClaimPhase = "Pending"
	RoleClaimPhaseProvisioning RoleClaimPhase = "Provisioning"
	RoleClaimPhaseReady        RoleClaimPhase = "Ready"
	RoleClaimPhaseFailed       RoleClaimPhase = "Failed"
	RoleClaimPhaseTerminating  RoleClaimPhase = "Terminating"
)

// RoleClaimStatus defines the observed state of RoleClaim.
type RoleClaimStatus struct {
	// Phase is a derived, human-readable summary of conditions.
	// +optional
	Phase RoleClaimPhase `json:"phase,omitempty"`

	// Conditions is the authoritative status surface. Standard types include
	// Ready, DatabaseClaimResolved, RoleReady, SecretReady.
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// CredentialsSecretName is the name of the Secret in the same namespace
	// that holds the role's credentials.
	// +optional
	CredentialsSecretName string `json:"credentialsSecretName,omitempty"`

	// RoleName is the resolved Postgres role name.
	// +optional
	RoleName string `json:"roleName,omitempty"`

	// ResolvedSchemas echoes the (schema, access) tuples that were applied,
	// regardless of whether spec.access (sugar) or spec.schemas was used.
	// +optional
	// +listType=map
	// +listMapKey=name
	ResolvedSchemas []SchemaGrant `json:"resolvedSchemas,omitempty"`

	// AppliedDefaultPrivileges records the (writer, schema) pairs for which
	// this RoleClaim's role currently holds default SELECT privileges. The
	// reconciler diffs against this list each pass: pairs no longer in the
	// intended universe are revoked via ALTER DEFAULT PRIVILEGES ... REVOKE.
	// +optional
	// +listType=map
	// +listMapKey=writer
	// +listMapKey=schema
	AppliedDefaultPrivileges []DefaultPrivilegeGrant `json:"appliedDefaultPrivileges,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=roleclaim;roleclaims,categories=cnpg
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.databaseClaimRef.name`
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.status.roleName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RoleClaim provisions a Postgres role with a permission profile against a
// DatabaseClaim in the same namespace, and emits a Secret with the role's
// credentials.
type RoleClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RoleClaimSpec   `json:"spec,omitempty"`
	Status RoleClaimStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RoleClaimList contains a list of RoleClaim.
type RoleClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RoleClaim `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RoleClaim{}, &RoleClaimList{})
}
