/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package postgres

import (
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
)

var identifierRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// ValidateIdentifier checks that name matches the safe identifier pattern
// (lowercase letter or underscore followed by up to 62 of letter/digit/_).
// Returns an error if not. Callers should rely on CRD validation as the
// primary guard; this is defense in depth.
func ValidateIdentifier(name string) error {
	if !identifierRe.MatchString(name) {
		return fmt.Errorf("invalid postgres identifier %q (must match %s)", name, identifierRe.String())
	}
	return nil
}

// Quote returns name wrapped in double quotes with any embedded quotes
// escaped. Safe to splice into SQL even if name fails ValidateIdentifier,
// though we still validate first to fail loud rather than producing weird SQL.
func Quote(name string) string {
	return pgx.Identifier{name}.Sanitize()
}
