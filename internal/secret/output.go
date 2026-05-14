/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package secret builds and reconciles RoleClaim credential Secrets.
package secret

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
)

// Credentials describes what goes into the output Secret. The keys are stable
// and documented; consumers may rely on them.
type Credentials struct {
	Host     string
	Port     int
	DBName   string
	User     string
	Password string
}

// Data builds the Secret data map (keys -> bytes). The set of keys is:
//   - host
//   - port
//   - dbname
//   - user
//   - password
//   - uri      (PostgreSQL URI, suitable for libpq-compatible clients)
//   - jdbc_uri (jdbc:postgresql:// form without embedded creds)
func (c Credentials) Data() map[string][]byte {
	return map[string][]byte{
		"host":     []byte(c.Host),
		"port":     []byte(strconv.Itoa(c.Port)),
		"dbname":   []byte(c.DBName),
		"user":     []byte(c.User),
		"password": []byte(c.Password),
		"uri":      []byte(c.URI()),
		"jdbc_uri": []byte(c.JDBCURI()),
	}
}

// URI returns a libpq-compatible URI including credentials and sslmode=require.
func (c Credentials) URI() string {
	u := url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   "/" + c.DBName,
	}
	q := u.Query()
	q.Set("sslmode", "require")
	u.RawQuery = q.Encode()
	return u.String()
}

// JDBCURI returns the JDBC form. Per JDBC convention, credentials are
// supplied as query parameters rather than userinfo.
func (c Credentials) JDBCURI() string {
	q := url.Values{}
	q.Set("user", c.User)
	q.Set("password", c.Password)
	q.Set("sslmode", "require")
	return fmt.Sprintf("jdbc:postgresql://%s:%d/%s?%s", c.Host, c.Port, c.DBName, q.Encode())
}

// GeneratePassword returns a 32-byte URL-safe random password.
func GeneratePassword() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
