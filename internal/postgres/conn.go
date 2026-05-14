/*
Copyright 2026 contributors to cnpg-dbclaim-operator.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package postgres

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
)

// AdminDatabase is the bootstrap database to connect to when issuing
// cluster-wide commands (CREATE DATABASE, DROP DATABASE).
const AdminDatabase = "postgres"

// ConnOpts is the connection target.
type ConnOpts struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	SSLMode  string // defaults to "require"
}

// Open returns a pgx connection pointed at the given target. The caller must
// Close it when done (typical: defer conn.Close(ctx)).
//
// Connections are short-lived: open per reconcile, close at the end.
func Open(ctx context.Context, opts ConnOpts) (*pgx.Conn, error) {
	if opts.Port == 0 {
		opts.Port = 5432
	}
	if opts.SSLMode == "" {
		opts.SSLMode = "require"
	}
	if opts.Database == "" {
		opts.Database = AdminDatabase
	}

	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(opts.User, opts.Password),
		Host:   fmt.Sprintf("%s:%d", opts.Host, opts.Port),
		Path:   "/" + opts.Database,
	}
	q := u.Query()
	q.Set("sslmode", opts.SSLMode)
	u.RawQuery = q.Encode()

	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(connectCtx, u.String())
	if err != nil {
		return nil, fmt.Errorf("connect postgres %s: %w", opts.Host, err)
	}
	return conn, nil
}
