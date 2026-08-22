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
	"strconv"
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
//
// Every lock wait on the session is bounded by lockWaitTimeout. Most of what a
// reconcile runs is a single autocommit statement — CREATE SCHEMA, ALTER ROLE,
// the RoleClaim GRANTs and ALTER DEFAULT PRIVILEGES — and each of them waits on
// a catalog row a session this operator does not control can hold: a tenant
// that owns its database, or a managed role acting on itself, leaves a
// transaction open and holds it. Each reconciler runs a single worker with no
// deadline, so an unbounded wait parks every claim of that kind with no event,
// no status and no error. With the bound the statement aborts with SQLSTATE
// 55P03, the claim gets a Warning event, and the requeue retries once the other
// session is gone. Transactions that need the same bound set it themselves
// (boundLockWait), which also covers connections opened elsewhere.
//
// It is sent as a startup parameter, so it outranks any lock_timeout an
// ALTER DATABASE/ROLE SET has left behind — a database owner can write one.
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
	q.Set("lock_timeout", strconv.FormatInt(lockWaitTimeout.Milliseconds(), 10))
	u.RawQuery = q.Encode()

	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(connectCtx, u.String())
	if err != nil {
		return nil, fmt.Errorf("connect postgres %s: %w", opts.Host, err)
	}
	return conn, nil
}
