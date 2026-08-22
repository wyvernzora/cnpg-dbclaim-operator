/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestOpenBoundsLockWaits covers the statements a reconcile runs outside any
// transaction of this package's own: EnsureSchema on the DatabaseClaim path,
// EnsureRole and GrantConnect on the RoleClaim path. Each waits on a catalog
// row another session can hold — an uncommitted CREATE SCHEMA of the same name,
// an open ALTER ROLE on the same role, an open GRANT on the same database —
// and each reconciler runs a single worker with no deadline, so an unbounded
// wait here parks every claim of that kind with no event and no status.
//
// The connection is opened through Open, the way every reconcile opens one,
// because that is where the bound is set: a connection made any other way
// proves nothing about what production waits on.
func TestOpenBoundsLockWaits(t *testing.T) {
	dsn := os.Getenv("CNPG_DBCLAIM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set CNPG_DBCLAIM_POSTGRES_DSN to run lock-wait integration tests against a real Postgres")
	}
	// Three subtests each sit out the full lock_timeout by design.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()

	scratch := scratchDatabase(ctx, t, admin)
	dbname := scratch.Config().Database
	// A database owner can leave a lock_timeout of its own behind. What Open
	// sends is a startup parameter, which outranks an ALTER DATABASE/ROLE SET,
	// so the bound must survive this.
	if _, err := admin.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s SET lock_timeout = 0", Quote(dbname))); err != nil {
		t.Fatalf("seed a database-level lock_timeout: %v", err)
	}
	conn := openViaOperator(ctx, t, dsn, dbname)

	// pg_settings reports lock_timeout in milliseconds, so this compares against
	// the constant rather than against however Postgres would spell it.
	var configured int64
	if err := conn.QueryRow(ctx,
		"SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'lock_timeout'").Scan(&configured); err != nil {
		t.Fatalf("read lock_timeout: %v", err)
	}
	if configured != lockWaitTimeout.Milliseconds() {
		t.Fatalf("connection opened by Open has lock_timeout = %dms, want %dms (0 means every statement waits forever)",
			configured, lockWaitTimeout.Milliseconds())
	}

	role := fmt.Sprintf("lockwait_%x", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN", Quote(role))); err != nil {
		t.Fatalf("create fixture role: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), fmt.Sprintf("DROP ROLE IF EXISTS %s", Quote(role)))
	})

	t.Run("EnsureSchema", func(t *testing.T) {
		// A tenant that owns its database creating the same schema: the row is
		// not visible to CREATE SCHEMA IF NOT EXISTS, so the insert waits on the
		// uncommitted transaction instead of skipping.
		release := hold(ctx, t, scratch, `CREATE SCHEMA "app"`)
		defer release()

		pid := conn.PgConn().PID()
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- EnsureSchema(ctx, conn, "app") }()
		awaitBlocked(ctx, t, admin, pid, done, "EnsureSchema")
		assertLockTimeout(t, done, start, "EnsureSchema")
	})

	t.Run("EnsureRole", func(t *testing.T) {
		// A managed login role acting on its own pg_authid row — ALTER ROLE on
		// oneself needs no privileges — holds the tuple EnsureRole's
		// unconditional ALTER has to update.
		release := hold(ctx, t, scratch, fmt.Sprintf("ALTER ROLE %s CONNECTION LIMIT 3", Quote(role)))
		defer release()

		pid := conn.PgConn().PID()
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- EnsureRole(ctx, conn, role, "s3cret", -1) }()
		awaitBlocked(ctx, t, admin, pid, done, "EnsureRole")
		assertLockTimeout(t, done, start, "EnsureRole")
	})

	t.Run("GrantConnect", func(t *testing.T) {
		// A database owner granting on its own database holds the pg_database
		// row every grant in the RoleClaim path updates.
		release := hold(ctx, t, scratch,
			fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", Quote(dbname), Quote(role)))
		defer release()

		pid := conn.PgConn().PID()
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- GrantConnect(ctx, conn, dbname, role) }()
		awaitBlocked(ctx, t, admin, pid, done, "GrantConnect")
		assertLockTimeout(t, done, start, "GrantConnect")
	})
}

// openViaOperator opens a connection to the named database through Open, the
// production connect path, with the connection details taken from the test DSN.
func openViaOperator(ctx context.Context, t *testing.T, dsn, database string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	sslmode := "disable"
	if cfg.TLSConfig != nil {
		sslmode = "require"
	}
	conn, err := Open(ctx, ConnOpts{
		Host:     cfg.Host,
		Port:     int(cfg.Port),
		Database: database,
		User:     cfg.User,
		Password: cfg.Password,
		SSLMode:  sslmode,
	})
	if err != nil {
		t.Fatalf("open %q through postgres.Open: %v", database, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// hold runs stmt on a fresh connection inside a transaction that is left open,
// so whatever catalog rows the statement touched stay locked until the returned
// function rolls it back.
func hold(ctx context.Context, t *testing.T, like *pgx.Conn, stmt string) func() {
	t.Helper()
	holder := secondConnection(ctx, t, like)
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holding transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, stmt); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("hold with %q: %v", stmt, err)
	}
	return func() { _ = tx.Rollback(context.Background()) }
}

// assertLockTimeout requires the work on done to have given up on its lock wait
// with SQLSTATE 55P03, after waiting the full lockWaitTimeout and no longer.
func assertLockTimeout(t *testing.T, done <-chan error, start time.Time, what string) {
	t.Helper()
	select {
	case err := <-done:
		waited := time.Since(start)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("%s: got err %v, want a lock_timeout abort (SQLSTATE 55P03)", what, err)
		}
		if waited < lockWaitTimeout {
			t.Fatalf("%s gave up after %v, before the %v it is meant to wait", what, waited, lockWaitTimeout)
		}
	case <-time.After(lockWaitTimeout + 20*time.Second):
		t.Fatalf("%s is still waiting after %v on a lock another session holds; the wait is unbounded",
			what, time.Since(start))
	}
}
