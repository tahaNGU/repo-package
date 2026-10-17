// Package pgxcompat exists only to prove, in CI, that the PostgreSQL store is
// driver-independent: it exercises the full state machine through
// jackc/pgx/v5's database/sql driver rather than lib/pq, which the rest of the
// test suite uses.
//
// It is a separate Go module so that neither driver becomes a dependency of
// the library itself. Run it like the other integration tests:
//
//	cd postgres/pgxcompat && go test ./...
//
// It skips without OUTBOX_TEST_POSTGRES_DSN.
package pgxcompat
