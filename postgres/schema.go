package postgres

import (
	"context"
	"embed"
	"fmt"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Migration is one SQL migration, rendered for a particular table name.
type Migration struct {
	// Name is the file name, for example "0001_outbox.up.sql".
	Name string
	// SQL is the rendered statement text. Every statement is idempotent, so
	// applying a migration twice is harmless.
	SQL string
	// Ordering reports whether the migration belongs to the optional ordered
	// delivery support ([WithOrdering]).
	Ordering bool
}

// Migrations returns the "up" migrations for table, in the order they must be
// applied. ordering includes the optional index that ordered delivery needs.
//
// Use it to feed your own migration tool:
//
//	ms, _ := postgres.Migrations(postgres.DefaultTable, false)
//	for _, m := range ms {
//		os.WriteFile(filepath.Join("db/migrations", m.Name), []byte(m.SQL), 0o644)
//	}
func Migrations(table string, ordering bool) ([]Migration, error) {
	return renderMigrations(table, ordering, "up")
}

// DownMigrations returns the "down" migrations for table, in the order they
// must be applied. The 0001 migration drops the table and therefore destroys
// undelivered messages.
func DownMigrations(table string, ordering bool) ([]Migration, error) {
	ms, err := renderMigrations(table, ordering, "down")
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(ms)-1; i < j; i, j = i+1, j-1 {
		ms[i], ms[j] = ms[j], ms[i]
	}
	return ms, nil
}

func renderMigrations(table string, ordering bool, direction string) ([]Migration, error) {
	qualified, prefix, err := parseTable(table)
	if err != nil {
		return nil, err
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: read migrations: %w", err)
	}
	var out []Migration
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, "."+direction+".sql") {
			continue
		}
		isOrdering := strings.Contains(name, "_ordering")
		if isOrdering && !ordering {
			continue
		}
		raw, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("outbox/postgres: read migration %s: %w", name, err)
		}
		sql := strings.NewReplacer("{{table}}", qualified, "{{prefix}}", prefix).Replace(string(raw))
		out = append(out, Migration{Name: name, SQL: sql, Ordering: isOrdering})
	}
	return out, nil
}

// Migrate creates the outbox table and its indexes if they do not exist,
// including the ordering index when the store was built with [WithOrdering].
// It is idempotent and safe to call on every start-up.
//
// It is a convenience for small deployments and for tests. If your project
// already owns schema migrations, render the SQL with [Migrations] and apply it
// with your own tool instead, so that schema changes stay in one place.
//
// Migrate does not run inside a transaction: each statement is applied in
// order and independently, so a failure part-way leaves the earlier statements
// applied. Re-running it completes the rest.
func (s *Store) Migrate(ctx context.Context) error {
	ms, err := Migrations(s.table, s.ordered)
	if err != nil {
		return err
	}
	for _, m := range ms {
		if _, err := s.db.ExecContext(ctx, m.SQL); err != nil {
			return fmt.Errorf("outbox/postgres: apply %s: %w", m.Name, err)
		}
	}
	return nil
}
