module github.com/tahaNGU/gocast/postgres/pgxcompat

go 1.24

require (
	github.com/jackc/pgx/v5 v5.5.4
	github.com/tahaNGU/gocast v0.1.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

// The core module lives in this repository.
replace github.com/tahaNGU/gocast => ../..
