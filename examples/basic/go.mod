module github.com/tahaNGU/gocast/examples/basic

go 1.24

require (
	github.com/lib/pq v1.10.9
	github.com/tahaNGU/gocast v0.1.0
)

// The core module lives in this repository.
replace github.com/tahaNGU/gocast => ../..
