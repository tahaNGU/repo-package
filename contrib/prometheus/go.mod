module github.com/tahaNGU/gocast/contrib/prometheus

go 1.24

require (
	github.com/prometheus/client_golang v1.19.1
	github.com/tahaNGU/gocast v0.1.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.48.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	golang.org/x/sys v0.17.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
)

// The core module lives in this repository. The directive makes this module
// build standalone against the working tree; it is ignored by anything that
// imports this module, which resolves the required version normally.
replace github.com/tahaNGU/gocast => ../..
