module github.com/tahaNGU/gocast/contrib/otel

go 1.24

require (
	github.com/tahaNGU/gocast v0.1.0
	go.opentelemetry.io/otel v1.38.0
	go.opentelemetry.io/otel/trace v1.38.0
)

require (
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk v1.38.0
	golang.org/x/sys v0.35.0 // indirect
)

// The core module lives in this repository. The directive makes this module
// build standalone against the working tree; it is ignored by anything that
// imports this module, which resolves the required version normally.
replace github.com/tahaNGU/gocast => ../..
