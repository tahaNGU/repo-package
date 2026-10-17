# Development tasks. The integration tests and database benchmarks need
# OUTBOX_TEST_POSTGRES_DSN; without it they skip.
#
#   ./scripts/pg.sh up    # start a throwaway PostgreSQL and print the DSN
#   make check            # vet + tests + race, core and contrib modules

MODULES := . ./contrib/prometheus ./contrib/otel ./examples/basic ./postgres/pgxcompat

.PHONY: check build vet test race bench bench-db fmt tidy pg-up pg-down clean

check: vet test race

build:
	@for m in $(MODULES); do echo "== build $$m"; (cd $$m && go build ./...) || exit 1; done

vet:
	@for m in $(MODULES); do echo "== vet $$m"; (cd $$m && go vet ./...) || exit 1; done
	@gofmt -l . | grep -v '^$$' && { echo "unformatted files above; run make fmt"; exit 1; } || true

test:
	@for m in $(MODULES); do echo "== test $$m"; (cd $$m && go test -count=1 ./...) || exit 1; done

race:
	@for m in $(MODULES); do echo "== race $$m"; (cd $$m && go test -count=1 -race ./...) || exit 1; done

# Library-only benchmarks: no database required.
bench:
	go test -run XXX -bench 'NewID|IDString|BackoffDelay|PayloadSerialisation|WorkerPipeline|PublishFailurePath' \
		-benchtime=20000x -benchmem ./

# Database benchmarks. Needs OUTBOX_TEST_POSTGRES_DSN.
bench-db:
	go test -run XXX -bench 'Enqueue|Claim|Acknowledge|Stats' -benchtime=200x -benchmem ./postgres/
	go test -run XXX -bench 'EndToEnd' -benchtime=3000x -benchmem -timeout=30m ./postgres/

fmt:
	gofmt -w .

tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done

pg-up:
	./scripts/pg.sh up

pg-down:
	./scripts/pg.sh down
