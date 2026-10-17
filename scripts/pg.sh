#!/usr/bin/env bash
# Starts or stops a throwaway PostgreSQL for the integration tests and
# benchmarks. It listens on 55432 so it cannot collide with a local server.
set -euo pipefail

NAME=${OUTBOX_PG_CONTAINER:-outbox-pg}
PORT=${OUTBOX_PG_PORT:-55432}
IMAGE=${OUTBOX_PG_IMAGE:-postgres:16-alpine}
DSN="postgres://outbox:outbox@127.0.0.1:${PORT}/outbox?sslmode=disable"

case "${1:-up}" in
up)
	if [ -n "$(docker ps -q -f "name=^${NAME}$")" ]; then
		echo "${NAME} is already running"
	else
		docker rm -f "${NAME}" >/dev/null 2>&1 || true
		docker run -d --name "${NAME}" \
			-e POSTGRES_USER=outbox -e POSTGRES_PASSWORD=outbox -e POSTGRES_DB=outbox \
			-p "${PORT}:5432" "${IMAGE}" >/dev/null
		printf 'waiting for postgres'
		for _ in $(seq 1 60); do
			if docker exec "${NAME}" pg_isready -U outbox -q 2>/dev/null; then
				echo " ready"
				break
			fi
			printf '.'
			sleep 1
		done
	fi
	echo
	echo "export OUTBOX_TEST_POSTGRES_DSN='${DSN}'"
	;;
down)
	docker rm -f "${NAME}" >/dev/null 2>&1 || true
	echo "${NAME} removed"
	;;
dsn)
	echo "${DSN}"
	;;
*)
	echo "usage: $0 [up|down|dsn]" >&2
	exit 2
	;;
esac
