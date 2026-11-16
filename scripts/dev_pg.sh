#!/usr/bin/env bash
# Dev-only helper: run an unprivileged PostgreSQL cluster extracted from debs.
# Docker Compose runs the official postgres image instead; this script exists
# so the suite can self-test on a machine without Docker.
set -euo pipefail

PREFIX="${PG_PREFIX:-$HOME/pg}"
BIN="$PREFIX/usr/lib/postgresql/15/bin"
PGDATA="${PGDATA:-$HOME/pgdata}"
PGPORT="${PGPORT:-5439}"
PGUSER="${PGUSER:-node}"
PGDB="${PGDB:-meterdb}"

export LD_LIBRARY_PATH="$PREFIX/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"

if [ ! -d "$PGDATA/base" ]; then
  mkdir -p "$PGDATA"
  "$BIN/initdb" -D "$PGDATA" -U "$PGUSER" --auth=trust --encoding=UTF8 >/dev/null
  cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses = 'localhost'
port = $PGPORT
unix_socket_directories = '/tmp'
fsync = off
synchronous_commit = off
full_page_writes = off
max_connections = 100
EOF
fi

"$BIN/pg_ctl" -D "$PGDATA" -l "$HOME/pg.log" -w start
"$BIN/psql" -h localhost -p "$PGPORT" -U "$PGUSER" -d postgres \
  -tc "SELECT 1 FROM pg_database WHERE datname = '$PGDB'" | grep -q 1 \
  || "$BIN/createdb" -h localhost -p "$PGPORT" -U "$PGUSER" "$PGDB"
echo "PostgreSQL ready on localhost:$PGPORT db=$PGDB"
