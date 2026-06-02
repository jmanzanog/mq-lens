#!/usr/bin/env sh
set -eu

HOST="${ACTIVEMQ_STOMP_HOST:-127.0.0.1}"
PORT="${ACTIVEMQ_STOMP_PORT:-61613}"
DESTINATION="${1:-ORDER.CREATED}"
BODY="${2:-{\"orderId\":\"local-1\",\"password\":\"secret\",\"status\":\"created\"}}"

if ! command -v nc >/dev/null 2>&1; then
  echo "nc is required" >&2
  exit 1
fi

{
  printf 'CONNECT\naccept-version:1.2\nhost:localhost\nlogin:admin\npasscode:admin\n\n\0'
  sleep 1
  printf 'SEND\ndestination:/queue/%s\ncontent-type:application/json\ncorrelation-id:local-correlation\n\n%s\0' "$DESTINATION" "$BODY"
  sleep 1
  printf 'DISCONNECT\n\n\0'
} | nc "$HOST" "$PORT"
