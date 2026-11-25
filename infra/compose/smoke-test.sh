#!/usr/bin/env bash
# Proves the v1 local infrastructure actually works (Phase 0 exit check).
# Run after: docker compose -f infra/compose/docker-compose.yml up -d
# Works in CI (ubuntu) and in Git Bash on Windows.
set -euo pipefail

COMPOSE=(docker compose -f "$(dirname "$0")/docker-compose.yml")
TIMEOUT_SEC="${SMOKE_TIMEOUT_SEC:-240}"

# Retry a check until it passes or the timeout runs out.
wait_for() {
  local name="$1"; shift
  local deadline=$((SECONDS + TIMEOUT_SEC))
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "FAIL  $name (not ready after ${TIMEOUT_SEC}s)"
      return 1
    fi
    sleep 3
  done
  echo "OK    $name"
}

kafka_topics_ready() {
  local topics
  topics=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)
  for t in location-updates presence-events user-last-location location-updates.dlq; do
    grep -qx "$t" <<<"$topics" || return 1
  done
}

keycloak_ready() {
  curl -fsS http://localhost:8080/realms/tracker/.well-known/openid-configuration | grep -q '"issuer"'
}

redis_ready() {
  "${COMPOSE[@]}" exec -T redis redis-cli ping | grep -q PONG
}

postgres_extensions_ready() {
  local count
  count=$("${COMPOSE[@]}" exec -T timescaledb psql -U tracker -d tracker -tAc \
    "select count(*) from pg_available_extensions where name in ('timescaledb','postgis')")
  [[ "$(tr -d '[:space:]' <<<"$count")" == "2" ]]
}

kafka_ui_ready() {
  curl -fsS -o /dev/null http://localhost:8081/
}

failed=0
wait_for "Kafka: 4 topics exist"                         kafka_topics_ready        || failed=1
wait_for "Keycloak: realm 'tracker' discovery document"  keycloak_ready            || failed=1
wait_for "Redis: PING"                                    redis_ready               || failed=1
wait_for "Postgres: timescaledb + postgis available"      postgres_extensions_ready || failed=1
wait_for "Kafka UI: http://localhost:8081"                kafka_ui_ready            || failed=1

if (( failed )); then
  echo "Smoke test FAILED"
  "${COMPOSE[@]}" ps
  exit 1
fi
echo "Smoke test passed"
