#!/usr/bin/env bash
# The demonstration: one manager and three workers, the PDFs in demo/samples as jobs, one worker
# killed with SIGKILL while it holds a job. Exits 0 only if no job was lost and at least one was
# reassigned.
#
#   bash demo/demo.sh             # needs target/*-service.jar: mvn package -DskipTests
#   COMPOSE=docker-compose bash demo/demo.sh
#   CONSOLE_PORT=9080 SUMMARY_PORT=9081 bash demo/demo.sh   # if 8080 or 8081 is taken
#
# Leaves the stack running so the result can be looked at; `$COMPOSE down -v` removes it.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=${COMPOSE:-$(command -v podman-compose || command -v docker-compose || echo "docker compose")}
export CONSOLE_PORT=${CONSOLE_PORT:-8080} SUMMARY_PORT=${SUMMARY_PORT:-8081}
SUMMARY=http://localhost:$SUMMARY_PORT/jobs/summary
JOBS=$(ls demo/samples/*.pdf | wc -l)
sql() { $COMPOSE exec -T postgres psql -U agenor -d jobs -tAc "$1" 2>/dev/null | tr -d '\r'; }
say() { printf '\n== %s\n' "$*"; }
fail() { printf '\nFAILED: %s\n' "$*" >&2; exit 1; }

say "starting the stack (compose: $COMPOSE)"
$COMPOSE down -v >/dev/null 2>&1 || true
# With rootful Docker the workers write demo-data/out as root: the host user cannot delete those
# files, so a container (root as well) empties the directory instead.
if ! rm -rf demo-data 2>/dev/null; then
  $COMPOSE run --rm --no-deps -T --entrypoint sh manager -c 'rm -rf /data/* /data/.[!.]*' >/dev/null
  rm -rf demo-data
fi
mkdir -p demo-data/in
cp demo/samples/*.pdf demo-data/in/
$COMPOSE up -d --build >/dev/null

say "waiting for the manager and three workers"
for _ in $(seq 1 90); do
  workers=$(sql "SELECT count(*) FROM agenor_agents WHERE agent_id LIKE 'worker-%'" || true)
  if curl -sf "$SUMMARY" >/dev/null 2>&1 && [ "${workers:-0}" -ge 3 ]; then break; fi
  sleep 2
done
[ "${workers:-0}" -ge 3 ] || fail "only ${workers:-0} workers registered after 3 minutes"

say "submitting $JOBS jobs"
$COMPOSE -f docker-compose.yml -f docker-compose.submit.yml run --rm submit | grep '^submitted' | tee demo-data/submitted.txt

say "waiting for a worker that holds a job"
victim=""
for _ in $(seq 1 60); do
  victim=$(sql "SELECT assignee FROM jobs WHERE state='ASSIGNED' GROUP BY assignee ORDER BY count(*) DESC, assignee LIMIT 1" || true)
  [ -n "$victim" ] && break
  sleep 0.5
done
[ -n "$victim" ] || fail "no job was ever ASSIGNED"
held=$(sql "SELECT string_agg(job_id, ' ') FROM jobs WHERE state='ASSIGNED' AND assignee='$victim'")
say "kill -9 $victim, which holds: $held"
killed_at=$(date +%s)
$COMPOSE kill -s KILL "$victim" >/dev/null

say "waiting for every job to finish (the killed worker's jobs wait for their deadline)"
for _ in $(seq 1 120); do
  open=$(sql "SELECT count(*) FROM jobs WHERE state IN ('SUBMITTED','ASSIGNED')")
  [ "${open:-1}" -eq 0 ] && break
  sleep 2
done

finished_at=$(date +%s)
say "result"
echo "all jobs settled $((finished_at - killed_at)) s after the kill"
sql "SELECT state, count(*) FROM jobs GROUP BY state ORDER BY state"
echo "-- summary endpoint:"; curl -s "$SUMMARY"; echo
reassigned=$(sql "SELECT job_id || ' (attempt ' || attempt || ', now with ' || assignee || ')' FROM jobs WHERE attempt > 1 ORDER BY job_id")
echo "-- reassigned jobs:"; echo "${reassigned:-none}"
total=$(sql "SELECT count(*) FROM jobs"); done_=$(sql "SELECT count(*) FROM jobs WHERE state='DONE'")
outputs=$(ls demo-data/out 2>/dev/null | wc -l)

if [ -n "$reassigned" ]; then
  first=$(echo "$reassigned" | head -1 | cut -d' ' -f1)
  echo; echo "Jaeger: http://localhost:16686/search?service=jobs-manager-manager&tags=%7B%22job.id%22%3A%22$first%22%7D"
  echo "(or search any service for the tag job.id=$first)"
fi

[ "$total" -eq "$JOBS" ]  || fail "$total jobs in the table, expected $JOBS"
[ "$done_" -eq "$JOBS" ]  || fail "$done_ of $JOBS jobs DONE"
[ "$outputs" -eq "$JOBS" ] || fail "$outputs output files, expected $JOBS"
[ -n "$reassigned" ]      || fail "no job was reassigned: the kill did not land on a job (rerun)"
echo; echo "OK: $JOBS of $JOBS done, $(echo "$reassigned" | wc -l) reassigned after killing $victim"
