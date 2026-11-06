# agenor-example-job-dispatcher

A job dispatcher built on [Agenor](https://github.com/mauro-mura/agenor) 0.40.0. A **manager** hands
PDF-to-text jobs to **workers** of different speeds by Contract Net (call for proposals, the lowest
estimate wins), keeps every job in PostgreSQL, and gives a job to someone else when its worker dies.

```
submit ──jobs.submitted──▶ manager ──CFP──▶ workers      Valkey: messages (Agenor Redis streams)
                              │   ◀─PROPOSE─┘            PostgreSQL: the job table + Agenor's directory
                              ├──AGREE──▶ winner         Jaeger: spans job.auction / job.execute / job.expire
                              ◀──INFORM/FAILURE── worker
```

The manager is the only writer of the job table. A job is `SUBMITTED → ASSIGNED → DONE | FAILED`;
every transition is one guarded statement, fenced by `(attempt, assignee, state)`, so a late result
from a worker that was given up on changes nothing. An assignment has a deadline of
`2 × estimate + margin`, kept in the table so it survives a restart of the manager; when it passes,
the job goes back to auction without the worker that held it, up to `MAX_ATTEMPTS`.

## Try it: the demonstration

Needs Java 21, Maven, and podman or docker with their compose.

```bash
mvn package -DskipTests          # builds target/agenor-example-job-dispatcher-*-service.jar
bash demo/demo.sh                # stack up, 12 jobs, kill -9 one worker, check
```

`demo/demo.sh` starts Valkey, PostgreSQL, Jaeger, one manager and three workers; submits the 12 PDFs
in `demo/samples`; waits until a worker holds a job; kills it with SIGKILL; and exits 0 only if all
12 jobs end `DONE` with their text extracted and at least one was reassigned. Then look:

- `curl localhost:8081/jobs/summary`: jobs per state, mean time to `DONE`, latest failures.
- Jaeger, <http://localhost:16686>: search the tag `job.id=<id>` (the script prints one). A reassigned
  job reads as `job.auction` (attempt 1) → `job.expire` → `job.auction` (attempt 2) → `job.execute`.
  The killed worker exports nothing for the job it died holding: its span never ended.
- <http://localhost:8080>: Agenor's own console (agents, dead letters). It does not know about jobs.

If 8080 or 8081 is taken, run it as `CONSOLE_PORT=9080 SUMMARY_PORT=9081 bash demo/demo.sh`.

`compose down -v` removes everything. Workers are **not** restarted when they die, on purpose: a job
that kills its worker would kill every restart too. Bring one back with `compose up -d worker-b`.

The workers pretend each job takes 4 s (`SIMULATED_WORK_MS`) so there is a moment in which to kill
one; a real extraction takes milliseconds. Without it, leave the variable out.

## Running it for real

One jar, three roles, configured only by environment variables; a missing one fails at start and
names itself.

```bash
java -jar target/agenor-example-job-dispatcher-0.1.0-SNAPSHOT-service.jar     # with NODE_ROLE=manager|worker|submit
```

| Variable | Roles | Default | |
|---|---|---|---|
| `NODE_ROLE` | all | | `manager`, `worker` or `submit` (publishes the PDFs under `STORAGE_ROOT/in` and exits) |
| `NODE_ID`, `AGENT_ID` | all | | stable across restarts: the node's stream and consumer derive from it |
| `REDIS_URI`, `JDBC_URL`, `JDBC_USER`, `JDBC_PASSWORD` | all | | |
| `STORAGE_ROOT` | worker, submit | `storage` | shared storage; results go to `out/<jobId>.txt` |
| `WORKER_CAPABILITIES` | worker | | kinds to run; only `pdf-to-text` exists |
| `CAPACITY`, `INITIAL_GUESS_MS` | worker | 5, 2000 | jobs in flight; first estimate before any measurement |
| `MAX_ATTEMPTS`, `CFP_TIMEOUT_MS`, `DEADLINE_MARGIN_MS`, `STALE_AFTER_MS`, `BATCH_SIZE` | manager | 3, 3000, 30000, 90000, 10 | deadline = 2 × estimate + margin |
| `HEARTBEAT_MS` | all | 10000 | must be well under `STALE_AFTER_MS` or workers stop being candidates, silently |
| `SUMMARY_PORT`, `CONSOLE_PORT` | manager | 8081, 8080 | `0` turns one off |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | all | off | OTLP over HTTP |
| `PENDING_MS` | all | 30000 | how long before an unacknowledged message is reclaimed |

On an empty database all processes may start at once; the compose does exactly that. The job table
has a Flyway history table of its own (`jobs_schema_history`), so it shares the database with
Agenor's directory in either order.

## Tests

```bash
mvn test                                                   # 26 unit tests, no containers
mvn verify -Pintegration                                   # + integration tests on real Valkey and PostgreSQL
mvn verify -Pfailure-tests                                 # + the failure tests, several minutes
```

The integration tests start their containers with Testcontainers and fail if there is no Docker; with
podman, point them at its socket:

```bash
export DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true
```

The failure tests kill a worker or the manager with SIGKILL, freeze Valkey or PostgreSQL, and send a
job that kills its workers. They start the service as child processes and kill them by the PID they
hold.

## What it does not do

No HTTP entry point (`submit` is the way in), no authentication, one auction at a time, one manager,
no cancellation (after `AGREE` the dialogue protocol allows only `INFORM` or `FAILURE`), no restart
of anything that dies.

## License

[Apache License 2.0](LICENSE).
