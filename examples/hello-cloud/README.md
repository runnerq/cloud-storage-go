# Hello Cloud

A runnable worker and producer using `backend.NewCloudBackend(...)`. It enqueues
a greeting workflow, runs two durable checkpoints, prints the result, and drains
the worker before exiting. The handler runs in this process; credentials,
activities, checkpoints and results are stored by the data plane.

## 1. Start the local data plane

This checkout currently requires Go 1.27 and the sibling `runnerq-go-sdk` and
`runnerq-cloud` directories. Local PostgreSQL setup also requires Docker Compose.

From the `runnerq-org` workspace in terminal one:

```sh
cd runnerq-cloud/dataplane
docker compose -f compose.dev.yaml up -d --wait
export RUNNERQ_DATA_DSN='postgres://runnerq_data:local-data-only@localhost:55433/runnerq_data?sslmode=disable'
export RUNNERQ_DATA_ADMIN_TOKEN='local-admin-change-me'
go run ./cmd/storaged migrate
go run ./cmd/storaged serve -addr :8081
```

For an existing data plane, use its endpoint and a store key and skip local
setup. Use HTTPS outside loopback.

## 2. Provision a store and a key

In terminal two, provision the app's store with the data-plane admin credential,
then issue it a key:

```sh
curl --fail-with-body -sS http://localhost:8081/v1/admin/stores \
  -H 'Authorization: Bearer local-admin-change-me' \
  -H 'Content-Type: application/json' \
  -d '{"app_id":"11111111-1111-4111-8111-111111111111","retention_completed_seconds":86400,"retention_failed_seconds":604800}'

curl --fail-with-body -sS http://localhost:8081/v1/admin/stores/<result.id>/keys \
  -H 'Authorization: Bearer local-admin-change-me' \
  -H 'Content-Type: application/json' -d '{"name":"hello-cloud"}'
```

Copy `result.secret` from the second response into `RUNNERQ_STORE_KEY` below.
The example app UUID is a local placeholder; with RunnerQ Cloud, the console
creates the store and shows its key instead. The worker only needs the store key,
which works for any queue in the store: `hello_cloud` appears the first time the
worker uses it. The admin credential and database DSN belong to service setup,
not the worker.

## 3. Run the example

From the `runnerq-org` workspace in terminal two:

```sh
cd cloud-storage-sdk/go
export RUNNERQ_DATA_ENDPOINT='http://localhost:8081'
export RUNNERQ_DATA_QUEUE='hello_cloud'
export RUNNERQ_STORE_KEY='paste-result.secret-here'

go run ./examples/hello-cloud -name Ada -request-id hello-001
```

Alongside the SDK logs, output includes:

```text
request: hello-001
workflow: <activity UUID>
step: create-profile
step: prepare-greeting
result: {"profile":{"id":"<profile UUID>","name":"Ada"},"greeting":"Hello, Ada! Your workflow used RunnerQ Cloud storage."}
```

Run the same command again. While the workflow is retained, `ReturnExisting`
reattaches to the same activity: the workflow ID and result remain the same and
the two step messages do not run again. To submit new work, change `-request-id`
or omit it to generate a fresh UUID. Reusing an ID returns the original workflow
even if you change `-name`.

The named checkpoints also let a replay reuse completed step results. These
steps only generate demo data; external side effects still need appropriate
idempotency when a process can fail between the effect and checkpoint persistence.

The example waits up to one minute. On timeout or an interrupted process, run
again with the same request ID to reconnect; a terminated execution's lease is
recovered by the data plane. Retention and lease recovery are service-owned, so
the worker builder deliberately has no retention policy.

| Environment variable | Default |
|---|---|
| `RUNNERQ_STORE_KEY` | Required store key |
| `RUNNERQ_DATA_ENDPOINT` | `http://localhost:8081` |
| `RUNNERQ_DATA_QUEUE` | `hello_cloud` |

See [the adapter README](../../README.md) for capabilities and transport behavior.
