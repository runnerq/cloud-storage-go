# Hello Cloud

A runnable worker and producer using `backend.NewCloudBackend(...)`. It enqueues
a greeting workflow, runs two durable checkpoints, prints the result, and drains
the worker before exiting. The handler runs in this process; credentials,
activities, checkpoints and results are stored by the data plane.

## Run it

You need a running data plane and a store key. For a local one, follow the
development setup in
[the data plane's README](https://github.com/runnerq/runnerq-cloud/blob/main/dataplane/README.md):
it starts `storaged` on `http://localhost:8081`, provisions a store and issues
its key. With RunnerQ Cloud, the console creates the store and shows its key.
The key reaches every queue in its store: `hello_cloud` appears the first time
the worker uses it. Use HTTPS outside loopback.

From this module's root (`cloud-storage-sdk/go`):

```sh
export RUNNERQ_STORE_KEY='your-store-key'
export RUNNERQ_DATA_ENDPOINT='http://localhost:8081'   # the default

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
