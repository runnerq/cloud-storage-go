# RunnerQ Cloud storage adapter for Go

An external implementation of RunnerQ's public storage interfaces. The worker
still executes handlers locally; this adapter persists and coordinates execution
through the independent RunnerQ data plane.

```go
import (
    "os"
    "github.com/alob-mtc/runnerq-go"
    backend "github.com/runnerq/cloud-storage-go"
)

cloud, err := backend.NewCloudBackend(
    os.Getenv("RUNNERQ_STORE_KEY"),
    backend.WithEndpoint("https://your-storage-service.example"),
    backend.WithQueue("payments"),
)
if err != nil {
    return err
}
engine, err := runnerq.Builder().
    QueueName("payments").
    Backend(cloud).
    Build()
```

The endpoint is required until there is a public hosted default. HTTP is allowed
only on loopback for development. `WithHTTPClient` accepts a custom client;
redirects are always disabled to avoid forwarding credentials or replaying writes.
A store key belongs to one hosted store and reaches any queue in it; a queue
appears in the store the first time a worker uses it. Keys and retention are
managed in the RunnerQ Cloud console (or the data-plane admin API).

Each engine built on the backend reports itself to the data plane every 10
seconds, and within about two seconds of a change (an activity starting or
finishing, or a drain beginning), and says goodbye when it stops, so RunnerQ Cloud's Fleet shows a
hosted app's workers with no agent. The report is the engine's snapshot
(identity, labels from `WorkerConfig.Labels`, activities in flight, outcome
counters, claim lag), in the same shape a conductor agent reports. The
backend is an `executor.Observer`, which the engine attaches on its own;
reporting failures are logged and never affect the worker.

Implemented capabilities: `storage.Storage`, `BatchQueueStorage`, `ResultWaiter`,
`AttemptLeaseStorage`, `CheckpointStorage`, `SpawnStorage`, `DependencyStorage`
and `ManagedMaintenanceStorage`. Queue scheduling, reaping and retention are
service-owned. Configure retention through the service, not the worker builder.
General `QueryStorage` and `CommandStorage` are not advertised in this version.

Requests honor context cancellation. The client does not automatically retry
mutations: a failed HTTP response can mean the write committed. The engine's
existing recovery logic retries fenced acknowledgements and checkpoints with the
same inputs; the backend reconciles them. For producer retries, use a stable
business idempotency key with return-existing behavior. A lost dequeue response
leaves a claim that service maintenance recovers after its lease expires.
Result waits renew bounded long polls.

## Development

For a complete runnable worker, see [Hello Cloud](examples/hello-cloud/README.md).
It runs two durable checkpoints and reruns the same request to retrieve its
persisted result:

```sh
go run ./examples/hello-cloud -name Ada -request-id hello-001
```

This module pins RunnerQ to an unreleased `main` commit, and the data plane pins
a published version of this module. Before publishing, release compatible
RunnerQ and adapter versions.

```sh
go test -race ./...
python3 tools/generate.py
```

`RUNNERQ_GO_SDK` and `RUNNERQ_CLOUD` point the generator at other checkouts
(default: the siblings `../../runnerq-go-sdk` and `../../runnerq-cloud`).
The generator refreshes the 32 typed operation bindings in this module and the
sibling data-plane dispatcher from the local storage interfaces. The wire contract
is versioned: review compatibility before regenerating for SDK changes. Shared
wire structures are in `protocol/`; the adapter imports no service internals.

[Service setup](https://github.com/runnerq/runnerq-cloud/blob/main/dataplane/README.md) and
[HTTP protocol](https://github.com/runnerq/runnerq-cloud/blob/main/dataplane/docs/protocol.md).
