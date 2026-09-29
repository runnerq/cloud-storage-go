package backend

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/runnerq/cloud-storage-go/protocol"
)

// reporters holds the heartbeat of each engine running on the backend.
type reporters struct {
	mu   sync.Mutex
	stop map[string]func()
}

// ExecutorStarted reports the engine to the data plane now, every
// protocol.HeartbeatInterval, and soon after it changes (at most once per
// protocol.MinReportGap) until it stops, so the console shows it in Fleet. The engine calls it (the backend is an executor.Observer).
// Reporting never affects the engine: failures are logged and retried at
// the next beat.
func (b *CloudBackend) ExecutorStarted(src executor.Source) {
	id := src.Snapshot().Info.ID
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	b.reporters.mu.Lock()
	if b.reporters.stop == nil {
		b.reporters.stop = map[string]func(){}
	}
	// Stopping ends the loop, then sends one last report: the loop's latest
	// may be up to a heartbeat old, and Fleet keeps what a stopped worker
	// last said.
	b.reporters.stop[id] = func() {
		cancel()
		<-done
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.executorCall(ctx, http.MethodPut, id, reportOf(src.Snapshot())); err != nil {
			slog.Warn("RunnerQ Cloud: final executor report failed", "executor", id, "error", err)
		}
	}
	b.reporters.mu.Unlock()

	go func() {
		defer close(done)
		every := func() time.Duration { return b.heartbeat }
		executor.Report(ctx, src, every, b.reportGap, func() {
			if err := b.executorCall(ctx, http.MethodPut, id, reportOf(src.Snapshot())); err != nil && ctx.Err() == nil {
				slog.Warn("RunnerQ Cloud: executor report failed; retrying", "executor", id, "error", err)
			}
		})
	}()
}

// ExecutorStopped stops the engine's heartbeat, sends a final report and
// says goodbye, so the console shows a clean stop, with the worker's final
// counts, rather than a lost worker.
func (b *CloudBackend) ExecutorStopped(id string) {
	b.reporters.mu.Lock()
	stop := b.reporters.stop[id]
	delete(b.reporters.stop, id)
	b.reporters.mu.Unlock()
	if stop != nil {
		stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.executorCall(ctx, http.MethodDelete, id, nil); err != nil {
		slog.Warn("RunnerQ Cloud: executor goodbye failed", "executor", id, "error", err)
	}
}

// executorCall reports to (PUT) or says goodbye to (DELETE) the executor
// endpoint, with a shorter timeout than storage calls: reporting is best
// effort and must not hold up the engine.
func (b *CloudBackend) executorCall(ctx context.Context, method, id string, body any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return b.request(ctx, method, "/v1/executors/"+url.PathEscape(id), body, nil)
}

// reportOf is a snapshot as an agent would report it.
func reportOf(snap executor.Snapshot) protocol.ExecutorReport {
	info, st, c := snap.Info, snap.State, snap.Counters
	r := protocol.ExecutorReport{
		SDK: protocol.SDKInfo{Name: info.SDK.Name, Version: info.SDK.Version, Language: info.SDK.Language},
		Executor: protocol.ExecutorInfo{
			ID: info.ID, Hostname: info.Hostname, Queues: []string{info.Queue}, ActivityTypes: info.ActivityTypes,
			MaxConcurrency: info.MaxConcurrency, StartedAt: timestamp(info.StartedAt), Labels: info.Labels,
		},
		State: protocol.ExecutorState{
			ID: info.ID, UptimeMS: snap.At.Sub(info.StartedAt).Milliseconds(), MaxConcurrency: info.MaxConcurrency,
			InFlight: len(st.Running), ClaimLagMS: c.LastClaimLag.Milliseconds(), HeartbeatFailures: c.HeartbeatFailures,
			Draining: st.Draining,
			Counters: &protocol.Counters{
				Claimed: c.Claimed, Succeeded: c.Succeeded, Retried: c.Retried, Failed: c.Failed,
				TimedOut: c.TimedOut, DeadLettered: c.DeadLettered, ClaimsLost: c.ClaimsLost,
			},
		},
	}
	for _, a := range st.Running {
		r.State.Running = append(r.State.Running, protocol.RunningActivity{
			ActivityID: a.ID.String(), Type: a.Type, Attempt: a.Attempt, StartedAt: timestamp(a.StartedAt),
		})
	}
	return r
}

func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

var _ executor.Observer = (*CloudBackend)(nil)
