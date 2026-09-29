package backend

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/runnerq/cloud-storage-go/protocol"
)

// ExecutorStarted reports the engine to the data plane (for the console's
// Fleet) now, every heartbeat and soon after each change, until it stops.
// Failures are logged and never affect the engine.
func (b *CloudBackend) ExecutorStarted(src executor.Source) {
	id := src.Snapshot().Info.ID
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	b.mu.Lock()
	// Stopping sends one last report: the loop's may be a heartbeat old, and
	// Fleet keeps what a stopped worker last said.
	b.reporters[id] = func() {
		cancel()
		<-done
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.executorCall(ctx, http.MethodPut, id, reportOf(src.Snapshot())); err != nil {
			slog.Warn("RunnerQ Cloud: final executor report failed", "executor", id, "error", err)
		}
	}
	b.mu.Unlock()

	go func() {
		defer close(done)
		executor.Report(ctx, src, func() time.Duration { return b.heartbeat }, b.reportGap, func() {
			if err := b.executorCall(ctx, http.MethodPut, id, reportOf(src.Snapshot())); err != nil && ctx.Err() == nil {
				slog.Warn("RunnerQ Cloud: executor report failed; retrying", "executor", id, "error", err)
			}
		})
	}()
}

// ExecutorStopped stops the engine's reports, sends a final one and says
// goodbye, so the console shows a clean stop rather than a lost worker.
func (b *CloudBackend) ExecutorStopped(id string) {
	b.mu.Lock()
	stop := b.reporters[id]
	delete(b.reporters, id)
	b.mu.Unlock()
	if stop != nil {
		stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.executorCall(ctx, http.MethodDelete, id, nil); err != nil {
		slog.Warn("RunnerQ Cloud: executor goodbye failed", "executor", id, "error", err)
	}
}

// executorCall PUTs a report or DELETEs (goodbye) the executor. Reporting is
// best effort, so its timeout is shorter than a storage call's.
func (b *CloudBackend) executorCall(ctx context.Context, method, id string, body any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return b.request(ctx, method, b.endpoint+"/v1/executors/"+url.PathEscape(id), body, nil)
}

// reportOf is a snapshot as a conductor agent would report it.
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
			Draining: st.Draining, Running: make([]protocol.RunningActivity, len(st.Running)),
			Counters: &protocol.Counters{
				Claimed: c.Claimed, Succeeded: c.Succeeded, Retried: c.Retried, Failed: c.Failed,
				TimedOut: c.TimedOut, DeadLettered: c.DeadLettered, ClaimsLost: c.ClaimsLost,
			},
		},
	}
	for i, a := range st.Running {
		r.State.Running[i] = protocol.RunningActivity{
			ActivityID: a.ID.String(), Type: a.Type, Attempt: a.Attempt, StartedAt: timestamp(a.StartedAt),
		}
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
