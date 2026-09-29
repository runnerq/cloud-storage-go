package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/alob-mtc/runnerq-go/storage"
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
	b.reporters.stop[id] = func() { cancel(); <-done }
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

// ExecutorStopped stops the engine's heartbeat and says goodbye, so the
// console shows a clean stop rather than a lost worker.
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

func (b *CloudBackend) executorCall(ctx context.Context, method, id string, body any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, b.endpoint+"/v1/executors/"+url.PathEscape(id), reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+b.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("RunnerQ-Storage-Version", protocol.Version)
	res, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	var response protocol.Response
	_ = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&response)
	if response.Error != nil {
		return response.Error.StorageError()
	}
	if res.StatusCode != http.StatusOK {
		return storage.NewUnavailableError("unexpected executor report response: " + res.Status)
	}
	return nil
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
