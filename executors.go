package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/runnerq/runnerq-cloud-storage-go/protocol"
)

// WithLabels tags this worker's executors in RunnerQ Cloud's Fleet (region,
// deploy version, ...).
func WithLabels(labels map[string]string) Option {
	return func(b *CloudBackend) { b.labels = maps.Clone(labels) }
}

// reporters holds the heartbeat of each engine running on the backend.
type reporters struct {
	mu   sync.Mutex
	stop map[string]func()
}

// ExecutorStarted reports the engine to the data plane now and every
// protocol.HeartbeatInterval until it stops, so the console shows it in
// Fleet. Reporting never affects the engine: failures are logged and
// retried at the next beat.
func (b *CloudBackend) ExecutorStarted(info storage.ExecutorInfo, state func() storage.ExecutorState) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	b.reporters.mu.Lock()
	if b.reporters.stop == nil {
		b.reporters.stop = map[string]func(){}
	}
	b.reporters.stop[info.ID] = func() { cancel(); <-done }
	b.reporters.mu.Unlock()

	host, _ := os.Hostname()
	go func() {
		defer close(done)
		tick := time.NewTicker(b.heartbeat)
		defer tick.Stop()
		for {
			st := state()
			report := protocol.ExecutorReport{
				Queue: info.Queue, Hostname: host, SDK: sdkInfo(), ActivityTypes: info.ActivityTypes,
				MaxConcurrency: info.MaxConcurrency, StartedAt: info.StartedAt, Draining: st.Draining,
				Running: make([]protocol.RunningActivity, 0, len(st.Running)), Labels: b.labels,
			}
			for _, r := range st.Running {
				report.Running = append(report.Running, protocol.RunningActivity{
					ActivityID: r.ID.String(), Type: r.Type, Attempt: r.Attempt, StartedAt: r.StartedAt,
				})
			}
			if err := b.executorCall(ctx, http.MethodPut, info.ID, report); err != nil && ctx.Err() == nil {
				slog.Warn("RunnerQ Cloud: executor report failed; retrying", "executor", info.ID, "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
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

// sdkInfo names the RunnerQ SDK compiled into the worker.
func sdkInfo() protocol.SDKInfo {
	info := protocol.SDKInfo{Name: "runnerq-go", Language: "go"}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range bi.Deps {
			if dep.Path == "github.com/alob-mtc/runnerq-go" {
				info.Version = dep.Version
			}
		}
	}
	return info
}

var _ storage.ExecutorReportingStorage = (*CloudBackend)(nil)
