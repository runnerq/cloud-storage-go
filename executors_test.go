package backend

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/google/uuid"
	"github.com/runnerq/runnerq-cloud-storage-go/protocol"
)

func TestExecutorReports(t *testing.T) {
	type call struct {
		method, path, auth string
		report             protocol.ExecutorReport
	}
	var mu sync.Mutex
	var calls []call
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := call{method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization")}
		_ = json.NewDecoder(r.Body).Decode(&c.report)
		mu.Lock()
		calls = append(calls, c)
		mu.Unlock()
		_, _ = w.Write([]byte(`{"result":null}`))
	}))
	defer server.Close()
	b, err := NewCloudBackend("rqh_secret", WithEndpoint(server.URL), WithQueue("payments"))
	if err != nil {
		t.Fatal(err)
	}
	b.heartbeat = 20 * time.Millisecond
	started := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	running := executor.Running{ID: uuid.New(), Type: "charge", Attempt: 2, StartedAt: started.Add(time.Minute)}
	src := source{
		Info: executor.Info{
			ID: "exec-1", Queue: "payments", ActivityTypes: []string{"charge"}, MaxConcurrency: 4, StartedAt: started,
			Hostname: "worker-a", SDK: executor.SDK{Name: "runnerq-go", Version: "v1.2.3", Language: "go"},
			Labels: map[string]string{"region": "eu"},
		},
		State:    executor.State{Running: []executor.Running{running}},
		Counters: executor.Counters{Claimed: 5, Succeeded: 3, DeadLettered: 1, HeartbeatFailures: 2, LastClaimLag: 1500 * time.Millisecond},
		At:       started.Add(2 * time.Minute),
	}
	b.ExecutorStarted(src)

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(calls)
		mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d heartbeats", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.ExecutorStopped("exec-1")
	mu.Lock()
	got := append([]call(nil), calls...)
	mu.Unlock()
	time.Sleep(60 * time.Millisecond) // no beats after the goodbye
	mu.Lock()
	after := len(calls)
	mu.Unlock()

	first := got[0]
	r := first.report
	ex, st := r.Executor, r.State
	if first.method != http.MethodPut || first.path != "/v1/executors/exec-1" || first.auth != "Bearer rqh_secret" ||
		r.SDK.Version != "v1.2.3" || ex.ID != "exec-1" || ex.Hostname != "worker-a" || len(ex.Queues) != 1 || ex.Queues[0] != "payments" ||
		ex.MaxConcurrency != 4 || ex.Labels["region"] != "eu" || ex.StartedAt != "2026-09-29T12:00:00.000Z" {
		t.Fatalf("first report: %+v", first)
	}
	if st.ID != "exec-1" || st.UptimeMS != 120_000 || st.InFlight != 1 || len(st.Running) != 1 ||
		st.Running[0].ActivityID != running.ID.String() || st.Running[0].Attempt != 2 || st.Running[0].StartedAt != "2026-09-29T12:01:00.000Z" ||
		st.ClaimLagMS != 1500 || st.HeartbeatFailures != 2 || st.Counters == nil || st.Counters.Claimed != 5 || st.Counters.DeadLettered != 1 {
		t.Fatalf("first state: %+v", st)
	}
	last := got[len(got)-1]
	if last.method != http.MethodDelete || last.path != "/v1/executors/exec-1" {
		t.Fatalf("last call: %+v", last)
	}
	if after != len(got) {
		t.Fatalf("%d calls after the goodbye", after-len(got))
	}
}

// source is a fixed snapshot.
type source executor.Snapshot

func (s source) Snapshot() executor.Snapshot { return executor.Snapshot(s) }
