package backend

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
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
	b, err := NewCloudBackend("rqh_secret", WithEndpoint(server.URL), WithQueue("payments"), WithLabels(map[string]string{"region": "eu"}))
	if err != nil {
		t.Fatal(err)
	}
	b.heartbeat = 20 * time.Millisecond
	running := storage.RunningActivity{ID: uuid.New(), Type: "charge", Attempt: 2, StartedAt: time.Now().UTC()}
	started := time.Now().UTC()
	b.ExecutorStarted(storage.ExecutorInfo{ID: "exec-1", Queue: "payments", ActivityTypes: []string{"charge"}, MaxConcurrency: 4, StartedAt: started},
		func() storage.ExecutorState { return storage.ExecutorState{Running: []storage.RunningActivity{running}} })

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
	if first.method != http.MethodPut || first.path != "/v1/executors/exec-1" || first.auth != "Bearer rqh_secret" ||
		r.Queue != "payments" || r.MaxConcurrency != 4 || r.Labels["region"] != "eu" || r.SDK.Name != "runnerq-go" ||
		len(r.Running) != 1 || r.Running[0].ActivityID != running.ID.String() || r.Running[0].Attempt != 2 || !r.StartedAt.Equal(started) {
		t.Fatalf("first report: %+v", first)
	}
	last := got[len(got)-1]
	if last.method != http.MethodDelete || last.path != "/v1/executors/exec-1" {
		t.Fatalf("last call: %+v", last)
	}
	if after != len(got) {
		t.Fatalf("%d calls after the goodbye", after-len(got))
	}
}
