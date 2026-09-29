// Package backend is RunnerQ's storage on RunnerQ Cloud's data plane.
package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
	"github.com/runnerq/cloud-storage-go/protocol"
)

// CloudBackend is one queue's storage in a RunnerQ Cloud store.
type CloudBackend struct {
	endpoint, queue, auth, queueURL string
	client                          *http.Client
	heartbeat, reportGap            time.Duration
	mu                              sync.Mutex
	reporters                       map[string]func() // executor id -> stop
}

// Option configures a CloudBackend.
type Option func(*CloudBackend)

// WithEndpoint sets the data plane's address (required).
func WithEndpoint(endpoint string) Option {
	return func(b *CloudBackend) { b.endpoint = strings.TrimRight(endpoint, "/") }
}

// WithQueue sets the queue (default "default").
func WithQueue(queue string) Option { return func(b *CloudBackend) { b.queue = queue } }

// WithHTTPClient sets the HTTP client; redirects are disabled on a copy of it.
func WithHTTPClient(client *http.Client) Option { return func(b *CloudBackend) { b.client = client } }

// NewCloudBackend creates the storage for one queue in apiKey's store (a store
// key reaches every queue in it). The endpoint must be HTTPS, or HTTP on loopback.
func NewCloudBackend(apiKey string, options ...Option) (*CloudBackend, error) {
	b := &CloudBackend{queue: "default", client: &http.Client{Transport: defaultTransport()}, heartbeat: protocol.HeartbeatInterval,
		reportGap: protocol.MinReportGap, reporters: map[string]func(){}}
	for _, o := range options {
		o(b)
	}
	u, err := url.Parse(b.endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
		return nil, storage.NewConfigurationError("provide an HTTPS data-plane endpoint (HTTP is allowed on loopback)")
	}
	if apiKey == "" || b.queue == "" || b.client == nil {
		return nil, storage.NewConfigurationError("API key, queue and HTTP client are required")
	}
	// A followed redirect would forward the key or replay a write.
	c := *b.client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	b.client = &c
	b.auth = "Bearer " + apiKey
	b.queueURL = b.endpoint + "/v1/queues/" + url.PathEscape(b.queue) + "/"
	return b, nil
}

// defaultTransport is http.DefaultTransport keeping MaxIdleConns (not two)
// idle connections per host, so concurrent HTTP/1.1 calls don't redial.
var defaultTransport = sync.OnceValue(func() http.RoundTripper {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport
	}
	t = t.Clone()
	t.MaxIdleConnsPerHost = t.MaxIdleConns
	return t
})

// SchedulesNatively is true: dequeue handles scheduled activities.
func (b *CloudBackend) SchedulesNatively() bool { return true }

// MaintenanceManaged is true: the data plane runs lease recovery and retention.
func (b *CloudBackend) MaintenanceManaged() bool { return true }

// call runs a storage operation on the queue. storaged bounds a request at
// 30s and a long poll at 25s; the extra 5s is for the network.
func (b *CloudBackend) call(ctx context.Context, method string, args, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	return b.request(ctx, http.MethodPost, b.queueURL+method, args, out)
}

// request sends args (none if nil) and decodes the envelope's result into out
// (if not nil). A lost or garbled reply is unavailable: the write may have committed.
func (b *CloudBackend) request(ctx context.Context, verb, target string, args, out any) error {
	var body io.Reader
	if args != nil {
		data, err := json.Marshal(args)
		if err != nil {
			return storage.NewSerializationError("encode storage request")
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, verb, target, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", b.auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("RunnerQ-Storage-Version", protocol.Version)
	res, err := b.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return storage.NewUnavailableError("storage transport failed; outcome may be unknown")
	}
	defer res.Body.Close()
	// Read to EOF so the connection is reused.
	data, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	var response protocol.Response
	if err == nil {
		err = json.Unmarshal(data, &response)
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return storage.NewUnavailableError("invalid or incomplete storage response; outcome may be unknown")
	}
	if response.Error != nil {
		return response.Error.StorageError()
	}
	if res.StatusCode != http.StatusOK || res.Header.Get("RunnerQ-Storage-Version") != protocol.Version {
		return storage.NewUnavailableError("unexpected storage response")
	}
	if out != nil {
		if err := json.Unmarshal(response.Result, out); err != nil {
			return storage.NewSerializationError("decode storage result")
		}
	}
	return nil
}

// WaitForResult long-polls, asking again each time the data plane's wait times out.
func (b *CloudBackend) WaitForResult(ctx context.Context, id uuid.UUID) (*storage.ActivityResult, error) {
	for {
		var result *storage.ActivityResult
		err := b.call(ctx, "WaitForResult", protocol.GetResultArgs{ActivityID: id}, &result) // GetResult's arguments
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrTimeout {
			continue
		}
		return result, err
	}
}

var (
	_ storage.Storage                   = (*CloudBackend)(nil)
	_ storage.BatchQueueStorage         = (*CloudBackend)(nil)
	_ storage.AttemptLeaseStorage       = (*CloudBackend)(nil)
	_ storage.CheckpointStorage         = (*CloudBackend)(nil)
	_ storage.SpawnStorage              = (*CloudBackend)(nil)
	_ storage.DependencyStorage         = (*CloudBackend)(nil)
	_ storage.ResultWaiter              = (*CloudBackend)(nil)
	_ storage.ManagedMaintenanceStorage = (*CloudBackend)(nil)
)
