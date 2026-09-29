// Package backend provides the external RunnerQ Cloud storage adapter.
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

type CloudBackend struct {
	endpoint, key, queue string
	auth, queueURL       string
	client               *http.Client
	heartbeat            time.Duration
	reportGap            time.Duration
	reporters            reporters
}
type Option func(*CloudBackend)

func WithEndpoint(endpoint string) Option {
	return func(b *CloudBackend) { b.endpoint = strings.TrimRight(endpoint, "/") }
}
func WithQueue(queue string) Option             { return func(b *CloudBackend) { b.queue = queue } }
func WithHTTPClient(client *http.Client) Option { return func(b *CloudBackend) { b.client = client } }

// NewCloudBackend creates a client for one queue (WithQueue, default
// "default") in the store apiKey belongs to; a store key reaches every queue in
// its store. Endpoint is explicit until the hosted service has a public address. Plain HTTP is allowed only on loopback.
func NewCloudBackend(apiKey string, options ...Option) (*CloudBackend, error) {
	b := &CloudBackend{key: apiKey, queue: "default", client: &http.Client{Transport: defaultTransport()}, heartbeat: protocol.HeartbeatInterval, reportGap: protocol.MinReportGap}
	for _, o := range options {
		o(b)
	}
	u, err := url.Parse(b.endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
		return nil, storage.NewConfigurationError("provide an HTTPS data-plane endpoint (HTTP is allowed on loopback)")
	}
	if b.key == "" || b.queue == "" || b.client == nil {
		return nil, storage.NewConfigurationError("API key, queue and HTTP client are required")
	}
	// Do not leak credentials or replay a write through an HTTP redirect.
	c := *b.client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	b.client = &c
	b.auth = "Bearer " + b.key
	b.queueURL = b.endpoint + "/v1/queues/" + url.PathEscape(b.queue) + "/"
	return b, nil
}

// defaultTransport is http.DefaultTransport keeping up to MaxIdleConns idle
// connections to the data plane instead of two, so a busy worker's
// concurrent HTTP/1.1 calls reuse connections rather than redialing.
var defaultTransport = sync.OnceValue(func() http.RoundTripper {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport
	}
	t = t.Clone()
	t.MaxIdleConnsPerHost = t.MaxIdleConns
	return t
})

func (b *CloudBackend) SchedulesNatively() bool  { return true }
func (b *CloudBackend) MaintenanceManaged() bool { return true }

// call runs one storage operation on the backend's queue.
func (b *CloudBackend) call(ctx context.Context, method string, args, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	return b.request(ctx, http.MethodPost, b.queueURL+method, args, out)
}

// request sends args (no body when nil) to the data plane and decodes the
// response envelope's result into out (unless nil).
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
	// Reading to EOF lets the connection be reused; Unmarshal is also cheaper than a Decoder.
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

// WaitForResult reconnects only after a bounded server-side read timeout.
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
