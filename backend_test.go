package backend

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
)

func TestEndpointValidation(t *testing.T) {
	for _, endpoint := range []string{"", "http://example.com", "ftp://localhost", "https://user:secret@example.com", "https://example.com?key=value"} {
		if _, err := NewCloudBackend("key", WithEndpoint(endpoint)); err == nil {
			t.Fatalf("accepted invalid endpoint %s", endpoint)
		}
	}
}
func TestRedirectIsNotFollowed(t *testing.T) {
	var followed atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Store(true) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	b, err := NewCloudBackend("secret", WithEndpoint(source.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.GetResult(t.Context(), uuid.New()); err == nil {
		t.Fatal("redirect succeeded")
	}
	if followed.Load() {
		t.Fatal("credential sent through redirect")
	}
}
func TestTypedErrorsAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if strings.Contains(r.URL.Path, "GetResult") {
			<-r.Context().Done()
			return
		}
		w.Header().Set("RunnerQ-Storage-Version", "1")
		w.WriteHeader(409)
		w.Write([]byte(`{"error":{"code":"claim_lost","message":"lost"}}`))
	}))
	defer server.Close()
	b, err := NewCloudBackend("secret", WithEndpoint(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.ProcessScheduled(t.Context())
	if se, ok := storage.IsStorageError(err); !ok || se.Kind != storage.ErrClaimLost {
		t.Fatalf("lost error classification: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := b.GetResult(ctx, uuid.New()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
