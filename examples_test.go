package backend

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/runnerq/cloud-storage-go/protocol"
)

// The spec's examples were encoded by this adapter before its types were
// generated from the spec. Decoding and re-encoding each must give the same
// bytes, so the generated types keep the wire exactly as it was.
func TestSpecExamplesRoundTrip(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("spec", "protocol", "storage", "examples", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no examples (is the spec submodule checked out? git submodule update --init): %v", err)
	}
	backend := reflect.TypeOf(&CloudBackend{})
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var ex struct {
			Operation string          `json:"operation"`
			Args      json.RawMessage `json:"args"`
			Result    json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(raw, &ex); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		args := protocol.NewArgs(ex.Operation)
		if args == nil {
			t.Errorf("%s: no arguments type for %s", file, ex.Operation)
			continue
		}
		roundTrip(t, ex.Operation+" args", ex.Args, args)

		method, ok := backend.MethodByName(ex.Operation)
		if !ok {
			t.Errorf("CloudBackend lacks %s", ex.Operation)
			continue
		}
		if method.Type.NumOut() == 2 {
			roundTrip(t, ex.Operation+" result", ex.Result, reflect.New(method.Type.Out(0)).Interface())
		} else if string(ex.Result) != "null" {
			t.Errorf("%s returns nothing but its example has %s", ex.Operation, ex.Result)
		}
	}
}

func TestSpecExecutorReportRoundTrip(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("spec", "protocol", "storage", "executor_report.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, "executor report", bytes.TrimSpace(raw), new(protocol.ExecutorReport))
}

func roundTrip(t *testing.T, what string, want json.RawMessage, into any) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(want))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		t.Errorf("%s: decode: %v", what, err)
		return
	}
	got, err := json.Marshal(into)
	if err != nil {
		t.Errorf("%s: encode: %v", what, err)
		return
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s: re-encoded\n  %s\nexample\n  %s", what, got, strings.TrimSpace(string(want)))
	}
}
