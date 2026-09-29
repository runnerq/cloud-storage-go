// Package protocol is the v1 storage wire format shared by the adapter and
// the data plane. It has no server imports.
package protocol

import (
	"encoding/json"

	"github.com/alob-mtc/runnerq-go/storage"
)

// Version is the RunnerQ-Storage-Version header's value.
const Version = "1"

// Response is every storage reply: a result or an error.
type Response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

// Error is a storage error on the wire.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

var kinds = map[storage.StorageErrorKind]string{
	storage.ErrUnavailable: "unavailable", storage.ErrConflict: "conflict",
	storage.ErrNotFound: "not_found", storage.ErrInternal: "internal",
	storage.ErrSerialization: "serialization", storage.ErrConfiguration: "configuration",
	storage.ErrTimeout: "timeout", storage.ErrDuplicateActivity: "duplicate_activity",
	storage.ErrIdempotencyConflict: "idempotency_conflict", storage.ErrClaimLost: "claim_lost",
	storage.ErrCheckpointConflict: "checkpoint_conflict", storage.ErrInvalidArgument: "invalid_argument",
	storage.ErrUnsupported: "unsupported",
}

// Code is k's wire code ("internal" if unknown).
func Code(k storage.StorageErrorKind) string {
	if s, ok := kinds[k]; ok {
		return s
	}
	return "internal"
}

// StorageError is e as a storage error; an unknown code is a configuration error.
func (e *Error) StorageError() error {
	for k, v := range kinds {
		if v == e.Code {
			return &storage.StorageError{Kind: k, Message: e.Message, Field: e.Field}
		}
	}
	return storage.NewConfigurationError(e.Message)
}
