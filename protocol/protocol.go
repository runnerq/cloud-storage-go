// Package protocol defines the v1 Go storage transport. It has no server imports.
package protocol

import (
	"encoding/json"
	"github.com/alob-mtc/runnerq-go/storage"
)

const Version = "1"

type Response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

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

func Code(k storage.StorageErrorKind) string {
	if s, ok := kinds[k]; ok {
		return s
	}
	return "internal"
}

func (e *Error) StorageError() error {
	for k, v := range kinds {
		if v == e.Code {
			return &storage.StorageError{Kind: k, Message: e.Message, Field: e.Field}
		}
	}
	return storage.NewConfigurationError(e.Message)
}
