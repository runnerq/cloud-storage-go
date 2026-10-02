// Package protocol is the v1 storage wire format shared by the adapter and
// the data plane. It has no server imports.
package protocol

import (
	"encoding/json"
	"slices"

	"github.com/alob-mtc/runnerq-go/storage"
)

type Response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// Code is k's wire code ("internal" if unknown).
func Code(k storage.StorageErrorKind) string {
	if k >= 0 && int(k) < len(ErrorCodes) {
		return ErrorCodes[k]
	}
	return "internal"
}

// StorageError is e as a storage error; an unknown code is a configuration error.
func (e *Error) StorageError() error {
	if k := slices.Index(ErrorCodes[:], e.Code); k >= 0 {
		return &storage.StorageError{Kind: storage.StorageErrorKind(k), Message: e.Message, Field: e.Field}
	}
	return storage.NewConfigurationError(e.Message)
}
