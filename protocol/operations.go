// Code generated from the RunnerQ storage contract; DO NOT EDIT.
package protocol

import (
	"encoding/json"
	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
	"time"
)

type GetResultArgs struct {
	ActivityID uuid.UUID `json:"activityID"`
}
type EnqueueArgs struct {
	Activity storage.QueuedActivity `json:"activity"`
}
type DequeueArgs struct {
	WorkerID      string        `json:"workerID"`
	Timeout       time.Duration `json:"timeout"`
	ActivityTypes []string      `json:"activityTypes"`
}
type AckSuccessArgs struct {
	ActivityID uuid.UUID       `json:"activityID"`
	Result     json.RawMessage `json:"result"`
	WorkerID   string          `json:"workerID"`
}
type AckFailureArgs struct {
	ActivityID uuid.UUID           `json:"activityID"`
	Failure    storage.FailureKind `json:"failure"`
	WorkerID   string              `json:"workerID"`
}
type ProcessScheduledArgs struct{}
type RequeueExpiredArgs struct {
	BatchSize int `json:"batchSize"`
}
type YieldArgs struct {
	ActivityID uuid.UUID `json:"activityID"`
	WakeAt     time.Time `json:"wakeAt"`
	WorkerID   string    `json:"workerID"`
	Kind       string    `json:"kind"`
	Step       string    `json:"step"`
}
type ExtendLeaseArgs struct {
	ActivityID uuid.UUID     `json:"activityID"`
	ExtendBy   time.Duration `json:"extendBy"`
}
type StoreResultArgs struct {
	ActivityID      uuid.UUID              `json:"activityID"`
	OwnerActivityID uuid.UUID              `json:"ownerActivityID"`
	Result          storage.ActivityResult `json:"result"`
	Step            string                 `json:"step"`
}
type WakeWaitingArgs struct {
	ActivityID uuid.UUID `json:"activityID"`
}
type SignalActivityArgs struct {
	ActivityID uuid.UUID       `json:"activityID"`
	SignalID   uuid.UUID       `json:"signalID"`
	Name       string          `json:"name"`
	Payload    json.RawMessage `json:"payload"`
}
type LookupIdempotencyActivityIDArgs struct {
	IdempotencyKey string `json:"idempotencyKey"`
}
type CleanupExpiredArgs struct {
	Policy    storage.RetentionPolicy `json:"policy"`
	BatchSize int                     `json:"batchSize"`
}
type EnqueueIdempotentArgs struct {
	Activity *storage.QueuedActivity `json:"activity"`
}
type RecordSpawnLinkedArgs struct {
	ChildID  uuid.UUID `json:"childID"`
	ParentID uuid.UUID `json:"parentID"`
}
type GetActivityArgs struct {
	ActivityID uuid.UUID `json:"activityID"`
}
type GetActivityEventsArgs struct {
	ActivityID uuid.UUID `json:"activityID"`
	Limit      int       `json:"limit"`
}
type GetActivityStepsArgs struct {
	OwnerActivityID uuid.UUID `json:"ownerActivityID"`
}
type GetChildrenArgs struct {
	ParentID uuid.UUID `json:"parentID"`
	Offset   int       `json:"offset"`
	Limit    int       `json:"limit"`
}
type GetSubtreeArgs struct {
	RootID uuid.UUID `json:"rootID"`
}
type ListDeadLetterArgs struct {
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}
type DequeueBatchArgs struct {
	WorkerIDPrefix string        `json:"workerIDPrefix"`
	Limit          int           `json:"limit"`
	Timeout        time.Duration `json:"timeout"`
	ActivityTypes  []string      `json:"activityTypes"`
}
type ExtendLeaseForWorkerArgs struct {
	ActivityID uuid.UUID     `json:"activityID"`
	WorkerID   string        `json:"workerID"`
	ExtendBy   time.Duration `json:"extendBy"`
}
type StoreCheckpointArgs struct {
	ResultID uuid.UUID              `json:"resultID"`
	OwnerID  uuid.UUID              `json:"ownerID"`
	WorkerID string                 `json:"workerID"`
	Result   storage.ActivityResult `json:"result"`
	Step     string                 `json:"step"`
}
type EnqueueForWorkerArgs struct {
	A        storage.QueuedActivity `json:"a"`
	OwnerID  uuid.UUID              `json:"ownerID"`
	WorkerID string                 `json:"workerID"`
}
type EnqueueIdempotentForWorkerArgs struct {
	A        *storage.QueuedActivity `json:"a"`
	OwnerID  uuid.UUID               `json:"ownerID"`
	WorkerID string                  `json:"workerID"`
}
type RegisterDependencyArgs struct {
	WaiterID uuid.UUID `json:"waiterID"`
	ResultID uuid.UUID `json:"resultID"`
	WorkerID string    `json:"workerID"`
}
type YieldForResultArgs struct {
	WaiterID   uuid.UUID  `json:"waiterID"`
	ResultID   uuid.UUID  `json:"resultID"`
	ProducerID *uuid.UUID `json:"producerID"`
	WakeAt     time.Time  `json:"wakeAt"`
	WorkerID   string     `json:"workerID"`
	Kind       string     `json:"kind"`
	Step       string     `json:"step"`
}
