// Code generated from the RunnerQ storage contract; DO NOT EDIT.
package backend

import (
	"context"
	"encoding/json"
	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
	"time"
)
import "github.com/alob-mtc/runnerq-cloud-storage-go/protocol"

func (b *CloudBackend) GetResult(ctx context.Context, activityID uuid.UUID) (*storage.ActivityResult, error) {
	var out *storage.ActivityResult
	err := b.call(ctx, "GetResult", protocol.GetResultArgs{ActivityID: activityID}, &out)
	return out, err
}
func (b *CloudBackend) Enqueue(ctx context.Context, activity storage.QueuedActivity) error {
	return b.call(ctx, "Enqueue", protocol.EnqueueArgs{Activity: activity}, nil)
}
func (b *CloudBackend) Dequeue(ctx context.Context, workerID string, timeout time.Duration, activityTypes []string) (*storage.QueuedActivity, error) {
	var out *storage.QueuedActivity
	err := b.call(ctx, "Dequeue", protocol.DequeueArgs{WorkerID: workerID, Timeout: timeout, ActivityTypes: activityTypes}, &out)
	return out, err
}
func (b *CloudBackend) AckSuccess(ctx context.Context, activityID uuid.UUID, result json.RawMessage, workerID string) error {
	return b.call(ctx, "AckSuccess", protocol.AckSuccessArgs{ActivityID: activityID, Result: result, WorkerID: workerID}, nil)
}
func (b *CloudBackend) AckFailure(ctx context.Context, activityID uuid.UUID, failure storage.FailureKind, workerID string) (bool, error) {
	var out bool
	err := b.call(ctx, "AckFailure", protocol.AckFailureArgs{ActivityID: activityID, Failure: failure, WorkerID: workerID}, &out)
	return out, err
}
func (b *CloudBackend) ProcessScheduled(ctx context.Context) (uint64, error) {
	var out uint64
	err := b.call(ctx, "ProcessScheduled", protocol.ProcessScheduledArgs{}, &out)
	return out, err
}
func (b *CloudBackend) RequeueExpired(ctx context.Context, batchSize int) (uint64, error) {
	var out uint64
	err := b.call(ctx, "RequeueExpired", protocol.RequeueExpiredArgs{BatchSize: batchSize}, &out)
	return out, err
}
func (b *CloudBackend) Yield(ctx context.Context, activityID uuid.UUID, wakeAt time.Time, workerID string, kind string, step string) error {
	return b.call(ctx, "Yield", protocol.YieldArgs{ActivityID: activityID, WakeAt: wakeAt, WorkerID: workerID, Kind: kind, Step: step}, nil)
}
func (b *CloudBackend) ExtendLease(ctx context.Context, activityID uuid.UUID, extendBy time.Duration) (bool, error) {
	var out bool
	err := b.call(ctx, "ExtendLease", protocol.ExtendLeaseArgs{ActivityID: activityID, ExtendBy: extendBy}, &out)
	return out, err
}
func (b *CloudBackend) StoreResult(ctx context.Context, activityID uuid.UUID, ownerActivityID uuid.UUID, result storage.ActivityResult, step string) error {
	return b.call(ctx, "StoreResult", protocol.StoreResultArgs{ActivityID: activityID, OwnerActivityID: ownerActivityID, Result: result, Step: step}, nil)
}
func (b *CloudBackend) WakeWaiting(ctx context.Context, activityID uuid.UUID) (bool, error) {
	var out bool
	err := b.call(ctx, "WakeWaiting", protocol.WakeWaitingArgs{ActivityID: activityID}, &out)
	return out, err
}
func (b *CloudBackend) SignalActivity(ctx context.Context, activityID uuid.UUID, signalID uuid.UUID, name string, payload json.RawMessage) error {
	return b.call(ctx, "SignalActivity", protocol.SignalActivityArgs{ActivityID: activityID, SignalID: signalID, Name: name, Payload: payload}, nil)
}
func (b *CloudBackend) LookupIdempotencyActivityID(ctx context.Context, idempotencyKey string) (uuid.UUID, error) {
	var out uuid.UUID
	err := b.call(ctx, "LookupIdempotencyActivityID", protocol.LookupIdempotencyActivityIDArgs{IdempotencyKey: idempotencyKey}, &out)
	return out, err
}
func (b *CloudBackend) CleanupExpired(ctx context.Context, policy storage.RetentionPolicy, batchSize int) (uint64, error) {
	var out uint64
	err := b.call(ctx, "CleanupExpired", protocol.CleanupExpiredArgs{Policy: policy, BatchSize: batchSize}, &out)
	return out, err
}
func (b *CloudBackend) EnqueueIdempotent(ctx context.Context, activity *storage.QueuedActivity) (*storage.IdempotencyResult, error) {
	var out *storage.IdempotencyResult
	err := b.call(ctx, "EnqueueIdempotent", protocol.EnqueueIdempotentArgs{Activity: activity}, &out)
	return out, err
}
func (b *CloudBackend) RecordSpawnLinked(ctx context.Context, childID uuid.UUID, parentID uuid.UUID) error {
	return b.call(ctx, "RecordSpawnLinked", protocol.RecordSpawnLinkedArgs{ChildID: childID, ParentID: parentID}, nil)
}
func (b *CloudBackend) GetActivity(ctx context.Context, activityID uuid.UUID) (*storage.ActivitySnapshot, error) {
	var out *storage.ActivitySnapshot
	err := b.call(ctx, "GetActivity", protocol.GetActivityArgs{ActivityID: activityID}, &out)
	return out, err
}
func (b *CloudBackend) GetActivityEvents(ctx context.Context, activityID uuid.UUID, limit int) ([]storage.ActivityEvent, error) {
	var out []storage.ActivityEvent
	err := b.call(ctx, "GetActivityEvents", protocol.GetActivityEventsArgs{ActivityID: activityID, Limit: limit}, &out)
	return out, err
}
func (b *CloudBackend) GetActivitySteps(ctx context.Context, ownerActivityID uuid.UUID) ([]storage.StepRecord, error) {
	var out []storage.StepRecord
	err := b.call(ctx, "GetActivitySteps", protocol.GetActivityStepsArgs{OwnerActivityID: ownerActivityID}, &out)
	return out, err
}
func (b *CloudBackend) GetChildren(ctx context.Context, parentID uuid.UUID, offset int, limit int) ([]storage.ActivitySnapshot, error) {
	var out []storage.ActivitySnapshot
	err := b.call(ctx, "GetChildren", protocol.GetChildrenArgs{ParentID: parentID, Offset: offset, Limit: limit}, &out)
	return out, err
}
func (b *CloudBackend) GetSubtree(ctx context.Context, rootID uuid.UUID) ([]storage.ActivitySnapshot, error) {
	var out []storage.ActivitySnapshot
	err := b.call(ctx, "GetSubtree", protocol.GetSubtreeArgs{RootID: rootID}, &out)
	return out, err
}
func (b *CloudBackend) ListDeadLetter(ctx context.Context, offset int, limit int) ([]storage.DeadLetterRecord, error) {
	var out []storage.DeadLetterRecord
	err := b.call(ctx, "ListDeadLetter", protocol.ListDeadLetterArgs{Offset: offset, Limit: limit}, &out)
	return out, err
}
func (b *CloudBackend) DequeueBatch(ctx context.Context, workerIDPrefix string, limit int, timeout time.Duration, activityTypes []string) ([]storage.DequeuedActivity, error) {
	var out []storage.DequeuedActivity
	err := b.call(ctx, "DequeueBatch", protocol.DequeueBatchArgs{WorkerIDPrefix: workerIDPrefix, Limit: limit, Timeout: timeout, ActivityTypes: activityTypes}, &out)
	return out, err
}
func (b *CloudBackend) ExtendLeaseForWorker(ctx context.Context, activityID uuid.UUID, workerID string, extendBy time.Duration) (bool, error) {
	var out bool
	err := b.call(ctx, "ExtendLeaseForWorker", protocol.ExtendLeaseForWorkerArgs{ActivityID: activityID, WorkerID: workerID, ExtendBy: extendBy}, &out)
	return out, err
}
func (b *CloudBackend) StoreCheckpoint(ctx context.Context, resultID uuid.UUID, ownerID uuid.UUID, workerID string, result storage.ActivityResult, step string) error {
	return b.call(ctx, "StoreCheckpoint", protocol.StoreCheckpointArgs{ResultID: resultID, OwnerID: ownerID, WorkerID: workerID, Result: result, Step: step}, nil)
}
func (b *CloudBackend) EnqueueForWorker(ctx context.Context, a storage.QueuedActivity, ownerID uuid.UUID, workerID string) error {
	return b.call(ctx, "EnqueueForWorker", protocol.EnqueueForWorkerArgs{A: a, OwnerID: ownerID, WorkerID: workerID}, nil)
}
func (b *CloudBackend) EnqueueIdempotentForWorker(ctx context.Context, a *storage.QueuedActivity, ownerID uuid.UUID, workerID string) (*storage.IdempotencyResult, error) {
	var out *storage.IdempotencyResult
	err := b.call(ctx, "EnqueueIdempotentForWorker", protocol.EnqueueIdempotentForWorkerArgs{A: a, OwnerID: ownerID, WorkerID: workerID}, &out)
	return out, err
}
func (b *CloudBackend) RegisterDependency(ctx context.Context, waiterID uuid.UUID, resultID uuid.UUID, workerID string) error {
	return b.call(ctx, "RegisterDependency", protocol.RegisterDependencyArgs{WaiterID: waiterID, ResultID: resultID, WorkerID: workerID}, nil)
}
func (b *CloudBackend) YieldForResult(ctx context.Context, waiterID uuid.UUID, resultID uuid.UUID, producerID *uuid.UUID, wakeAt time.Time, workerID string, kind string, step string) error {
	return b.call(ctx, "YieldForResult", protocol.YieldForResultArgs{WaiterID: waiterID, ResultID: resultID, ProducerID: producerID, WakeAt: wakeAt, WorkerID: workerID, Kind: kind, Step: step}, nil)
}
