package protocol

import "time"

// ExecutorReport is a worker's heartbeat to the data plane, so RunnerQ
// Cloud's console can show a hosted app's workers (Fleet). It is what a
// conductor agent says about its executor, in the same shapes: its hello's
// sdk and executor, and its executor.report state (with the running list).
// Sent with the store key to PUT /v1/executors/{id} every
// HeartbeatInterval; DELETE /v1/executors/{id} says goodbye on a clean stop.
type ExecutorReport struct {
	SDK      SDKInfo       `json:"sdk"`
	Executor ExecutorInfo  `json:"executor"`
	State    ExecutorState `json:"state"`
}

type SDKInfo struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Language string `json:"language"`
}

// ExecutorInfo is who the executor is. Timestamps are RFC 3339, UTC.
type ExecutorInfo struct {
	ID             string            `json:"id"`
	Hostname       string            `json:"hostname,omitempty"`
	Queues         []string          `json:"queues,omitempty"`
	ActivityTypes  []string          `json:"activity_types,omitempty"`
	MaxConcurrency int               `json:"max_concurrency,omitempty"`
	StartedAt      string            `json:"started_at,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

// ExecutorState is what the executor is doing, and has done since it
// started.
type ExecutorState struct {
	ID                string            `json:"id"`
	UptimeMS          int64             `json:"uptime_ms"`
	MaxConcurrency    int               `json:"max_concurrency"`
	InFlight          int               `json:"in_flight"`
	Running           []RunningActivity `json:"running,omitempty"`
	ClaimLagMS        int64             `json:"claim_lag_ms"`
	HeartbeatFailures uint64            `json:"heartbeat_failures"`
	Draining          bool              `json:"draining"`
	Counters          *Counters         `json:"counters,omitempty"`
}

type RunningActivity struct {
	ActivityID string `json:"activity_id"`
	Type       string `json:"type"`
	Attempt    int    `json:"attempt"`
	StartedAt  string `json:"started_at"`
}

// Counters count activity outcomes since the executor started.
type Counters struct {
	Claimed      uint64 `json:"claimed"`
	Succeeded    uint64 `json:"succeeded"`
	Retried      uint64 `json:"retried"`
	Failed       uint64 `json:"failed"`
	TimedOut     uint64 `json:"timed_out"`
	DeadLettered uint64 `json:"dead_lettered"`
	ClaimsLost   uint64 `json:"claims_lost"`
}

// HeartbeatInterval is how often a worker reports. The data plane treats a
// worker as gone after three missed reports.
const HeartbeatInterval = 10 * time.Second

// MinReportGap spaces the extra reports a worker sends soon after it
// changes (an activity starting or finishing, or a drain beginning).
const MinReportGap = 2 * time.Second
