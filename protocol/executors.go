package protocol

import "time"

// ExecutorReport is a worker's heartbeat (PUT /v1/executors/{id}, with the
// store key) for the console's Fleet, in a conductor agent's shapes: its
// hello's sdk and executor, and its executor.report state. DELETE
// /v1/executors/{id} says goodbye on a clean stop.
type ExecutorReport struct {
	SDK      SDKInfo       `json:"sdk"`
	Executor ExecutorInfo  `json:"executor"`
	State    ExecutorState `json:"state"`
}

// SDKInfo names the worker's RunnerQ SDK.
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

// ExecutorState is what the executor is doing, and has done since it started.
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

// RunningActivity is an activity the executor is running.
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

// HeartbeatInterval is how often a worker reports; three missed reports
// and the data plane counts it gone.
const HeartbeatInterval = 10 * time.Second

// MinReportGap is the least time between the extra reports a worker sends
// when it changes.
const MinReportGap = 2 * time.Second
