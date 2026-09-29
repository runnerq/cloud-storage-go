package protocol

import "time"

// ExecutorReport is a worker's heartbeat to the data plane: which engine it
// is and what it is doing, so RunnerQ Cloud's console can show a hosted
// app's workers (Fleet). Sent with the store key to
// PUT /v1/executors/{id} every HeartbeatInterval; DELETE /v1/executors/{id}
// says goodbye on a clean stop.
type ExecutorReport struct {
	Queue          string            `json:"queue"`
	Hostname       string            `json:"hostname"`
	SDK            SDKInfo           `json:"sdk"`
	ActivityTypes  []string          `json:"activity_types"`
	MaxConcurrency int               `json:"max_concurrency"`
	StartedAt      time.Time         `json:"started_at"`
	Draining       bool              `json:"draining"`
	Running        []RunningActivity `json:"running"`
	Labels         map[string]string `json:"labels,omitempty"`
}

type SDKInfo struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Language string `json:"language"`
}

type RunningActivity struct {
	ActivityID string    `json:"activity_id"`
	Type       string    `json:"type"`
	Attempt    int       `json:"attempt"`
	StartedAt  time.Time `json:"started_at"`
}

// HeartbeatInterval is how often a worker reports. The data plane treats a
// worker as gone after three missed reports.
const HeartbeatInterval = 10 * time.Second
