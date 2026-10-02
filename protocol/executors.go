package protocol

import "time"

// HeartbeatInterval is how often a worker reports; three missed reports
// and the data plane counts it gone.
const HeartbeatInterval = 10 * time.Second

// MinReportGap is the least time between the extra reports a worker sends
// when it changes.
const MinReportGap = 2 * time.Second
