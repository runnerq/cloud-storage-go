package backend

// The storage protocol comes from runnerq-spec (the spec submodule).
//go:generate go run -C spec ./tools/gen -part storage -lang go-protocol -pkg protocol -out ../protocol/operations.go
//go:generate go run -C spec ./tools/gen -part storage -lang go-client -pkg backend -out ../operations.go
//go:generate go run -C spec ./tools/gen -part executor-report -lang go -pkg protocol -out ../protocol/executor_report.go
