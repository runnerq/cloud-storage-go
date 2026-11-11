package backend

// The storage protocol comes from runnerq-spec (the spec submodule).
//go:generate go run -C spec ./tools/gen -part storage -lang go-protocol -storage-import github.com/runnerq/runnerq-go/storage -pkg protocol -out ../protocol/operations.go
//go:generate go run -C spec ./tools/gen -part storage -lang go-client -storage-import github.com/runnerq/runnerq-go/storage -pkg backend -out ../operations.go
//go:generate go run -C spec ./tools/gen -part executor-report -lang go -pkg protocol -out ../protocol/executor_report.go
