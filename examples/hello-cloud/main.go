// Hello Cloud runs a checkpointed workflow using RunnerQ's cloud storage adapter.
// See README.md in this directory for service setup and queue provisioning.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alob-mtc/runnerq-go"
	"github.com/google/uuid"
	backend "github.com/runnerq/runnerq-cloud-storage-go"
)

const activityType = "cloud_hello"

type Profile struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type HelloWorkflow struct {
	runnerq.DefaultDeadLetterHandler
}

func (*HelloWorkflow) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var input struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(payload, &input); err != nil || strings.TrimSpace(input.Name) == "" {
		return nil, runnerq.NewNonRetryError("a name is required")
	}

	// This is demo data, not a call to an external account service. A completed
	// checkpoint is retrieved from the data plane when the workflow replays.
	profile, err := ctx.RunStep("create-profile", func(context.Context) (Profile, error) {
		fmt.Println("step: create-profile")
		return Profile{ID: uuid.NewString(), Name: input.Name}, nil
	})
	if err != nil {
		return nil, err
	}
	greeting, err := ctx.RunStep("prepare-greeting", func(context.Context) (string, error) {
		fmt.Println("step: prepare-greeting")
		return fmt.Sprintf("Hello, %s! Your workflow used RunnerQ Cloud storage.", profile.Name), nil
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Profile  Profile `json:"profile"`
		Greeting string  `json:"greeting"`
	}{profile, greeting})
}

func main() {
	name := flag.String("name", "Ada", "name to greet")
	requestID := flag.String("request-id", "", "stable request ID; repeat it to await the same workflow (default: new UUID)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, strings.TrimSpace(*name), *requestID); err != nil && !errors.Is(err, context.Canceled) {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, name, requestID string) error {
	if name == "" {
		return errors.New("-name must not be empty")
	}
	apiKey := os.Getenv("RUNNERQ_STORE_KEY")
	if apiKey == "" {
		return errors.New("set RUNNERQ_STORE_KEY to a provisioned queue key; see examples/hello-cloud/README.md")
	}
	endpoint := envOr("RUNNERQ_DATA_ENDPOINT", "http://localhost:8081")
	queue := envOr("RUNNERQ_DATA_QUEUE", "hello_cloud")
	cloud, err := backend.NewCloudBackend(apiKey, backend.WithEndpoint(endpoint), backend.WithQueue(queue))
	if err != nil {
		return fmt.Errorf("configure cloud backend: %w", err)
	}
	// The worker executes handlers here; all durable state lives in the data
	// plane. Configure retention on the service rather than on this builder.
	engine, err := runnerq.Builder().Backend(cloud).QueueName(queue).MaxWorkers(2).
		ShutdownGrace(5 * time.Second).Build()
	if err != nil {
		return err
	}
	engine.RegisterActivityWithName(activityType, &HelloWorkflow{})
	if requestID == "" {
		requestID = uuid.NewString()
	}
	fmt.Printf("request: %s\n", requestID)
	payload, err := json.Marshal(map[string]string{"name": name})
	if err != nil {
		return err
	}
	future, err := engine.GetActivityExecutor().ActivityNamed(activityType).
		Payload(payload).IdempotencyKeyOption(requestID, runnerq.ReturnExisting).Execute(ctx)
	if err != nil {
		return fmt.Errorf("enqueue workflow: %w", err)
	}
	fmt.Printf("workflow: %s\n", future.ActivityID())

	workerCtx, cancelWorker := context.WithCancel(ctx)
	engineDone := make(chan struct{})
	var engineErr error
	go func() {
		engineErr = engine.Start(workerCtx)
		close(engineDone)
	}()
	defer func() {
		// Cancellation works even if an already-completed future resolves before
		// the engine goroutine starts. Start drains in-flight work before returning.
		cancelWorker()
		<-engineDone
	}()
	waitCtx, cancelWait := context.WithTimeout(ctx, time.Minute)
	defer cancelWait()
	type outcome struct {
		result json.RawMessage
		err    error
	}
	completed := make(chan outcome, 1)
	go func() {
		result, err := future.GetResult(waitCtx)
		completed <- outcome{result, err}
	}()
	select {
	case <-engineDone:
		if engineErr != nil {
			return fmt.Errorf("worker: %w", engineErr)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("worker stopped before the workflow completed")
	case out := <-completed:
		if out.err != nil {
			return fmt.Errorf("await workflow (reuse -request-id %s to reconnect): %w", requestID, out.err)
		}
		fmt.Printf("result: %s\n", out.result)
		return nil
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
