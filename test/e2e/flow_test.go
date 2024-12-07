package e2e_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Pigotz/gopherate/internal/channels"
	"github.com/Pigotz/gopherate/pkg/broker"
	"github.com/Pigotz/gopherate/pkg/consumer"
	"github.com/Pigotz/gopherate/pkg/producer"
)

// Specific example

type ComputeFibonacciTask struct {
	steps int
}

func (w *ComputeFibonacciTask) Function() string {
	return "fibonacci"
}

func (w *ComputeFibonacciTask) Args() []string {
	return []string{strconv.Itoa(w.steps)}
}

func PrintErrors(ctx context.Context, t *testing.T, errors chan error) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case err := <-errors:
			t.Logf("Error: %v", err)
		}
	}
}

func PrintLogs(ctx context.Context, t *testing.T, logs chan string) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case log := <-logs:
			t.Logf("Log: %v", log)
		}
	}
}

func fibonacciHandler(args []string) ([]string, []error) {
	if len(args) != 1 {
		return nil, []error{errors.New("expected 1 argument")}
	}

	steps, err := strconv.Atoi(args[0])

	if err != nil {
		return nil, []error{err}
	}

	a, b := 0, 1

	for i := 0; i < steps; i++ {
		a, b = b, a+b
	}

	return []string{strconv.Itoa(a)}, nil
}

func TestFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var waitGroup sync.WaitGroup

	errors := make(chan error, 100)
	defer close(errors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		PrintErrors(ctx, t, errors)
	}()

	logs := make(chan string, 100)
	defer close(logs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		PrintLogs(ctx, t, logs)
	}()

	// section broker
	broker, err := broker.NewBroker("tcp://*:5555", nil)

	if err != nil {
		t.Errorf("Failed to create broker, error: %v", err)
	}

	defer broker.Close()

	err = broker.Bind()

	if err != nil {
		t.Errorf("Failed to bind broker, error: %v", err)
	}

	brokerErrors := make(chan error, 100)
	defer close(brokerErrors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[BROKER]", brokerErrors, errors)
	}()

	brokerLogs := make(chan string, 100)
	defer close(brokerLogs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[BROKER]", brokerLogs, logs)
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		broker.Run(ctx, brokerErrors, brokerLogs)
	}()

	// section producer

	producer, err := producer.NewProducer("producer1", "tcp://localhost:5555", nil)

	if err != nil {
		t.Errorf("Failed to create producer, error: %v", err)
	}

	defer producer.Close()

	err = producer.Connect()

	if err != nil {
		t.Errorf("Failed to connect to producer, error: %v", err)
	}

	producerErrors := make(chan error, 100)
	defer close(producerErrors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[PRODUCER]", producerErrors, errors)
	}()

	producerLogs := make(chan string, 100)
	defer close(producerLogs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[PRODUCER]", producerLogs, logs)
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		producer.Run(ctx, producerErrors, producerLogs)
	}()

	// section consumers

	consumer1, err := consumer.NewConsumer("consumer1", "tcp://localhost:5555", nil, consumer.Handlers{
		"fibonacci": fibonacciHandler,
	})

	if err != nil {
		t.Errorf("Failed to create consumer1, error: %v", err)
	}

	defer consumer1.Close()

	err = consumer1.Connect()

	if err != nil {
		t.Errorf("Failed to connect to consumer1, error: %v", err)
	}

	consumer1Errors := make(chan error, 100)
	defer close(consumer1Errors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[CONSUMER 1]", consumer1Errors, errors)
	}()

	consumer1Logs := make(chan string, 100)
	defer close(consumer1Logs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[CONSUMER 1]", consumer1Logs, logs)
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		consumer1.Run(ctx, consumer1Errors, consumer1Logs)
	}()

	consumer2, err := consumer.NewConsumer("consumer2", "tcp://localhost:5555", nil, consumer.Handlers{
		"fibonacci": fibonacciHandler,
	})

	if err != nil {
		t.Errorf("Failed to create consumer2, error: %v", err)
	}

	defer consumer2.Close()

	err = consumer2.Connect()

	if err != nil {
		t.Errorf("Failed to connect to consumer2, error: %v", err)
	}

	consumer2Errors := make(chan error, 100)
	defer close(consumer2Errors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[CONSUMER 2]", consumer2Errors, errors)
	}()

	consumer2Logs := make(chan string, 100)
	defer close(consumer2Logs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[CONSUMER 2]", consumer2Logs, logs)
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		consumer2.Run(ctx, consumer2Errors, consumer2Logs)
	}()

	// section task

	computeFibonacciTask := &ComputeFibonacciTask{
		steps: 100,
	}

	results, err := producer.Process(ctx, computeFibonacciTask, 5*time.Second)

	if err != nil {
		t.Errorf("Task failed: %v", err)
	}

	t.Logf("Results: %v", results)

	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %v", len(results))
	}

	if results[0] != "3736710778780434371" {
		t.Errorf("Expected 3736710778780434371, got %v", results[0])
	}

	// section cleanup

	cancel()

	t.Log("Waiting for all goroutines to finish")

	waitGroup.Wait()
}
