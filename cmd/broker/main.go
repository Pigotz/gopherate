package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Pigotz/gopherate/internal/channels"
	"github.com/Pigotz/gopherate/pkg/broker"
)

func handleSigint(ctx context.Context, cancel context.CancelFunc) {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)

	select {
	case <-ctx.Done():
		return
	case <-sigint:
		fmt.Println("Received SIGINT, cancelling context")

		cancel()
	}
}

func PrintErrors(ctx context.Context, errors chan error) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case err := <-errors:
			fmt.Printf("[%s] [ERROR] %s\n", time.Now().UTC().String(), err)
		}
	}
}

func PrintLogs(ctx context.Context, logs chan string) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case log := <-logs:
			fmt.Printf("[%s] [LOG] %s\n", time.Now().UTC().String(), log)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go handleSigint(ctx, cancel)

	var waitGroup sync.WaitGroup

	errors := make(chan error, 100)
	defer close(errors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		PrintErrors(ctx, errors)
	}()

	logs := make(chan string, 100)
	defer close(logs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		PrintLogs(ctx, logs)
	}()

	// TODO: Expose configuration as CLI args
	broker, err := broker.NewBroker("tcp://*:5555", nil)

	if err != nil {
		panic(fmt.Sprintf("Failed to create broker, error: %v", err))
	}

	defer broker.Close()

	err = broker.Bind()

	if err != nil {
		panic(fmt.Sprintf("Failed to bind broker, error: %v", err))
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

	waitGroup.Wait()
}
