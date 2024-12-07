package actor_test

import (
	"context"
	"sync"
	"testing"

	actor "github.com/Pigotz/gopherate/internal/actor"
	"github.com/Pigotz/gopherate/internal/channels"
)

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

func TestRouterDealerActors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
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

	t.Cleanup(cancel)

	router, err := actor.NewRouterActor("tcp://*:5555")

	if err != nil {
		t.Fatal(err)
	}

	defer router.Close()

	dealer, err := actor.NewDealerActor("dealer1", "tcp://localhost:5555")

	if err != nil {
		t.Fatal(err)
	}

	defer dealer.Close()

	err = router.Bind()

	if err != nil {
		t.Fatal(err)
	}

	err = dealer.Connect()

	if err != nil {
		t.Fatal(err)
	}

	routerErrors := make(chan error, 100)
	defer close(routerErrors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[ROUTER]", routerErrors, errors)
	}()

	routerLogs := make(chan string, 100)
	defer close(routerLogs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[ROUTER]", routerLogs, logs)
	}()

	go router.Run(ctx, routerErrors, routerLogs)

	dealerErrors := make(chan error, 100)
	defer close(dealerErrors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[DEALER]", dealerErrors, errors)
	}()

	dealerLogs := make(chan string, 100)
	defer close(dealerLogs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[DEALER]", dealerLogs, logs)
	}()

	go dealer.Run(ctx, dealerErrors, dealerLogs)

	sendMessage := actor.Message{
		Parts: []string{"PING"},
	}

	dealer.Send(sendMessage)

	if err != nil {
		t.Fatal(err)
	}

	receivedMessage, err := router.Receive(ctx)

	if err != nil {
		t.Fatal(err)
	}

	if receivedMessage.Identity != "dealer1" {
		t.Fatalf("Expected dealer1, got %s", receivedMessage.Identity)
	}

	if receivedMessage.Parts[0] != "PING" {
		t.Fatalf("Expected PING, got %s", receivedMessage.Parts[0])
	}

	cancel()

	t.Log("Waiting for all goroutines to finish")

	waitGroup.Wait()
}
