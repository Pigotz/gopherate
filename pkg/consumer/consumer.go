package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Pigotz/gopherate/internal/actor"
	"github.com/Pigotz/gopherate/internal/channels"
	"github.com/Pigotz/gopherate/internal/fsm"
	"github.com/Pigotz/gopherate/internal/messages"
)

// TODO: Add support for errors and logs channels
type Handler func([]string) ([]string, []error)
type Handlers map[string]Handler

type Option func(*Consumer)

func WithRunningTimeout(timeout time.Duration) Option {
	return func(w *Consumer) {
		w.runningTimeout = timeout
	}
}

func WithConcurrency(concurrency int) Option {
	return func(w *Consumer) {
		w.concurrency = concurrency
	}
}

type Consumer struct {
	dealer   *actor.DealerActor
	fsm      *fsm.FSM
	handlers Handlers
	// Options
	runningTimeout time.Duration
	concurrency    int
}

type StartState struct{}
type SendAnnounceState struct{}
type WaitAnnounceAckState struct{}
type RunningState struct{}

func NewConsumer(identity string, connectAddress string, actorOpts []actor.Option, handlers Handlers, opts ...Option) (*Consumer, error) {
	dealer, err := actor.NewDealerActor(identity, connectAddress, actorOpts...)

	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		runningTimeout: 60 * time.Second,
		concurrency:    1,
	}

	for _, opt := range opts {
		opt(consumer)
	}

	consumer.dealer = dealer
	consumer.handlers = handlers

	fsm := fsm.NewFSM(&StartState{}, fsm.Callbacks{
		"*consumer.StartState": func(ctx context.Context, state fsm.State, errors chan<- error, logs chan<- string) (fsm.State, error) {
			logs <- "Starting"

			return &SendAnnounceState{}, nil
		},
		"*consumer.SendAnnounceState": func(ctx context.Context, state fsm.State, errors chan<- error, logs chan<- string) (fsm.State, error) {
			logs <- "Sending announce message"

			if message, ok := messages.ConvertToGenericMessage(messages.ConsumerAnnounceMessage{}); ok {
				consumer.dealer.Send(actor.NewMessage(message.Parts...))
			} else {
				return &StartState{}, fmt.Errorf("failed to convert ConsumerAnnounceMessage to GenericMessage")
			}

			return &WaitAnnounceAckState{}, nil
		},
		"*consumer.WaitAnnounceAckState": func(ctx context.Context, state fsm.State, errors chan<- error, logs chan<- string) (fsm.State, error) {
			logs <- "Waiting for announce ack message"

			message, err := consumer.dealer.Receive(ctx)

			if err != nil {
				errors <- err

				return nil, err
			}

			logs <- fmt.Sprintf("Received message: %v", message)

			switch v := messages.InterpretMessage(messages.NewMessage(message.Parts...)).(type) {
			case messages.ConsumerAnnounceAckMessage:
				return &RunningState{}, nil
			default:
				return &StartState{}, fmt.Errorf("unknown message: %T %v", v, v)
			}
		},
		"*consumer.RunningState": func(ctx context.Context, state fsm.State, errors chan<- error, logs chan<- string) (fsm.State, error) {
			logs <- "Running"

			ctx, cancel := context.WithTimeout(ctx, consumer.runningTimeout)
			defer cancel()

			var waitGroup sync.WaitGroup

			for i := 0; i < consumer.concurrency; i++ {
				logs <- fmt.Sprintf("Starting sub-consumer: %d", i)

				consumerLogs := make(chan string, 100)
				defer close(consumerLogs)

				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()

					channels.PrefixStringChannel(ctx, fmt.Sprintf("[CONSUMER-%d]", i), consumerLogs, logs)
				}()

				consumerErrors := make(chan error, 100)
				defer close(consumerErrors)

				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()

					channels.WrapErrorChannel(ctx, fmt.Sprintf("[CONSUMER-%d]", i), consumerErrors, errors)
				}()

				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()

					consumer.processWork(ctx, consumerErrors, consumerLogs)
				}()
			}

			waitGroup.Wait()

			return &StartState{}, nil
		},
	})

	consumer.fsm = fsm

	return consumer, nil
}

func (w *Consumer) Connect() error {
	return w.dealer.Connect()
}

func (w *Consumer) Close() {
	w.dealer.Close()
}

func (w *Consumer) Run(ctx context.Context, errors chan<- error, logs chan<- string) {
	var waitGroup sync.WaitGroup

	dealerLogs := make(chan string, 100)
	defer close(dealerLogs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[DEALER]", dealerLogs, logs)
	}()

	dealerErrors := make(chan error, 100)
	defer close(dealerErrors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[DEALER]", dealerErrors, errors)
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		w.dealer.Run(ctx, dealerErrors, dealerLogs)
	}()

	fsmLogs := make(chan string, 100)
	defer close(fsmLogs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[FSM]", fsmLogs, logs)
	}()

	fsmErrors := make(chan error, 100)
	defer close(fsmErrors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[FSM]", fsmErrors, errors)
	}()

	w.fsm.Run(ctx, fsmErrors, fsmLogs)

	waitGroup.Wait()
}

func (w *Consumer) processWork(ctx context.Context, errors chan<- error, logs chan<- string) {
	for ctx.Err() == nil {
		dealerMessage, err := w.dealer.Receive(ctx)

		if err != nil {
			errors <- err

			return
		}

		logs <- fmt.Sprintf("Received message: %v", dealerMessage)

		switch v := messages.InterpretMessage(messages.NewMessage(dealerMessage.Parts...)).(type) {
		case messages.ConsumerWorkMessage:
			handler, ok := w.handlers[v.Function]

			if !ok {
				w.dealer.Send(actor.NewMessage(messages.ConsumerWorkErrorsMessage{
					ID:     v.ID,
					Errors: []error{fmt.Errorf("failed to find handler for task function: %s", v.Function)},
				}.ToGenericMessage().Parts...))

				continue
			}

			results, errors := handler(v.Args)

			if len(errors) > 0 {
				w.dealer.Send(actor.NewMessage(messages.ConsumerWorkErrorsMessage{
					ID:     v.ID,
					Errors: errors,
				}.ToGenericMessage().Parts...))

				continue
			}

			w.dealer.Send(actor.NewMessage(messages.ConsumerWorkResultsMessage{
				ID:      v.ID,
				Results: results,
			}.ToGenericMessage().Parts...))
		default:
			errors <- fmt.Errorf("unknown message: %v", dealerMessage)
		}
	}
}
