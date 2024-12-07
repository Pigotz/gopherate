package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/Pigotz/gopherate/internal/actor"
	"github.com/Pigotz/gopherate/internal/channels"
	"github.com/Pigotz/gopherate/internal/messages"

	"math/rand"
)

type Option func(*Broker)

type Broker struct {
	router       *actor.RouterActor
	producers    map[string]struct{}
	consumers    []string
	pendingTasks map[string]string
}

func NewBroker(bindAddress string, actorOpts []actor.Option, opts ...Option) (*Broker, error) {
	router, err := actor.NewRouterActor(bindAddress, actorOpts...)

	if err != nil {
		return nil, err
	}

	broker := &Broker{}

	for _, opt := range opts {
		opt(broker)
	}

	broker.router = router
	broker.producers = make(map[string]struct{})
	broker.consumers = make([]string, 0)
	broker.pendingTasks = make(map[string]string)

	return broker, nil
}

func (b *Broker) Bind() error {
	return b.router.Bind()
}

func (b *Broker) Close() {
	b.router.Close()
}

func (b *Broker) Run(ctx context.Context, errors chan<- error, logs chan<- string) {
	var waitGroup sync.WaitGroup

	routerLogs := make(chan string, 100)
	defer close(routerLogs)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.PrefixStringChannel(ctx, "[ROUTER]", routerLogs, logs)
	}()

	routerErrors := make(chan error, 100)
	defer close(routerErrors)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		channels.WrapErrorChannel(ctx, "[ROUTER]", routerErrors, errors)
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		b.router.Run(ctx, routerErrors, routerLogs)
	}()

	logs <- "Broker started"

	for ctx.Err() == nil {
		logs <- "Waiting for message"

		routerMessageWithIdentity, err := b.router.Receive(ctx)

		if err != nil {
			errors <- err

			continue
		}

		logs <- fmt.Sprintf("Received message: %v", routerMessageWithIdentity)

		switch v := messages.InterpretMessage(messages.NewMessage(routerMessageWithIdentity.Parts...)).(type) {
		case messages.ProducerAnnounceMessage:
			logs <- fmt.Sprintf("Received Producer Announce Message: %v", v)

			if genericMessage, ok := messages.ConvertToGenericMessage(messages.ProducerAnnounceAckMessage{}); ok {
				b.AddProducer(routerMessageWithIdentity.Identity)

				logs <- fmt.Sprintf("Added Producer: %s", routerMessageWithIdentity.Identity)

				logs <- fmt.Sprintf("Sending Producer Announce Ack Message to %s: %v", routerMessageWithIdentity.Identity, genericMessage)

				b.router.Send(actor.NewMessageWithIdentity(routerMessageWithIdentity.Identity, genericMessage.Parts...))
			} else {
				errors <- fmt.Errorf("failed to convert Producer Announce Ack Message to Generic Message")
			}
		case messages.ConsumerAnnounceMessage:
			logs <- fmt.Sprintf("Received Consumer Announce Message: %v", v)

			if genericMessage, ok := messages.ConvertToGenericMessage(messages.ConsumerAnnounceAckMessage{}); ok {
				b.AddConsumer(routerMessageWithIdentity.Identity)

				logs <- fmt.Sprintf("Added Consumer: %s", routerMessageWithIdentity.Identity)

				logs <- fmt.Sprintf("Sending Consumer Announce Ack Message to %s: %v", routerMessageWithIdentity.Identity, genericMessage)

				b.router.Send(actor.NewMessageWithIdentity(routerMessageWithIdentity.Identity, genericMessage.Parts...))

				logs <- fmt.Sprintf("Sent Consumer Announce Ack Message to %s: %v", routerMessageWithIdentity.Identity, genericMessage)
			} else {
				errors <- fmt.Errorf("failed to convert Consumer Announce Ack Message to Generic Message")
			}
		case messages.ProducerWorkMessage:
			logs <- fmt.Sprintf("Received Producer Work Message: %v", v)

			consumerIdentity, err := b.PickRandomConsumer()

			if err != nil {
				errors <- fmt.Errorf("failed to pick random consumer, error: %v", err)

				if genericMessage, ok := messages.ConvertToGenericMessage(messages.ProducerWaitMessage{
					// TODO: Make this configurable
					RetryAfter: 5,
				}); ok {
					b.router.Send(actor.NewMessageWithIdentity(routerMessageWithIdentity.Identity, genericMessage.Parts...))
				} else {
					errors <- fmt.Errorf("failed to convert Producer Wait Message to Generic Message")
				}
			}

			logs <- fmt.Sprintf("Picked Consumer: %s", consumerIdentity)

			b.pendingTasks[v.ID] = routerMessageWithIdentity.Identity

			if genericMessage, ok := messages.ConvertToGenericMessage(messages.ConsumerWorkMessage{
				ID:       v.ID,
				Function: v.Function,
				Args:     v.Args,
			}); ok {
				b.router.Send(actor.NewMessageWithIdentity(consumerIdentity, genericMessage.Parts...))
			} else {
				errors <- fmt.Errorf("failed to convert Producer Work Message to Generic Message")
			}
		case messages.ConsumerWorkResultsMessage:
			logs <- fmt.Sprintf("Received Consumer Work Results Message: %v", v)

			producerIdentity, ok := b.pendingTasks[v.ID]

			if !ok {
				errors <- fmt.Errorf("no pending task found for ID: %s", v.ID)
				continue
			}

			if genericMessage, ok := messages.ConvertToGenericMessage(messages.ProducerWorkResultMessage{
				ID:      v.ID,
				Results: v.Results,
			}); ok {
				delete(b.pendingTasks, v.ID)

				b.router.Send(actor.NewMessageWithIdentity(producerIdentity, genericMessage.Parts...))
			} else {
				errors <- fmt.Errorf("failed to convert Consumer Work Result Message to Generic Message")
			}
		case messages.ConsumerWorkErrorsMessage:
			logs <- fmt.Sprintf("Received Consumer Work Errors Message: %v", v)

			producerIdentity, ok := b.pendingTasks[v.ID]

			if !ok {
				errors <- fmt.Errorf("no pending task found for ID: %s", v.ID)
				continue
			}

			if genericMessage, ok := messages.ConvertToGenericMessage(messages.ProducerWorkErrorMessage{
				ID:     v.ID,
				Errors: v.Errors,
			}); ok {
				delete(b.pendingTasks, v.ID)

				b.router.Send(actor.NewMessageWithIdentity(producerIdentity, genericMessage.Parts...))
			} else {
				errors <- fmt.Errorf("failed to convert Consumer Work Error Message to Generic Message")
			}
		default:
			errors <- fmt.Errorf("unknown message type: %v %T %v", routerMessageWithIdentity, v, v)
		}
	}

	logs <- "Broker stopped"

	waitGroup.Wait()
}

func (b *Broker) AddProducer(producerIdentity string) {
	b.producers[producerIdentity] = struct{}{}
}

func (b *Broker) AddConsumer(consumerIdentity string) {
	b.consumers = append(b.consumers, consumerIdentity)
}

func (b *Broker) PickRandomConsumer() (string, error) {
	if len(b.consumers) == 0 {
		return "", fmt.Errorf("no consumers available")
	}

	if len(b.consumers) == 1 {
		return b.consumers[0], nil
	}

	return b.consumers[rand.Intn(len(b.consumers))], nil
}
