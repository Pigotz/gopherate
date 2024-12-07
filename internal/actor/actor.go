package actor

import (
	"context"
	"fmt"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

type Message struct {
	Parts []string
}

func NewMessage(parts ...string) Message {
	return Message{Parts: parts}
}

type MessageWithIdentity struct {
	Identity string
	Parts    []string
}

func NewMessageWithIdentity(identity string, parts ...string) MessageWithIdentity {
	return MessageWithIdentity{Identity: identity, Parts: parts}
}

type Actor struct {
	zmqContext     *zmq.Context
	zmqSocket      *zmq.Socket
	socketLock     sync.Mutex
	receiveTimeout time.Duration
	inboxSize      int
	inbox          chan Message
	sendTimeout    time.Duration
	outboxSize     int
	outbox         chan Message
}

type Option func(*Actor)

func WithReceiveTimeout(timeout time.Duration) Option {
	return func(a *Actor) {
		a.receiveTimeout = timeout
	}
}

func WithSendTimeout(timeout time.Duration) Option {
	return func(a *Actor) {
		a.sendTimeout = timeout
	}
}

func WithInboxSize(size int) Option {
	return func(a *Actor) {
		a.inboxSize = size
	}
}

func WithOutboxSize(size int) Option {
	return func(a *Actor) {
		a.outboxSize = size
	}
}

func NewActor(actorType zmq.Type, opts ...Option) (*Actor, error) {
	actor := &Actor{
		receiveTimeout: 5 * time.Second,
		inboxSize:      100,
		sendTimeout:    5 * time.Second,
		outboxSize:     100,
	}

	for _, opt := range opts {
		opt(actor)
	}

	zmqContext, err := zmq.NewContext()

	if err != nil {
		return nil, err
	}

	zmqSocket, err := zmqContext.NewSocket(actorType)

	if err != nil {
		return nil, err
	}

	if actor.receiveTimeout > 0 {
		zmqSocket.SetRcvtimeo(actor.receiveTimeout)
	}

	if actor.sendTimeout > 0 {
		zmqSocket.SetSndtimeo(actor.sendTimeout)
	}

	actor.zmqContext = zmqContext
	actor.zmqSocket = zmqSocket
	actor.inbox = make(chan Message, actor.inboxSize)
	actor.outbox = make(chan Message, actor.outboxSize)

	return actor, nil
}

func (a *Actor) Close() {
	close(a.inbox)
	close(a.outbox)

	a.zmqSocket.Close()
	a.zmqContext.Term()
}

func (a *Actor) Send(message Message) {
	a.outbox <- message
}

func (a *Actor) Receive(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case message, ok := <-a.inbox:
		if !ok {
			return nil, fmt.Errorf("inbox closed")
		}

		return &message, nil
	}
}

func (a *Actor) Run(ctx context.Context, errors chan<- error, logs chan<- string) {
	var waitGroup sync.WaitGroup
	defer waitGroup.Wait()

	// Send goroutine
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case message, ok := <-a.outbox:
				if !ok {
					return
				}

				var messageBytes int

				for _, part := range message.Parts {
					messageBytes += len(part)
				}

				a.socketLock.Lock()
				sentBytes, err := a.zmqSocket.SendMessageDontwait(message.Parts)
				a.socketLock.Unlock()

				if err != nil {
					if zmq.AsErrno(err).Error() == "resource temporarily unavailable" {
						continue
					}

					errors <- err

					continue
				}

				if sentBytes != messageBytes {
					errors <- fmt.Errorf("sent %d bytes out of %d", sentBytes, messageBytes)
				}
			}
		}
	}()

	// Receive goroutine
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		for ctx.Err() == nil {
			a.socketLock.Lock()
			parts, err := a.zmqSocket.RecvMessage(zmq.DONTWAIT)
			a.socketLock.Unlock()

			if len(parts) > 0 {
				logs <- fmt.Sprintf("Received message: %v", parts)
			}

			if err != nil {
				if zmq.AsErrno(err).Error() == "resource temporarily unavailable" {
					continue
				}

				errors <- err

				continue
			}

			a.inbox <- Message{Parts: parts}
		}
	}()
}

type RouterActor struct {
	actor       *Actor
	bindAddress string
}

func NewRouterActor(bindAddress string, opts ...Option) (*RouterActor, error) {
	actor, err := NewActor(zmq.ROUTER, opts...)

	if err != nil {
		return nil, err
	}

	return &RouterActor{
		actor:       actor,
		bindAddress: bindAddress,
	}, nil
}

func (a *RouterActor) Bind() error {
	err := a.actor.zmqSocket.Bind(a.bindAddress)

	if err != nil {
		return err
	}

	return nil
}

func (a *RouterActor) Close() {
	a.actor.Close()
}

func (a *RouterActor) Send(message MessageWithIdentity) {
	a.actor.Send(Message{
		Parts: append([]string{message.Identity}, message.Parts...),
	})
}

func (a *RouterActor) Receive(ctx context.Context) (*MessageWithIdentity, error) {
	message, err := a.actor.Receive(ctx)

	if err != nil {
		return nil, err
	}

	return &MessageWithIdentity{
		Identity: message.Parts[0],
		Parts:    message.Parts[1:],
	}, nil
}

func (a *RouterActor) Run(ctx context.Context, errors chan<- error, logs chan<- string) {
	a.actor.Run(ctx, errors, logs)
}

type DealerActor struct {
	actor          *Actor
	identity       string
	connectAddress string
}

func NewDealerActor(identity string, connectAddress string, opts ...Option) (*DealerActor, error) {
	actor, err := NewActor(zmq.DEALER, opts...)

	if err != nil {
		return nil, err
	}

	actor.zmqSocket.SetIdentity(identity)

	return &DealerActor{
		actor:          actor,
		identity:       identity,
		connectAddress: connectAddress,
	}, nil
}

func (a *DealerActor) Connect() error {
	err := a.actor.zmqSocket.Connect(a.connectAddress)

	if err != nil {
		return err
	}

	return nil
}

func (a *DealerActor) Close() {
	a.actor.Close()
}

func (a *DealerActor) Send(message Message) {
	a.actor.Send(message)
}

func (a *DealerActor) Receive(ctx context.Context) (*Message, error) {
	return a.actor.Receive(ctx)
}

func (a *DealerActor) Run(ctx context.Context, errors chan<- error, logs chan<- string) {
	a.actor.Run(ctx, errors, logs)
}
