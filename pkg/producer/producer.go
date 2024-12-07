package producer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Pigotz/gopherate/internal/actor"
	"github.com/Pigotz/gopherate/internal/channels"
	"github.com/Pigotz/gopherate/internal/fsm"
	"github.com/Pigotz/gopherate/internal/messages"
)

type GenericTask interface {
	Function() string
	Args() []string
}

type Task struct {
	ID       string
	Function string
	Args     []string
}

type TaskChannels struct {
	results chan []string
	errors  chan []error
}

type Option func(*Producer)

func WithRunningTimeout(timeout time.Duration) Option {
	return func(s *Producer) {
		s.runningTimeout = timeout
	}
}

func WithTasksBuffer(buffer int) Option {
	return func(s *Producer) {
		s.tasksBuffer = buffer
	}
}

type Producer struct {
	dealer  *actor.DealerActor
	fsm     *fsm.FSM
	tasks   chan Task
	pending map[string]TaskChannels
	// Options
	runningTimeout time.Duration
	tasksBuffer    int
}

type StartState struct{}
type SendAnnounceState struct{}
type WaitAnnounceAckState struct{}
type RunningState struct{}

func NewProducer(identity string, connectAddress string, actorOpts []actor.Option, opts ...Option) (*Producer, error) {
	dealer, err := actor.NewDealerActor(identity, connectAddress, actorOpts...)

	if err != nil {
		return nil, err
	}

	producer := &Producer{
		runningTimeout: 60 * time.Second,
		tasksBuffer:    100,
	}

	for _, opt := range opts {
		opt(producer)
	}

	producer.dealer = dealer
	producer.tasks = make(chan Task, producer.tasksBuffer)
	producer.pending = make(map[string]TaskChannels)

	fsm := fsm.NewFSM(&StartState{}, fsm.Callbacks{
		"*producer.StartState": func(ctx context.Context, state fsm.State, errors chan<- error, logs chan<- string) (fsm.State, error) {
			logs <- "Starting"

			return &SendAnnounceState{}, nil
		},
		"*producer.SendAnnounceState": func(ctx context.Context, state fsm.State, errors chan<- error, logs chan<- string) (fsm.State, error) {
			logs <- "Sending Producer Announce Message"

			if message, ok := messages.ConvertToGenericMessage(messages.ProducerAnnounceMessage{}); ok {
				producer.dealer.Send(actor.NewMessage(message.Parts...))
			} else {
				return &StartState{}, fmt.Errorf("failed to convert ProducerAnnounceMessage to Message")
			}

			return &WaitAnnounceAckState{}, nil
		},
		"*producer.WaitAnnounceAckState": func(ctx context.Context, state fsm.State, errors chan<- error, logs chan<- string) (fsm.State, error) {
			logs <- "Waiting for Producer Announce Ack Message"

			dealerMessage, err := producer.dealer.Receive(ctx)

			if err != nil {
				errors <- err

				return nil, err
			}

			logs <- fmt.Sprintf("Received message: %v", dealerMessage)

			switch v := messages.InterpretMessage(messages.NewMessage(dealerMessage.Parts...)).(type) {
			case messages.ProducerAnnounceAckMessage:
				return &RunningState{}, nil
			default:
				return &StartState{}, fmt.Errorf("unknown message: %v", v)
			}
		},
		"*producer.RunningState": func(ctx context.Context, state fsm.State, errors chan<- error, logs chan<- string) (fsm.State, error) {
			logs <- "Running"

			ctx, cancel := context.WithTimeout(ctx, producer.runningTimeout)
			defer cancel()

			waitChannel := make(chan int, 100) // Unsure about this
			defer close(waitChannel)

			var waitGroup sync.WaitGroup

			processTasksLogs := make(chan string, 100)
			defer close(processTasksLogs)

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()

				channels.PrefixStringChannel(ctx, "[PROCESS TASKS]", processTasksLogs, logs)
			}()

			processTasksErrors := make(chan error, 100)
			defer close(processTasksErrors)

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()

				channels.WrapErrorChannel(ctx, "[PROCESS TASKS]", processTasksErrors, errors)
			}()

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()

				producer.processTasks(ctx, processTasksErrors, processTasksLogs, waitChannel)
			}()

			processMessagesLogs := make(chan string, 100)
			defer close(processMessagesLogs)

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()

				channels.PrefixStringChannel(ctx, "[PROCESS MESSAGES]", processMessagesLogs, logs)
			}()

			processMessagesErrors := make(chan error, 100)
			defer close(processMessagesErrors)

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()

				channels.WrapErrorChannel(ctx, "[PROCESS MESSAGES]", processMessagesErrors, errors)
			}()

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()

				producer.processMessages(ctx, processMessagesErrors, processMessagesLogs, waitChannel)
			}()

			waitGroup.Wait()

			return &StartState{}, nil
		},
	})

	producer.fsm = fsm

	return producer, nil
}

func (s *Producer) Connect() error {
	return s.dealer.Connect()
}

func (s *Producer) Close() {
	close(s.tasks)

	s.dealer.Close()
}

func (s *Producer) Run(ctx context.Context, errors chan<- error, logs chan<- string) {
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

		s.dealer.Run(ctx, dealerErrors, dealerLogs)
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

	s.fsm.Run(ctx, fsmErrors, fsmLogs)

	waitGroup.Wait()
}

func randomHex(bytes int) string {
	buffer := make([]byte, bytes)
	rand.Read(buffer)
	return fmt.Sprintf("%x", buffer)
}

func (s *Producer) Process(ctx context.Context, genericTask GenericTask, timeout time.Duration) ([]string, error) {
	task := &Task{
		ID:       randomHex(16),
		Function: genericTask.Function(),
		Args:     genericTask.Args(),
	}

	if s.pending == nil {
		s.pending = make(map[string]TaskChannels)
	}

	resultsChan := make(chan []string)
	defer close(resultsChan)

	errorsChan := make(chan []error)
	defer close(errorsChan)

	s.pending[task.ID] = TaskChannels{
		results: resultsChan,
		errors:  errorsChan,
	}

	defer delete(s.pending, task.ID)

	s.tasks <- *task

	if timeout == 0 {
		timeout = 5 * time.Second
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context done")
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout")
	case results := <-resultsChan:
		return results, nil
	case errs := <-errorsChan:
		return nil, errors.Join(errs...)
	}
}

func (s *Producer) processTasks(ctx context.Context, errors chan<- error, logs chan<- string, waitChannel <-chan int) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			logs <- "Context done"
			return
		case retryAfter := <-waitChannel:
			logs <- fmt.Sprintf("Waiting %d seconds", retryAfter)
			time.Sleep(time.Duration(retryAfter) * time.Second)
		case task, ok := <-s.tasks:
			if !ok {
				logs <- "No more tasks"
				continue
			}

			logs <- fmt.Sprintf("Sending task: %v", task)

			if message, ok := messages.ConvertToGenericMessage(messages.ProducerWorkMessage{
				ID:       task.ID,
				Function: task.Function,
				Args:     task.Args,
			}); ok {
				s.dealer.Send(actor.NewMessage(message.Parts...))
			} else {
				errors <- fmt.Errorf("failed to convert Producer Work Message to Generic Message")
				continue
			}
		}
	}
}

func (s *Producer) processMessages(ctx context.Context, errors chan<- error, logs chan<- string, waitChannel chan<- int) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			logs <- "Context done"
			return
		default:
			dealerMessage, err := s.dealer.Receive(ctx)

			if err != nil {
				errors <- err

				return
			}

			logs <- fmt.Sprintf("Received message: %v", dealerMessage)

			switch v := messages.InterpretMessage(messages.NewMessage(dealerMessage.Parts...)).(type) {
			case messages.ProducerWorkResultMessage:
				if task, ok := s.pending[v.ID]; ok {
					delete(s.pending, v.ID)

					task.results <- v.Results
				} else {
					errors <- fmt.Errorf("task with ID %s does not exist", v.ID)
				}
			case messages.ProducerWorkErrorMessage:
				if task, ok := s.pending[v.ID]; ok {
					delete(s.pending, v.ID)

					task.errors <- v.Errors
				} else {
					errors <- fmt.Errorf("task with ID %s does not exist", v.ID)
				}
			case messages.ProducerWaitMessage:
				logs <- fmt.Sprintf("Waiting %d seconds", v.RetryAfter)
				waitChannel <- v.RetryAfter
			default:
				errors <- fmt.Errorf("unknown message: %v", v)
			}
		}
	}
}
