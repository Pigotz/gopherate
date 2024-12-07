// FILE: fsm_test.go
package fsm

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewMachine(t *testing.T) {
	type InitialState struct{}

	initialState := &InitialState{}
	FSM := NewFSM(initialState, nil)

	if FSM.state != initialState {
		t.Errorf("expected initial state to be %v, got %v", initialState, FSM.state)
	}
}

func TestRegisterCallback(t *testing.T) {
	type InitialState struct{}
	type NextState struct{}

	initialState := &InitialState{}

	FSM := NewFSM(initialState, nil)
	callbackName := fmt.Sprintf("%T", initialState)

	FSM.RegisterCallback(callbackName, func(ctx context.Context, state State, errors chan<- error, logs chan<- string) (State, error) {
		return &NextState{}, nil
	})

	if _, ok := FSM.callbacks[callbackName]; !ok {
		t.Errorf("expected callback %v to be registered", callbackName)
	}
}

func TestRunMachine(t *testing.T) {
	type InitialState struct{}
	type NextState struct{}

	initialState := &InitialState{}
	nextState := &NextState{}
	machine := NewFSM(initialState, nil)

	machine.RegisterCallback("*fsm.InitialState", func(ctx context.Context, state State, errors chan<- error, logs chan<- string) (State, error) {
		return nextState, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	errorsChan := make(chan error, 1)
	logsChan := make(chan string, 1)

	go machine.Run(ctx, errorsChan, logsChan)

	select {
	case err := <-errorsChan:
		t.Errorf("unexpected error: %v", err)
	case <-time.After(100 * time.Millisecond):
		if machine.state != nextState {
			t.Errorf("expected state to be %v, got %v", nextState, machine.state)
		}
	}
}

func TestRunMachineMultipleCallbacks(t *testing.T) {
	type InitialState struct{}
	type SecondState struct{}
	type FinalState struct{}

	initialState := &InitialState{}
	secondState := &SecondState{}
	finalState := &FinalState{}

	machine := NewFSM(initialState, nil)

	machine.RegisterCallback("*fsm.InitialState", func(ctx context.Context, state State, errors chan<- error, logs chan<- string) (State, error) {
		return secondState, nil
	})

	machine.RegisterCallback("*fsm.SecondState", func(ctx context.Context, state State, errors chan<- error, logs chan<- string) (State, error) {
		return finalState, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	errorsChan := make(chan error, 1)
	logsChan := make(chan string, 1)

	go machine.Run(ctx, errorsChan, logsChan)

	select {
	case err := <-errorsChan:
		t.Errorf("unexpected error: %v", err)
	case <-time.After(200 * time.Millisecond):
		if machine.state != finalState {
			t.Errorf("expected state to be %v, got %v", finalState, machine.state)
		}
	}
}
