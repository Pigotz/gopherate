package fsm

import (
	"context"
	"fmt"
)

type State interface{}

type Callback func(ctx context.Context, state State, errors chan<- error, logs chan<- string) (State, error)
type Callbacks map[string]Callback

type FSM struct {
	state     State
	callbacks Callbacks
}

func NewFSM(state State, callbacks Callbacks) *FSM {
	if callbacks == nil {
		callbacks = make(Callbacks)
	}

	return &FSM{state: state, callbacks: callbacks}
}

func (m *FSM) RegisterCallback(name string, callback Callback) {
	m.callbacks[name] = callback
}

func (m *FSM) Run(ctx context.Context, errors chan<- error, logs chan<- string) {
	for ctx.Err() == nil && m.state != nil {
		callbackID := fmt.Sprintf("%T", m.state)

		if callback, ok := m.callbacks[callbackID]; ok {
			newState, err := callback(ctx, m.state, errors, logs)

			if err != nil {
				errors <- err
			}

			m.state = newState
		} else {
			return
		}
	}
}
