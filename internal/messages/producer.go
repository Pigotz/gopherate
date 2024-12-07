package messages

import (
	"fmt"
	"strconv"
)

type ProducerAnnounceMessage struct{}

func (m ProducerAnnounceMessage) ToGenericMessage() Message {
	return Message{Parts: []string{"PRODUCER", "ANNOUNCE"}}
}

type ProducerAnnounceAckMessage struct{}

func (m ProducerAnnounceAckMessage) ToGenericMessage() Message {
	return Message{Parts: []string{"PRODUCER", "ANNOUNCE", "ACK"}}
}

type ProducerWorkMessage struct {
	ID       string
	Function string
	Args     []string
}

func (m ProducerWorkMessage) ToGenericMessage() Message {
	parts := []string{"PRODUCER", "WORK", m.ID, m.Function}
	parts = append(parts, m.Args...)
	return Message{Parts: parts}
}

type ProducerWaitMessage struct {
	RetryAfter int
}

func (m ProducerWaitMessage) ToGenericMessage() Message {
	parts := []string{"PRODUCER", "WAIT", strconv.Itoa(m.RetryAfter)}
	return Message{Parts: parts}
}

type ProducerWorkResultMessage struct {
	ID      string
	Results []string
}

func (m ProducerWorkResultMessage) ToGenericMessage() Message {
	parts := []string{"PRODUCER", "WORK", m.ID, "OK"}
	parts = append(parts, m.Results...)
	return Message{Parts: parts}
}

type ProducerWorkErrorMessage struct {
	ID     string
	Errors []error
}

func (m ProducerWorkErrorMessage) ToGenericMessage() Message {
	parts := []string{"PRODUCER", "WORK", m.ID, "KO"}

	for _, err := range m.Errors {
		parts = append(parts, err.Error())
	}

	return Message{Parts: parts}
}

func init() {
	RegisterReversibleRule(
		"messages.ProducerAnnounceMessage",
		func(parts []string) bool {
			return len(parts) == 2 && parts[0] == "PRODUCER" && parts[1] == "ANNOUNCE"
		},
		func(parts []string) any {
			return ProducerAnnounceMessage{}
		},
		func(specialized any) Message {
			return specialized.(ProducerAnnounceMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ProducerAnnounceAckMessage",
		func(parts []string) bool {
			return len(parts) == 3 && parts[0] == "PRODUCER" && parts[1] == "ANNOUNCE" && parts[2] == "ACK"
		},
		func(parts []string) any {
			return ProducerAnnounceAckMessage{}
		},
		func(specialized any) Message {
			return specialized.(ProducerAnnounceAckMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ProducerWaitMessage",
		func(parts []string) bool {
			return len(parts) >= 2 && parts[0] == "PRODUCER" && parts[1] == "WAIT"
		},
		func(parts []string) any {
			if len(parts) != 3 {
				return ProducerWaitMessage{}
			}

			retryAfter, err := strconv.Atoi(parts[2])

			if err != nil {
				return ProducerWaitMessage{}
			}

			return ProducerWaitMessage{RetryAfter: retryAfter}
		},
		func(specialized any) Message {
			return specialized.(ProducerWaitMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ProducerWorkResultMessage",
		func(parts []string) bool {
			return len(parts) >= 4 && parts[0] == "PRODUCER" && parts[1] == "WORK" && parts[3] == "OK"
		},
		func(parts []string) any {
			return ProducerWorkResultMessage{
				ID:      parts[2],
				Results: parts[4:],
			}
		},
		func(specialized any) Message {
			return specialized.(ProducerWorkResultMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ProducerWorkErrorMessage",
		func(parts []string) bool {
			return len(parts) >= 4 && parts[0] == "PRODUCER" && parts[1] == "WORK" && parts[3] == "KO"
		},
		func(parts []string) any {
			errors := make([]error, 0, len(parts)-4)

			for _, e := range parts[4:] {
				errors = append(errors, fmt.Errorf(e))
			}

			return ProducerWorkErrorMessage{
				ID:     parts[2],
				Errors: errors,
			}
		},
		func(specialized any) Message {
			return specialized.(ProducerWorkErrorMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ProducerWorkMessage",
		func(parts []string) bool {
			return len(parts) >= 4 && parts[0] == "PRODUCER" && parts[1] == "WORK"
		},
		func(parts []string) any {
			return ProducerWorkMessage{
				ID:       parts[2],
				Function: parts[3],
				Args:     parts[4:],
			}
		},
		func(specialized any) Message {
			return specialized.(ProducerWorkMessage).ToGenericMessage()
		},
	)
}
