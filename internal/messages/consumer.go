package messages

import "fmt"

type ConsumerAnnounceMessage struct{}

func (m ConsumerAnnounceMessage) ToGenericMessage() Message {
	return Message{Parts: []string{"CONSUMER", "ANNOUNCE"}}
}

type ConsumerAnnounceAckMessage struct{}

func (m ConsumerAnnounceAckMessage) ToGenericMessage() Message {
	return Message{Parts: []string{"CONSUMER", "ANNOUNCE", "ACK"}}
}

type ConsumerWorkMessage struct {
	ID       string
	Function string
	Args     []string
}

func (m ConsumerWorkMessage) ToGenericMessage() Message {
	parts := []string{"CONSUMER", "WORK", m.ID, m.Function}
	parts = append(parts, m.Args...)
	return Message{Parts: parts}
}

type ConsumerWorkResultsMessage struct {
	ID      string
	Results []string
}

func (m ConsumerWorkResultsMessage) ToGenericMessage() Message {
	parts := []string{"CONSUMER", "WORK", m.ID, "OK"}
	parts = append(parts, m.Results...)
	return Message{Parts: parts}
}

type ConsumerWorkErrorsMessage struct {
	ID     string
	Errors []error
}

func (m ConsumerWorkErrorsMessage) ToGenericMessage() Message {
	parts := []string{"CONSUMER", "WORK", m.ID, "KO"}

	for _, e := range m.Errors {
		parts = append(parts, e.Error())
	}

	return Message{Parts: parts}
}

func init() {
	RegisterReversibleRule(
		"messages.ConsumerAnnounceMessage",
		func(parts []string) bool {
			return len(parts) == 2 && parts[0] == "CONSUMER" && parts[1] == "ANNOUNCE"
		},
		func(parts []string) any {
			return ConsumerAnnounceMessage{}
		},
		func(specialized any) Message {
			return specialized.(ConsumerAnnounceMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ConsumerAnnounceAckMessage",
		func(parts []string) bool {
			return len(parts) == 3 && parts[0] == "CONSUMER" && parts[1] == "ANNOUNCE" && parts[2] == "ACK"
		},
		func(parts []string) any {
			return ConsumerAnnounceAckMessage{}
		},
		func(specialized any) Message {
			return specialized.(ConsumerAnnounceAckMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ConsumerWorkResultsMessage",
		func(parts []string) bool {
			return len(parts) >= 4 && parts[0] == "CONSUMER" && parts[1] == "WORK" && parts[3] == "OK"
		},
		func(parts []string) any {
			return ConsumerWorkResultsMessage{
				ID:      parts[2],
				Results: parts[4:],
			}
		},
		func(specialized any) Message {
			return specialized.(ConsumerWorkResultsMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ConsumerWorkErrorsMessage",
		func(parts []string) bool {
			return len(parts) >= 4 && parts[0] == "CONSUMER" && parts[1] == "WORK" && parts[3] == "KO"
		},
		func(parts []string) any {
			errors := make([]error, 0, len(parts)-4)

			for _, e := range parts[4:] {
				errors = append(errors, fmt.Errorf(e))
			}

			return ConsumerWorkErrorsMessage{
				ID:     parts[2],
				Errors: errors,
			}
		},
		func(specialized any) Message {
			return specialized.(ConsumerWorkErrorsMessage).ToGenericMessage()
		},
	)

	RegisterReversibleRule(
		"messages.ConsumerWorkMessage",
		func(parts []string) bool {
			return len(parts) >= 4 && parts[0] == "CONSUMER" && parts[1] == "WORK"
		},
		func(parts []string) any {
			return ConsumerWorkMessage{
				ID:       parts[2],
				Function: parts[3],
				Args:     parts[4:],
			}
		},
		func(specialized any) Message {
			return specialized.(ConsumerWorkMessage).ToGenericMessage()
		},
	)
}
