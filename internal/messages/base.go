package messages

import (
	"fmt"
)

// Generic Message Struct
type Message struct {
	Parts []string
}

func NewMessage(parts ...string) Message {
	return Message{Parts: parts}
}

// ReversibleMessage Interface
type ReversibleMessage interface {
	ToGenericMessage() Message
}

// Message Rules
type MessageRule struct {
	Match   func(parts []string) bool
	Handler func(parts []string) any
}

// Registry of Rules
var messageRules []MessageRule
var reverseHandlers = make(map[string]func(any) Message)

// Register Rule
func RegisterReversibleRule(
	spacializedType string,
	match func(parts []string) bool,
	handler func(parts []string) any,
	reverse func(any) Message,
) {
	messageRules = append(messageRules, MessageRule{Match: match, Handler: handler})
	reverseHandlers[spacializedType] = reverse
}

// Interpret Message
func InterpretMessage(msg Message) any {
	for _, rule := range messageRules {
		if rule.Match(msg.Parts) {
			return rule.Handler(msg.Parts)
		}
	}

	return nil
}

// Convert to Generic Message Using Reverse Handlers
func ConvertToGenericMessage(specialized any) (Message, bool) {
	typeName := fmt.Sprintf("%T", specialized)

	if reverse, ok := reverseHandlers[typeName]; ok {
		return reverse(specialized), true
	}

	return Message{}, false
}
