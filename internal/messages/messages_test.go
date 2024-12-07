package messages_test

import (
	"testing"

	"github.com/Pigotz/gopherate/internal/messages"
)

func TestInterpretMessage(t *testing.T) {
	// Test ProducerAnnounceMessage
	msg1 := messages.Message{Parts: []string{"PRODUCER", "ANNOUNCE"}}
	specialized1 := messages.InterpretMessage(msg1)

	if _, ok := specialized1.(messages.ProducerAnnounceMessage); !ok {
		t.Errorf("Expected ProducerAnnounceMessage, got %#v", specialized1)
	}

	// Test ProducerWorkMessage
	msg2 := messages.Message{Parts: []string{"PRODUCER", "WORK", "some-id", "some-function", "arg1", "arg2"}}
	specialized2 := messages.InterpretMessage(msg2)

	if swm, ok := specialized2.(messages.ProducerWorkMessage); ok {
		if swm.ID != "some-id" || swm.Function != "some-function" || len(swm.Args) != 2 {
			t.Errorf("Unexpected ProducerWorkMessage content: %#v", swm)
		}
	} else {
		t.Errorf("Expected ProducerWorkMessage, got %#v", specialized2)
	}
}

func TestConvertToGenericMessage(t *testing.T) {
	// Test ProducerAnnounceMessage to Generic
	specialized1 := messages.ProducerAnnounceMessage{}
	generic1, ok := messages.ConvertToGenericMessage(specialized1)

	if !ok || len(generic1.Parts) != 2 || generic1.Parts[0] != "PRODUCER" || generic1.Parts[1] != "ANNOUNCE" {
		t.Errorf("Failed to convert ProducerAnnounceMessage to generic: %#v", generic1)
	}

	// Test ProducerWorkMessage to Generic
	specialized2 := messages.ProducerWorkMessage{
		ID:       "some-id",
		Function: "some-function",
		Args:     []string{"arg1", "arg2"},
	}
	generic2, ok := messages.ConvertToGenericMessage(specialized2)

	if !ok || len(generic2.Parts) != 6 || generic2.Parts[0] != "PRODUCER" || generic2.Parts[1] != "WORK" ||
		generic2.Parts[2] != "some-id" || generic2.Parts[3] != "some-function" {
		t.Errorf("Failed to convert ProducerWorkMessage to generic: %#v", generic2)
	}
}

func TestRoundTripConversion(t *testing.T) {
	// Round-trip conversion
	original := messages.Message{Parts: []string{"PRODUCER", "WORK", "some-id", "some-function", "arg1", "arg2"}}
	specialized := messages.InterpretMessage(original)

	if generic, ok := messages.ConvertToGenericMessage(specialized); !ok || len(generic.Parts) != len(original.Parts) {
		t.Errorf("Round-trip conversion failed: original=%#v, generic=%#v", original, generic)
	}
}
