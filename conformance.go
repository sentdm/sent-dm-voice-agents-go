// The conformance agent of sent-dm-voice-protocol's conformance/README.md, on this SDK's API. The protocol's
// harness runs it in CI before every release, through cmd/conformance: it connects to SENT_CONFORMANCE_URL with
// any API key and does exactly what the README's table says.

package voiceagents

import (
	"context"
	"errors"
	"strings"
)

// ConformanceGreeting is what the conformance agent says first in a text session.
const ConformanceGreeting = "Hello from the conformance agent."

// ConformanceStep is one action the conformance agent performs for a user.said.
type ConformanceStep struct {
	// Action is say or tool.
	Action    string
	Text      string
	Last      *bool
	Name      string
	Arguments map[string]any
}

// Decide is what a user.said asks for: the actions the agent performs in order. A command is the text up to the
// first space; stream takes the next word as the first piece.
func Decide(text string) []ConformanceStep {
	command, rest, _ := strings.Cut(text, " ")
	switch command {
	case "echo":
		return []ConformanceStep{{Action: "say", Text: rest}}
	case "stream":
		word, remainder, _ := strings.Cut(rest, " ")
		last := false
		return []ConformanceStep{{Action: "say", Text: word, Last: &last}, {Action: "say", Text: remainder}}
	case "transfer":
		return []ConformanceStep{{Action: "tool", Name: "transfer", Arguments: map[string]any{"to": map[string]any{"kind": "user", "value": "conformance-desk"}}}}
	case "hangup":
		return []ConformanceStep{{Action: "tool", Name: "hangup"}}
	case "dtmf":
		return []ConformanceStep{{Action: "tool", Name: "send_dtmf", Arguments: map[string]any{"digits": rest}}}
	case "hold":
		return []ConformanceStep{{Action: "tool", Name: "hold", Arguments: map[string]any{"held": true}}}
	default:
		return nil
	}
}

// ConformanceHandler is the conformance agent's behaviour.
type ConformanceHandler struct{ BaseHandler }

func (ConformanceHandler) OnSessionStart(_ context.Context, session *Session) {
	if session.SessionType == "text" {
		_ = session.Say(ConformanceGreeting)
	}
}

func (ConformanceHandler) OnUserSaid(ctx context.Context, session *Session, said *UserSaid) {
	if said.Correction {
		if err := session.Clear(); errors.Is(err, ErrSessionEnded) {
			return
		}
	}
	for _, step := range Decide(said.Text) {
		var err error
		if step.Action == "say" {
			var options []SayOption
			if step.Last != nil {
				options = append(options, Last(*step.Last))
			}
			err = session.Say(step.Text, options...)
		} else {
			// The harness answers every tool call; the result also reaches OnToolResult.
			_, err = session.Tool(ctx, step.Name, step.Arguments)
		}
		if errors.Is(err, ErrSessionEnded) {
			return
		}
	}
}

func (ConformanceHandler) OnToolResult(_ context.Context, session *Session, result *ToolResult) {
	if !result.Ok && result.Error != nil {
		_ = session.Say("Tool failed: " + result.Error.Code)
	}
}

func (ConformanceHandler) OnAudio(_ context.Context, session *Session, samples []byte) {
	_ = session.SendAudio(samples)
}
