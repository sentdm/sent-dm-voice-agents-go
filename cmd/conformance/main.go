// The conformance agent of sent-dm-voice-protocol, as a program: the protocol's harness runs it with
// SENT_CONFORMANCE_URL set, in CI before every release, and by hand against a harness on localhost.
package main

import (
	"log/slog"
	"os"

	voiceagents "github.com/sentdm/sent-dm-voice-agents-go"
)

func main() {
	url := os.Getenv("SENT_CONFORMANCE_URL")
	if url == "" {
		url = "ws://localhost:8765/v1/agents/connect"
	}
	agent := voiceagents.New(voiceagents.Options{
		APIKey:  "conformance",
		AgentID: "agent_conformance",
		URL:     url,
		Handler: voiceagents.ConformanceHandler{},
		Logger:  slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err := agent.Serve(); err != nil {
		slog.Error("the agent stopped", "error", err)
		os.Exit(1)
	}
}
