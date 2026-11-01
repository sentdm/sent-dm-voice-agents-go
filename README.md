# Sent Voice Agents for Go

An AI agent that takes and places phone calls through [Sent](https://sent.dm), on the
[Sent Voice Agents protocol](https://github.com/sentdm/sent-dm-voice-protocol) 0.2.0, for Go 1.23
and later.

Status: Preview, with the protocol. Generated and rendered by `sent-dm-sdk-pipeline` from the protocol at
`v0.2.0`; every release passes the protocol's conformance scenarios in CI.

Read the [Voice Agents docs](https://docs.sent.dm) to get started.

## Install

```sh
go get github.com/sentdm/sent-dm-voice-agents-go
```

Go 1.23 or later. The one dependency is `github.com/coder/websocket`, which depends on nothing.

## Quickstart

```go
package main

import (
	"context"
	"os"
	"strings"

	voiceagents "github.com/sentdm/sent-dm-voice-agents-go"
)

type support struct{ voiceagents.BaseHandler }

func (support) OnSessionStart(ctx context.Context, session *voiceagents.Session) {
	session.Say("Hi, this is Acme support. How can I help?")
}

func (support) OnUserSaid(ctx context.Context, session *voiceagents.Session, said *voiceagents.UserSaid) {
	if strings.Contains(said.Text, "person") {
		session.Say("Transferring you now.")
		session.TransferNumber(ctx, "+14155550100")
	} else {
		session.Say("You said " + said.Text)
	}
}

func main() {
	agent := voiceagents.New(voiceagents.Options{
		APIKey:  os.Getenv("SENT_API_KEY"),
		AgentID: "agent_...",
		Handler: support{},
	})
	if err := agent.Serve(); err != nil {
		panic(err)
	}
}
```

`Serve()` connects to Sent with the account API key and serves calls until SIGINT or SIGTERM; `Run(ctx)` does
the same until `ctx` ends or `Stop()` is called. Each session's handlers run in order on their own goroutine, so
sessions never wait for one another. The runtime answers heartbeats, opens a new connection when one drains or
is lost, and resumes the sessions that can move; the handlers only see calls.

| Handler                               | When                                                                                           |
| ------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `OnSessionStart(ctx, session)`        | A call reached the agent. In a text session, `Say` to speak                                    |
| `OnUserSaid(ctx, session, said)`      | Text sessions: the caller said something; `said.Correction` means it replaces the previous one |
| `OnAudio(ctx, session, samples)`      | Audio sessions: the caller's audio, 16-bit mono PCM at `session.SampleRate`                    |
| `OnToolResult(ctx, session, result)`  | Sent answered a tool call; the waiting `Tool` call gets the same result                        |
| `OnSessionEnd(ctx, session, end)`     | The call left the agent; `end.Reason` says why                                                 |
| `OnSessionLost(ctx, session)`         | The connection was lost and the session could not be resumed                                   |

On a `Session`: `Say(text, Last(false), Interruptible(false))`, `Clear()`, `SendAudio(samples)`, and the tools
`Transfer(ctx, party)`, `TransferNumber(ctx, number)`, `Hangup(ctx)`, `SendDTMF(ctx, digits)`, `Hold(ctx, held)`,
each returning Sent's `ToolResult`. `session.From`, `To`, `Direction`, `SessionType`, `Language`, `Context`,
`SampleRate` carry what `session.start` said.

`frames.go` holds every frame as a type, with `Decode` and `Encode` that check the protocol's rules; it is
generated from the protocol and not edited by hand.

## Develop

```sh
gofmt -l . && go vet ./... && go test ./...     # formatting, vet, the tests against the protocol's fixtures
go build -o bin/conformance ./cmd/conformance   # the conformance agent the protocol's action runs in CI
```

Releases are cut by release-please from `main`; the release tag on this public repository is the release, and
the publish workflow asks the Go module proxy for it so it is indexed at once.
