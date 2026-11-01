// The agent's bookkeeping around a connection, without a socket: which errors refuse a resume, when the
// backoff starts over, what a lost session's goroutine sees, and what a draining connection reports when it
// closes.

package voiceagents

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func errorFrame(t *testing.T, frameType, code string) *ErrorFrame {
	t.Helper()
	extra := ""
	if frameType != "" {
		extra = fmt.Sprintf(`,"frame_type":%q`, frameType)
	}
	return decode(t, fmt.Sprintf(`{"type":"error","session_id":"vs_1","error":{"code":%q,"message":"no"}%s}`, code, extra)).(*ErrorFrame)
}

func agentWith(t *testing.T, handler Handler) (*Agent, *connection) {
	t.Helper()
	a := New(Options{APIKey: "k", AgentID: "agent_1", Handler: handler, Logger: quiet})
	a.ctx, a.cancel = context.WithCancel(context.Background())
	c := &connection{sessions: map[string]*Session{}, log: quiet, ready: true}
	a.connections[c] = struct{}{}
	return a, c
}

func resumingSession(t *testing.T, a *Agent, c *connection) *Session {
	t.Helper()
	s := newSession(start(t, ""), c)
	s.resuming = true
	a.live[s.ID] = s
	c.sessions[s.ID] = s
	return s
}

func TestOnlyAnErrorAboutTheResumeRefusesIt(t *testing.T) {
	a, c := agentWith(t, nil)
	s := resumingSession(t, a, c)
	a.failed(c, errorFrame(t, "agent.say", "frame_not_allowed"))
	if !s.resuming || c.sessions[s.ID] == nil || s.Lost() {
		t.Fatal("an unrelated error refused the resume")
	}
	a.failed(c, errorFrame(t, "session.resume", "resume_failed"))
	if !s.Lost() || c.sessions[s.ID] != nil || a.live[s.ID] != nil {
		t.Fatal("the refused resume did not lose the session")
	}
	if item, _ := s.inbox.get(context.Background()); item != nil {
		t.Fatalf("inbox got %v", item)
	}
	a.Stop()
}

func TestAFullConnectionMovesTheResumeElsewhere(t *testing.T) {
	a, c := agentWith(t, nil)
	s := resumingSession(t, a, c)
	a.failed(c, errorFrame(t, "", "connection_full"))
	if s.Lost() || c.sessions[s.ID] != nil {
		t.Fatal("lost or kept")
	}
	a.mu.Lock()
	var opened []*connection
	for other := range a.connections {
		if other != c {
			opened = append(opened, other)
		}
	}
	a.mu.Unlock()
	if len(opened) != 1 || len(opened[0].pending) != 1 || opened[0].pending[0] != s {
		t.Fatalf("opened %d", len(opened))
	}
	a.Stop()
	a.wg.Wait()
}

func TestTheBackoffStartsOverAfterAHeartbeatNotOnReady(t *testing.T) {
	a, c := agentWith(t, nil)
	c.ready = false
	a.attempt = 3
	a.ready(context.Background(), c, decode(t, `{"type":"connection.ready","protocol_version":"0.2.0","heartbeat_interval_ms":15000,"max_sessions":10}`).(*ConnectionReady))
	if a.attempt != 3 {
		t.Fatal("ready reset the backoff")
	}
	a.handle(context.Background(), c, decode(t, `{"type":"ping","ts":1}`))
	if a.attempt != 0 {
		t.Fatal("a ping did not reset the backoff")
	}
	a.Stop()
}

type noticing struct {
	BaseHandler
	seen chan string
}

func (h *noticing) OnSessionStart(ctx context.Context, session *Session) {
	if _, err := session.Hangup(ctx); errors.Is(err, ErrSessionEnded) {
		h.seen <- "tool failed"
	}
}

func (h *noticing) OnSessionLost(context.Context, *Session) {
	h.seen <- "lost"
	panic("thrown by the handler")
}

func TestALostSessionReachesItsHandlerAfterItsToolCallFails(t *testing.T) {
	h := &noticing{seen: make(chan string, 4)}
	a, c := agentWith(t, h)
	a.start(context.Background(), c, start(t, ""))
	s := a.Sessions()[0]
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.pending) == 1 })
	a.lost(s)
	if got := []string{<-h.seen, <-h.seen}; fmt.Sprint(got) != "[tool failed lost]" {
		t.Fatalf("got %v", got)
	}
	if len(a.Sessions()) != 0 {
		t.Fatal("still live")
	}
	a.Stop()
	a.wg.Wait()
}

func TestADrainingConnectionReportsItsLeftoverSessionsLostWhenItCloses(t *testing.T) {
	h := &noticing{seen: make(chan string, 4)}
	a, c := agentWith(t, h)
	c.draining = true
	a.start(context.Background(), c, decode(t, `{"type":"session.start","session_id":"vs_1","call_id":"call_1","agent_id":"agent_1","direction":"inbound","from":{"kind":"number","value":"+14155550100"},"to":{"kind":"agent","value":"agent_1"},"session_type":"text","started_at":"2026-10-31T00:00:00Z"}`).(*SessionStart))
	s := a.Sessions()[0]
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.pending) == 1 })
	a.closed(c)
	if got := []string{<-h.seen, <-h.seen}; fmt.Sprint(got) != "[tool failed lost]" {
		t.Fatalf("got %v", got)
	}
	if !s.Lost() || len(a.Sessions()) != 0 || len(a.connections) != 0 {
		t.Fatal("not reported")
	}
	a.Stop()
	a.wg.Wait()
}
