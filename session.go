// One session: a call while this agent is on it.
//
// A Session is what the handlers of an Agent receive. It carries what session.start said about the call, sends
// the agent's frames on whichever connection the session is on, keeps the latest counted frames so a resume can
// send again the ones Sent did not read, and turns a tool call into the tool.result that answers it.

package voiceagents

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrSessionEnded is returned once the call has left the agent: session.end arrived, or the session was lost.
var ErrSessionEnded = errors.New("the session has ended")

// ErrTextSession is returned by SendAudio on a text session.
var ErrTextSession = errors.New("the session is a text session")

// sender is the connection a session sends on: the agent gives each session one, and moves it on a resume.
type sender interface {
	sendText(kind string, data []byte)
	sendBytes(frame []byte)
}

// Session is the call the agent is on, and the ways to act on it. The agent creates it; its handlers run in order.
type Session struct {
	ID        string
	CallID    string
	AgentID   string
	Direction string
	From      *Party
	To        *Party
	// SessionType is text or audio.
	SessionType string
	Language    string
	Context     map[string]any
	StartedAt   string
	// SampleRate is the Hz of the session's 16-bit mono PCM, both ways, in an audio session.
	SampleRate int
	// Start is what session.start said about the call.
	Start *SessionStart

	mu        sync.Mutex
	stream    int
	hasStream bool
	token     string
	windowMs  int64
	received  int64
	end       *SessionEnd
	lost      bool
	conn      sender
	resuming  bool
	kept      []Frame
	before    int64
	calls     int
	pending   map[string]chan *ToolResult
	inbox     *inbox
}

func newSession(start *SessionStart, conn sender) *Session {
	s := &Session{
		ID:          start.SessionID,
		CallID:      start.CallID,
		AgentID:     start.AgentID,
		Direction:   start.Direction,
		From:        start.From,
		To:          start.To,
		SessionType: start.SessionType,
		Context:     map[string]any{},
		StartedAt:   start.StartedAt,
		SampleRate:  16000,
		Start:       start,
		conn:        conn,
		pending:     map[string]chan *ToolResult{},
		inbox:       newInbox(),
	}
	if start.Language != nil {
		s.Language = *start.Language
	}
	for k, v := range start.Context {
		s.Context[k] = v
	}
	if start.Audio != nil {
		s.stream, s.hasStream = int(start.Audio.Stream), true
		if start.Audio.SampleRate != nil {
			s.SampleRate = int(*start.Audio.SampleRate)
		}
	}
	if start.Resume != nil {
		s.token, s.windowMs = start.Resume.Token, start.Resume.WindowMs
	}
	return s
}

// ----- what the agent reads about the call -----

// Stream is the number in the header of the session's binary audio frames on its current connection.
func (s *Session) Stream() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream, s.hasStream
}

// Token is the latest resume token, when the session can be resumed. Keep it out of logs.
func (s *Session) Token() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token, s.token != ""
}

// Resumable is whether the session survives a lost connection: it has a resume token.
func (s *Session) Resumable() bool { _, ok := s.Token(); return ok }

// WindowMs is how long Sent keeps the session for a resume after its connection is lost.
func (s *Session) WindowMs() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.windowMs
}

// Received is how many of Sent's counted frames for the session the agent has read: the received of a resume.
func (s *Session) Received() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

// Sent is how many counted frames the agent has sent for the session.
func (s *Session) Sent() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.before + int64(len(s.kept))
}

// End is session.end, once it arrived.
func (s *Session) End() *SessionEnd {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.end
}

// Lost is whether the connection was lost and the session could not be resumed: Sent ends the call without a
// session.end.
func (s *Session) Lost() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lost
}

// Ended is whether the call has left the agent: session.end arrived, or the session was lost.
func (s *Session) Ended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.end != nil || s.lost
}

func (s *Session) endedErr() error { return fmt.Errorf("session %s: %w", s.ID, ErrSessionEnded) }

// ----- what the agent does on the call -----

// SayOption changes how a Say is spoken.
type SayOption func(*AgentSay)

// Last marks a piece of a reply: send pieces with Last(false) and finish with Last(true); they are spoken as one
// utterance.
func Last(last bool) SayOption { return func(say *AgentSay) { say.Last = &last } }

// Interruptible is whether the caller speaking stops this speech.
func Interruptible(interruptible bool) SayOption {
	return func(say *AgentSay) { say.Interruptible = &interruptible }
}

// Say speaks text to the caller of a text session.
func (s *Session) Say(text string, options ...SayOption) error {
	say := &AgentSay{SessionID: s.ID, Text: text}
	for _, option := range options {
		option(say)
	}
	return s.send(say)
}

// Clear drops the speech or audio the agent sent that Sent has not played yet.
func (s *Session) Clear() error { return s.send(&AgentClear{SessionID: s.ID}) }

// SendAudio plays samples, 16-bit little-endian mono PCM at the session's sample rate, in an audio session.
// Send 20 ms per call, and no more than 200 ms. It returns ErrSessionEnded once the call has left the agent.
func (s *Session) SendAudio(samples []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.end != nil || s.lost {
		return s.endedErr()
	}
	if !s.hasStream {
		return fmt.Errorf("session %s: %w", s.ID, ErrTextSession)
	}
	// The stream number belongs to the old connection; on the new one it may be another caller's until
	// session.resumed assigns this session's. Audio is not counted, so there is nothing to send again.
	if s.resuming {
		return nil
	}
	frame, err := AudioFrame(s.stream, samples)
	if err != nil {
		return err
	}
	s.conn.sendBytes(frame)
	return nil
}

// Transfer moves the call to another party. On success the session then ends with transferred.
func (s *Session) Transfer(ctx context.Context, to *Party) (*ToolResult, error) {
	party := map[string]any{"kind": to.Kind}
	if to.Value != nil {
		party["value"] = *to.Value
	}
	return s.Tool(ctx, "transfer", map[string]any{"to": party})
}

// TransferNumber moves the call to an E.164 number.
func (s *Session) TransferNumber(ctx context.Context, number string) (*ToolResult, error) {
	return s.Transfer(ctx, &Party{Kind: "number", Value: &number})
}

// Hangup ends the call, after the speech the agent queued before it.
func (s *Session) Hangup(ctx context.Context) (*ToolResult, error) { return s.Tool(ctx, "hangup", nil) }

// SendDTMF plays digits on the call: 0 to 9, *, #, A to D, and w to wait half a second.
func (s *Session) SendDTMF(ctx context.Context, digits string) (*ToolResult, error) {
	return s.Tool(ctx, "send_dtmf", map[string]any{"digits": digits})
}

// Hold puts the caller on hold, or takes them off it.
func (s *Session) Hold(ctx context.Context, held bool) (*ToolResult, error) {
	return s.Tool(ctx, "hold", map[string]any{"held": held})
}

// Tool calls a tool by name and waits for its tool.result. It returns ErrSessionEnded if the session ends first,
// and ctx's error if ctx ends first.
func (s *Session) Tool(ctx context.Context, name string, arguments map[string]any) (*ToolResult, error) {
	s.mu.Lock()
	if s.end != nil || s.lost {
		s.mu.Unlock()
		return nil, s.endedErr()
	}
	s.calls++
	ref := fmt.Sprintf("t%d", s.calls)
	answer := make(chan *ToolResult, 1)
	s.pending[ref] = answer
	s.mu.Unlock()
	if err := s.send(&ToolCall{SessionID: s.ID, CallRef: ref, Name: name, Arguments: arguments}); err != nil {
		s.mu.Lock()
		delete(s.pending, ref)
		s.mu.Unlock()
		return nil, err
	}
	select {
	case result, ok := <-answer:
		if !ok {
			return nil, s.endedErr()
		}
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ----- what the agent's connection does with the session -----

// send sends a frame for the session on its connection, keeping it if it counts. A frame that breaks the
// protocol is refused before it counts. While a resume is unanswered the frame is kept and not sent: the replay
// after session.resumed sends it, once, on the connection the session is on. Keeping and sending happen under
// one lock, so the wire order is the kept order.
func (s *Session) send(frame Frame) error {
	data, err := Encode(frame)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.end != nil || s.lost {
		return s.endedErr()
	}
	s.keep(frame)
	if s.resuming {
		return nil
	}
	s.conn.sendText(frame.Type(), data)
	return nil
}

// keep runs under s.mu.
func (s *Session) keep(frame Frame) {
	if _, uncounted := Uncounted[frame.Type()]; uncounted {
		return
	}
	s.kept = append(s.kept, frame)
	if len(s.kept) > RetainedFrames {
		s.kept = s.kept[1:]
		s.before++
	}
}

// read counts one of Sent's frames for the session, as Counting frames in docs/protocol.md says.
func (s *Session) read(frame Frame) {
	if _, uncounted := Uncounted[frame.Type()]; uncounted {
		return
	}
	s.mu.Lock()
	s.received++
	s.mu.Unlock()
}

func (s *Session) resumeFrame() *SessionResume {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &SessionResume{SessionID: s.ID, Token: s.token, Received: s.received}
}

// resumed takes the answer to a session.resume and sends again, on the session's connection, the frames Sent
// did not read, or a hangup when Sent needs one the session no longer keeps. It returns how many it sent. The
// replay and the end of the resume happen under the session's lock, so nothing a handler sends can slip between.
func (s *Session) resumed(frame *SessionResumed) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = ""
	if frame.Resume != nil {
		s.token = frame.Resume.Token
	}
	if frame.Audio != nil {
		s.stream, s.hasStream = int(frame.Audio.Stream), true
	}
	// Without a next token the session ended while it was detached; its replay ends with session.end.
	if frame.Resume == nil {
		s.resuming = false
		return 0
	}
	var again []Frame
	if frame.Received < s.before {
		s.calls++
		again = []Frame{&ToolCall{SessionID: s.ID, CallRef: fmt.Sprintf("t%d", s.calls), Name: "hangup"}}
	} else {
		// A received past what was sent is Sent's mistake; there is nothing to send again for it.
		read := min(int(frame.Received-s.before), len(s.kept))
		again = append([]Frame(nil), s.kept[read:]...)
	}
	s.before = frame.Received
	s.kept = nil
	for _, frameAgain := range again {
		s.keep(frameAgain)
		if data, err := Encode(frameAgain); err == nil {
			s.conn.sendText(frameAgain.Type(), data)
		}
	}
	s.resuming = false
	return len(again)
}

func (s *Session) answerTool(result *ToolResult) {
	s.mu.Lock()
	answer, ok := s.pending[result.CallRef]
	if ok {
		delete(s.pending, result.CallRef)
	}
	s.mu.Unlock()
	if ok {
		answer <- result
	}
}

func (s *Session) finish(end *SessionEnd) {
	s.mu.Lock()
	s.end = end
	s.failPending()
	s.mu.Unlock()
}

// abandon marks the session lost: no tool.result or session.end will come, so every waiting tool call fails,
// and anything sent from now on is refused.
func (s *Session) abandon() {
	s.mu.Lock()
	s.lost = true
	s.resuming = false
	s.failPending()
	s.mu.Unlock()
}

// failPending runs under s.mu.
func (s *Session) failPending() {
	for ref, answer := range s.pending {
		close(answer)
		delete(s.pending, ref)
	}
}

// inbox is the session's queue of what its handlers see, in the order it arrived, with no bound.
type inbox struct {
	mu     sync.Mutex
	items  []any
	signal chan struct{}
}

func newInbox() *inbox { return &inbox{signal: make(chan struct{}, 1)} }

func (q *inbox) put(item any) {
	q.mu.Lock()
	q.items = append(q.items, item)
	q.mu.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func (q *inbox) get(ctx context.Context) (any, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			item := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			return item, true
		}
		q.mu.Unlock()
		select {
		case <-q.signal:
		case <-ctx.Done():
			return nil, false
		}
	}
}
