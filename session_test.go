// The session's bookkeeping, with a connection that records what it sends: counting both ways, the retained
// frames, what a resume sends again, and tool calls answered by their result.

package voiceagents

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type recorder struct {
	frames []Frame
	audio  [][]byte
}

func (r *recorder) sendText(_ string, data []byte) {
	frame, err := Decode(data)
	if err != nil {
		panic(err)
	}
	r.frames = append(r.frames, frame)
}

func (r *recorder) sendBytes(frame []byte) { r.audio = append(r.audio, frame) }

func (r *recorder) texts() []string {
	var texts []string
	for _, frame := range r.frames {
		switch f := frame.(type) {
		case *AgentSay:
			texts = append(texts, f.Text)
		case *ToolCall:
			texts = append(texts, f.Name)
		}
	}
	return texts
}

func start(t *testing.T, extra string) *SessionStart {
	t.Helper()
	data := `{"type":"session.start","session_id":"vs_1","call_id":"call_1","agent_id":"agent_1","direction":"inbound",` +
		`"from":{"kind":"number","value":"+14155550100"},"to":{"kind":"agent","value":"agent_1"},"session_type":"text",` +
		`"started_at":"2026-10-31T00:00:00Z","resume":{"token":"vsr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","window_ms":15000}` + extra + `}`
	frame, err := Decode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return frame.(*SessionStart)
}

func resumed(t *testing.T, received int, token byte) *SessionResumed {
	t.Helper()
	data := fmt.Sprintf(`{"type":"session.resumed","session_id":"vs_1","received":%d,"resume":{"token":"vsr_%s","window_ms":15000}}`,
		received, string(make([]byte, 0))+repeat(token, 30))
	frame, err := Decode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return frame.(*SessionResumed)
}

func repeat(b byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return string(out)
}

func decode(t *testing.T, data string) Frame {
	t.Helper()
	frame, err := Decode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func mustSay(t *testing.T, s *Session, text string, options ...SayOption) {
	t.Helper()
	if err := s.Say(text, options...); err != nil {
		t.Fatal(err)
	}
}

func TestCountsBothWaysAndKeepsTheLatestFrames(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, ""), r)
	if !s.Resumable() || s.Received() != 0 || s.Sent() != 0 {
		t.Fatal("fresh session")
	}
	s.read(decode(t, `{"type":"user.said","session_id":"vs_1","text":"hi"}`))
	s.read(decode(t, `{"type":"session.resumed","session_id":"vs_1","received":0}`))
	if s.Received() != 1 {
		t.Fatalf("received %d", s.Received())
	}
	for i := 0; i < RetainedFrames+5; i++ {
		mustSay(t, s, fmt.Sprintf("line %d", i))
	}
	if s.Sent() != RetainedFrames+5 || len(r.frames) != RetainedFrames+5 || len(s.kept) != RetainedFrames || s.before != 5 {
		t.Fatalf("sent %d, recorded %d, kept %d, before %d", s.Sent(), len(r.frames), len(s.kept), s.before)
	}
}

func TestAResumeSendsAgainWhatSentDidNotRead(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, ""), r)
	mustSay(t, s, "one")
	mustSay(t, s, "two")
	mustSay(t, s, "three")
	resume := s.resumeFrame()
	if resume.Token[:4] != "vsr_" || resume.Received != 0 {
		t.Fatalf("resume %#v", resume)
	}
	s.resuming = true
	if n := s.resumed(resumed(t, 1, 'b')); n != 2 {
		t.Fatalf("sent again %d", n)
	}
	want := []string{"one", "two", "three", "two", "three"}
	if got := r.texts(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v", got)
	}
	if token, _ := s.Token(); token[:5] != "vsr_b" {
		t.Fatalf("token %s", token)
	}
	if s.Sent() != 3 {
		t.Fatalf("sent %d", s.Sent())
	}
}

func TestAResumePastTheRetainedFramesHangsUp(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, ""), r)
	for i := 0; i < RetainedFrames+3; i++ {
		mustSay(t, s, fmt.Sprint(i))
	}
	s.resuming = true
	if n := s.resumed(resumed(t, 1, 'c')); n != 1 {
		t.Fatalf("sent again %d", n)
	}
	call, ok := r.frames[len(r.frames)-1].(*ToolCall)
	if !ok || call.Name != "hangup" {
		t.Fatalf("last frame %#v", r.frames[len(r.frames)-1])
	}
}

func TestAResumePastWhatWasSentSendsNothingAgain(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, ""), r)
	mustSay(t, s, "one")
	s.resuming = true
	if n := s.resumed(resumed(t, 5, 'e')); n != 0 || s.Sent() != 5 {
		t.Fatalf("sent again %d, sent %d", n, s.Sent())
	}
}

func TestToolCallsAreAnsweredByTheirResult(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, ""), r)
	results := make(chan *ToolResult, 1)
	go func() {
		result, err := s.TransferNumber(context.Background(), "+14155550199")
		if err != nil {
			panic(err)
		}
		results <- result
	}()
	waitFor(t, func() bool { return len(r.frames) == 1 })
	call := r.frames[0].(*ToolCall)
	if call.Name != "transfer" || fmt.Sprint(call.Arguments["to"]) != "map[kind:number value:+14155550199]" {
		t.Fatalf("call %#v", call)
	}
	s.answerTool(decode(t, fmt.Sprintf(`{"type":"tool.result","session_id":"vs_1","call_ref":%q,"ok":true}`, call.CallRef)).(*ToolResult))
	select {
	case result := <-results:
		if !result.Ok {
			t.Fatal("not ok")
		}
	case <-time.After(time.Second):
		t.Fatal("no result")
	}
}

func TestASessionThatEndedRefusesMore(t *testing.T) {
	s := newSession(start(t, ""), &recorder{})
	errs := make(chan error, 1)
	go func() { _, err := s.Hangup(context.Background()); errs <- err }()
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.pending) == 1 })
	s.finish(decode(t, `{"type":"session.end","session_id":"vs_1","reason":"caller_hung_up","duration_ms":10}`).(*SessionEnd))
	if err := <-errs; !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("got %v", err)
	}
	if err := s.Say("too late"); !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("got %v", err)
	}
}

func TestAudioGoesOutOnTheSessionStream(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, `,"session_type":"audio","audio":{"stream":7}`), r)
	if err := s.SendAudio(make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	if len(r.audio) != 1 || r.audio[0][0] != 0 || r.audio[0][1] != 7 || len(r.audio[0]) != 642 {
		t.Fatalf("audio %v", r.audio)
	}
}

func TestAFrameThatBreaksTheProtocolIsRefusedBeforeItCounts(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, ""), r)
	var frameErr *FrameError
	if err := s.Say(""); !errors.As(err, &frameErr) {
		t.Fatalf("got %v", err)
	}
	if _, err := s.SendDTMF(context.Background(), "xyz"); !errors.As(err, &frameErr) {
		t.Fatalf("got %v", err)
	}
	if s.Sent() != 0 || len(r.frames) != 0 {
		t.Fatal("counted a refused frame")
	}
}

func TestFramesSentDuringAResumeWaitForTheReplay(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, ""), r)
	mustSay(t, s, "before")
	s.mu.Lock()
	s.resuming = true
	s.mu.Unlock()
	mustSay(t, s, "during")
	results := make(chan *ToolResult, 1)
	go func() { result, _ := s.Hangup(context.Background()); results <- result }()
	waitFor(t, func() bool { return s.Sent() == 3 })
	if got := r.texts(); fmt.Sprint(got) != "[before]" {
		t.Fatalf("sent early: %v", got)
	}
	if n := s.resumed(resumed(t, 1, 'd')); n != 2 {
		t.Fatalf("sent again %d", n)
	}
	if got := r.texts(); fmt.Sprint(got) != "[before during hangup]" {
		t.Fatalf("got %v", got)
	}
	call := r.frames[2].(*ToolCall)
	s.answerTool(decode(t, fmt.Sprintf(`{"type":"tool.result","session_id":"vs_1","call_ref":%q,"ok":true}`, call.CallRef)).(*ToolResult))
	if result := <-results; result == nil || !result.Ok {
		t.Fatal("no result")
	}
}

func TestAudioDuringAResumeIsDroppedAndAfterTheEndRefused(t *testing.T) {
	r := &recorder{}
	s := newSession(start(t, `,"session_type":"audio","audio":{"stream":7}`), r)
	s.resuming = true
	if err := s.SendAudio(make([]byte, 640)); err != nil || len(r.audio) != 0 {
		t.Fatalf("err %v, audio %d", err, len(r.audio))
	}
	s.resuming = false
	s.finish(decode(t, `{"type":"session.end","session_id":"vs_1","reason":"caller_hung_up","duration_ms":10}`).(*SessionEnd))
	if err := s.SendAudio(make([]byte, 640)); !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("got %v", err)
	}
}

func TestALostSessionFailsItsToolCallsAndRefusesMore(t *testing.T) {
	s := newSession(start(t, ""), &recorder{})
	errs := make(chan error, 1)
	go func() { _, err := s.Hangup(context.Background()); errs <- err }()
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.pending) == 1 })
	s.abandon()
	if err := <-errs; !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("got %v", err)
	}
	if !s.Ended() || !s.Lost() {
		t.Fatal("not ended")
	}
	if err := s.Say("too late"); !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("got %v", err)
	}
}

func TestUnknownSessionFramesCountAndErrorsDoNot(t *testing.T) {
	s := newSession(start(t, ""), &recorder{})
	s.read(decode(t, `{"type":"session.note","session_id":"vs_1","x":1}`))
	s.read(decode(t, `{"type":"error","session_id":"vs_1","error":{"code":"frame_not_allowed","message":"no"}}`))
	if s.Received() != 1 {
		t.Fatalf("received %d", s.Received())
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
