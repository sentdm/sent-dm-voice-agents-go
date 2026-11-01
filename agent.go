// The agent: one process that serves calls on the Sent Voice Agents protocol.
//
// Give New a Handler and call Run or Serve: the agent connects to Sent with the account API key and serves
// sessions until stopped. The runtime answers heartbeats, opens a new connection when one drains or is lost, and
// resumes the sessions that can move, so the handlers only see calls. One goroutine runs each session's handlers
// in the order Sent's frames arrived; different sessions run at the same time.

package voiceagents

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

// DefaultURL is where the agent connects when Options.URL is empty.
const DefaultURL = "wss://" + Host + ConnectPath

const (
	// maxBackoff is the longest wait before another attempt to connect.
	maxBackoff = 30 * time.Second
	// handshakeTimeout is how long a handshake may take before the attempt counts as failed, and how long an open
	// socket may wait for connection.ready.
	handshakeTimeout = 10 * time.Second
	// silentIntervals is how many heartbeat intervals without a frame make a connection dead, as Heartbeats in
	// docs/protocol.md says.
	silentIntervals = 3
	// writeTimeout bounds one write to the socket.
	writeTimeout = 10 * time.Second
	// readLimit is the largest message read from Sent.
	readLimit = 4 << 20
)

// refusals is why Sent refuses a handshake with each status, from Connecting in docs/protocol.md. Each needs a person.
var refusals = map[int]string{
	400: "agent_id or the subprotocol is missing",
	401: "the API key is not valid",
	403: "voice is not enabled for the account, or the key does not own the agent",
	404: "the agent does not exist",
}

// Refused is the error Run returns when Sent refused the connection for a reason that needs a person.
type Refused struct {
	Status int
	Reason string
}

func (r *Refused) Error() string { return fmt.Sprintf("%d: %s", r.Status, r.Reason) }

// Handler is what an agent does on its calls. Embed BaseHandler and override what you need.
type Handler interface {
	// OnConnected: a connection to Sent is ready for sessions.
	OnConnected(ctx context.Context, ready *ConnectionReady)
	// OnSessionStart: a call reached the agent. In a text session, Say to speak; an audio session waits for audio.
	OnSessionStart(ctx context.Context, session *Session)
	// OnUserSaid: text sessions, the caller said something. With Correction true it replaces the previous one.
	OnUserSaid(ctx context.Context, session *Session, said *UserSaid)
	// OnAudio: audio sessions, the caller's audio, 16-bit little-endian mono PCM at session.SampleRate.
	OnAudio(ctx context.Context, session *Session, samples []byte)
	// OnToolResult: Sent answered a tool call. The call's waiting Tool gets the same result.
	OnToolResult(ctx context.Context, session *Session, result *ToolResult)
	// OnFrame: a session frame this SDK version does not know. It was counted; nothing else is needed.
	OnFrame(ctx context.Context, session *Session, frame *UnknownFrame)
	// OnSessionEnd: the call left the agent. end.Reason says why.
	OnSessionEnd(ctx context.Context, session *Session, end *SessionEnd)
	// OnSessionLost: the session's connection was lost and it could not be resumed. Sent ends the call with
	// agent_unreachable; no session.end follows.
	OnSessionLost(ctx context.Context, session *Session)
	// OnError: Sent refused a frame; the connection stays open.
	OnError(ctx context.Context, err *ErrorFrame)
}

// BaseHandler does nothing on every event. Embed it and override the methods you need.
type BaseHandler struct{}

func (BaseHandler) OnConnected(context.Context, *ConnectionReady)       {}
func (BaseHandler) OnSessionStart(context.Context, *Session)            {}
func (BaseHandler) OnUserSaid(context.Context, *Session, *UserSaid)     {}
func (BaseHandler) OnAudio(context.Context, *Session, []byte)           {}
func (BaseHandler) OnToolResult(context.Context, *Session, *ToolResult) {}
func (BaseHandler) OnFrame(context.Context, *Session, *UnknownFrame)    {}
func (BaseHandler) OnSessionEnd(context.Context, *Session, *SessionEnd) {}
func (BaseHandler) OnSessionLost(context.Context, *Session)             {}
func (BaseHandler) OnError(context.Context, *ErrorFrame)                {}

// Options is what an agent needs.
type Options struct {
	// APIKey is the account API key. Keep it on the server the agent runs on.
	APIKey string
	// AgentID is the agent, as POST /v3/voice/agents created it.
	AgentID string
	// URL is where to connect. Empty means Sent.
	URL string
	// Handler is what the agent does on its calls.
	Handler Handler
	// Logger is where the runtime's log lines go. Nil means slog's default; tokens never appear in them.
	Logger *slog.Logger
}

// connection is one WebSocket to Sent, and the sessions it reads and writes.
type connection struct {
	ready     bool
	draining  bool
	sessions  map[string]*Session
	pending   []*Session
	heartbeat time.Duration
	expected  string
	due       time.Duration
	watchdog  *time.Timer
	log       *slog.Logger

	writeMu sync.Mutex
	socket  *websocket.Conn
}

func (c *connection) setSocket(socket *websocket.Conn) {
	c.writeMu.Lock()
	c.socket = socket
	c.writeMu.Unlock()
}

func (c *connection) sendText(kind string, data []byte) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.socket == nil {
		// A session frame is still counted as sent. Sent's received on the next resume says to send it again.
		c.log.Info("not sent: the connection is not open", "type", kind)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	if err := c.socket.Write(ctx, websocket.MessageText, data); err != nil {
		c.log.Info("not sent: the connection is not open", "type", kind)
	}
}

func (c *connection) sendBytes(frame []byte) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.socket == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	_ = c.socket.Write(ctx, websocket.MessageBinary, frame)
}

// Agent serves calls. Its Handler's methods run on the session's own goroutine, in order.
type Agent struct {
	apiKey  string
	agentID string
	url     string
	handler Handler
	log     *slog.Logger

	mu          sync.Mutex
	connections map[*connection]struct{}
	live        map[string]*Session
	attempt     int
	stopped     bool
	err         error
	done        chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// New is an agent for the options. It connects when Run or Serve is called.
func New(options Options) *Agent {
	a := &Agent{
		apiKey:      options.APIKey,
		agentID:     options.AgentID,
		url:         options.URL,
		handler:     options.Handler,
		log:         options.Logger,
		connections: map[*connection]struct{}{},
		live:        map[string]*Session{},
		done:        make(chan struct{}),
	}
	if a.url == "" {
		a.url = DefaultURL
	}
	if a.handler == nil {
		a.handler = BaseHandler{}
	}
	if a.log == nil {
		a.log = slog.Default()
	}
	return a
}

// ----- running -----

// Run serves calls until Stop is called or ctx ends. It returns a *Refused when Sent refuses the connection for a
// reason that needs a person, and nil otherwise.
func (a *Agent) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	a.ctx, a.cancel = ctx, cancel
	a.mu.Unlock()
	a.open(nil, 0)
	select {
	case <-a.done:
	case <-ctx.Done():
		a.Stop()
	}
	a.wg.Wait()
	cancel()
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

// Serve runs the agent until SIGINT or SIGTERM. For a program whose main is the agent.
func (a *Agent) Serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return a.Run(ctx)
}

// Stop stops serving: every connection closes with 1000, so the calls on them follow the agent's fallback at once.
func (a *Agent) Stop() { a.finish(nil) }

// Sessions is every live session.
func (a *Agent) Sessions() []*Session {
	a.mu.Lock()
	defer a.mu.Unlock()
	sessions := make([]*Session, 0, len(a.live))
	for _, session := range a.live {
		sessions = append(sessions, session)
	}
	return sessions
}

func (a *Agent) finish(err error) {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return
	}
	a.stopped = true
	a.err = err
	connections := make([]*connection, 0, len(a.connections))
	for c := range a.connections {
		connections = append(connections, c)
	}
	cancel := a.cancel
	a.mu.Unlock()
	for _, c := range connections {
		c.writeMu.Lock()
		socket := c.socket
		c.writeMu.Unlock()
		if socket != nil {
			_ = socket.Close(websocket.StatusNormalClosure, "")
		}
	}
	if cancel != nil {
		cancel()
	}
	close(a.done)
}

// ----- connections -----

func (a *Agent) open(pending []*Session, delay time.Duration) {
	c := &connection{sessions: map[string]*Session{}, pending: pending, log: a.log}
	a.mu.Lock()
	a.connections[c] = struct{}{}
	ctx := a.ctx
	a.mu.Unlock()
	a.wg.Add(1)
	go a.serve(ctx, c, delay)
}

func (a *Agent) serve(ctx context.Context, c *connection, delay time.Duration) {
	defer a.wg.Done()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			a.closed(c)
			return
		}
	}
	target, err := url.Parse(a.url)
	if err != nil {
		a.log.Error("the URL does not parse", "error", err)
		a.finish(fmt.Errorf("url %q: %w", a.url, err))
		a.closed(c)
		return
	}
	query := target.Query()
	query.Set("agent_id", a.agentID)
	target.RawQuery = query.Encode()
	dialCtx, dialCancel := context.WithTimeout(ctx, handshakeTimeout)
	socket, response, err := websocket.Dial(dialCtx, target.String(), &websocket.DialOptions{
		HTTPHeader:   http.Header{"Authorization": {"Bearer " + a.apiKey}},
		Subprotocols: []string{Subprotocol},
	})
	dialCancel()
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		if reason, ok := refusals[status]; ok {
			a.log.Error("refused", "status", status, "reason", reason)
			a.finish(&Refused{Status: status, Reason: reason})
		} else if status > 0 {
			a.log.Warn("refused, trying again", "status", status)
		} else {
			a.log.Warn("connect failed", "error", err)
		}
		a.closed(c)
		return
	}
	socket.SetReadLimit(readLimit)
	c.setSocket(socket)
	a.log.Info("connected", "host", target.Host)
	// Until connection.ready arrives the socket has the handshake's deadline; ready hands it to heard.
	a.arm(c, socket, handshakeTimeout, "connection.ready")
	a.read(ctx, c, socket)
	a.mu.Lock()
	if c.watchdog != nil {
		c.watchdog.Stop()
	}
	a.mu.Unlock()
	_ = socket.CloseNow()
	c.setSocket(nil)
	a.closed(c)
}

func (a *Agent) read(ctx context.Context, c *connection, socket *websocket.Conn) {
	for {
		kind, data, err := socket.Read(ctx)
		if err != nil {
			if status := websocket.CloseStatus(err); status != -1 {
				a.log.Info("closed", "code", int(status))
			} else if ctx.Err() != nil {
				a.log.Info("closed by the agent")
			} else {
				a.log.Info("lost", "error", err)
			}
			return
		}
		a.heard(c)
		if kind == websocket.MessageBinary {
			a.audio(c, data)
			continue
		}
		frame, err := Decode(data)
		if err != nil {
			a.log.Warn("ignored a frame", "error", err)
			continue
		}
		a.handle(ctx, c, frame)
	}
}

// arm sets the connection's one watchdog: nothing within due closes the socket.
func (a *Agent) arm(c *connection, socket *websocket.Conn, due time.Duration, expected string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c.expected, c.due = expected, due
	if c.watchdog == nil {
		c.watchdog = time.AfterFunc(due, func() {
			a.mu.Lock()
			expected, due := c.expected, c.due
			a.mu.Unlock()
			a.log.Warn("nothing arrived, closing", "expected", expected, "after", due)
			_ = socket.CloseNow()
		})
		return
	}
	c.watchdog.Reset(due)
}

// heard resets the watchdog: no frame for three heartbeat intervals means the connection is dead.
func (a *Agent) heard(c *connection) {
	a.mu.Lock()
	ready, heartbeat, watchdog := c.ready, c.heartbeat, c.watchdog
	if ready && watchdog != nil {
		c.expected, c.due = "frame", silentIntervals*heartbeat
		watchdog.Reset(silentIntervals * heartbeat)
	}
	a.mu.Unlock()
}

func (a *Agent) handle(ctx context.Context, c *connection, frame Frame) {
	defer func() {
		if r := recover(); r != nil {
			// A frame the runtime could not handle is skipped; the connection goes on.
			a.log.Error("handling a frame failed", "type", frame.Type(), "panic", r, "stack", string(debug.Stack()))
		}
	}()
	switch f := frame.(type) {
	case *ConnectionReady:
		a.ready(ctx, c, f)
	case *Ping:
		if data, err := Encode(&Pong{Ts: f.Ts}); err == nil {
			c.sendText("pong", data)
		}
		// A connection that lived through a heartbeat interval is a good one: the next loss starts the backoff
		// over. A connection that dies before its first ping keeps it growing.
		a.mu.Lock()
		if c.ready {
			a.attempt = 0
		}
		a.mu.Unlock()
	case *ConnectionDraining:
		a.drain(c)
	case *SessionStart:
		a.start(ctx, c, f)
	case *SessionResumed:
		a.resumed(c, f)
	case *ErrorFrame:
		a.failed(c, f)
		go a.safely(ctx, func() { a.handler.OnError(ctx, f) })
	case SessionFrame:
		if f.Session() == "" {
			a.log.Info("ignored a frame", "type", frame.Type())
			return
		}
		a.sessionFrame(c, f)
	default:
		a.log.Info("ignored a frame", "type", frame.Type())
	}
}

func (a *Agent) ready(ctx context.Context, c *connection, frame *ConnectionReady) {
	a.mu.Lock()
	c.ready = true
	c.heartbeat = time.Duration(frame.HeartbeatIntervalMs) * time.Millisecond
	var pending []*Session
	for _, session := range c.pending {
		if a.live[session.ID] == session {
			pending = append(pending, session)
		}
	}
	c.pending = nil
	a.mu.Unlock()
	a.heard(c)
	a.log.Info("ready", "protocol", frame.ProtocolVersion, "max_sessions", frame.MaxSessions, "resuming", len(pending))
	for _, session := range pending {
		a.resume(c, session)
	}
	go a.safely(ctx, func() { a.handler.OnConnected(ctx, frame) })
}

func (a *Agent) resume(c *connection, session *Session) {
	a.mu.Lock()
	for other := range a.connections {
		delete(other.sessions, session.ID)
	}
	c.sessions[session.ID] = session
	a.mu.Unlock()
	session.mu.Lock()
	session.conn = c
	session.resuming = true
	session.mu.Unlock()
	if data, err := Encode(session.resumeFrame()); err == nil {
		c.sendText("session.resume", data)
	}
}

func (a *Agent) drain(c *connection) {
	a.mu.Lock()
	c.draining = true
	var moving []*Session
	for _, session := range c.sessions {
		session.mu.Lock()
		if session.token != "" && !session.resuming {
			moving = append(moving, session)
		}
		session.mu.Unlock()
	}
	a.mu.Unlock()
	a.log.Info("draining", "moving", len(moving))
	a.open(moving, 0)
}

func (a *Agent) start(ctx context.Context, c *connection, frame *SessionStart) {
	session := newSession(frame, c)
	a.mu.Lock()
	a.live[session.ID] = session
	c.sessions[session.ID] = session
	a.mu.Unlock()
	a.log.Info("session started", "session", session.ID, "session_type", frame.SessionType, "direction", frame.Direction)
	a.wg.Add(1)
	go a.runSession(ctx, session)
}

func (a *Agent) sessionFrame(c *connection, frame SessionFrame) {
	a.mu.Lock()
	session, ok := c.sessions[frame.Session()]
	a.mu.Unlock()
	// A session that moved to another connection is read there: Sent replays what this one did not deliver.
	if !ok {
		return
	}
	session.mu.Lock()
	resuming := session.resuming
	session.mu.Unlock()
	if resuming {
		return
	}
	session.read(frame)
	switch f := frame.(type) {
	case *ToolResult:
		session.answerTool(f)
		session.inbox.put(f)
	case *SessionEnd:
		session.finish(f)
		a.forget(session)
		session.inbox.put(f)
	default:
		session.inbox.put(frame)
	}
}

func (a *Agent) audio(c *connection, message []byte) {
	if len(message) < HeaderBytes {
		return
	}
	stream := StreamOf(message)
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range c.sessions {
		session.mu.Lock()
		match := session.hasStream && session.stream == stream && !session.resuming
		session.mu.Unlock()
		if match {
			session.inbox.put(message)
			return
		}
	}
}

func (a *Agent) resumed(c *connection, frame *SessionResumed) {
	a.mu.Lock()
	session, ok := c.sessions[frame.SessionID]
	a.mu.Unlock()
	if !ok {
		return
	}
	session.mu.Lock()
	resuming := session.resuming
	session.mu.Unlock()
	if !resuming {
		return
	}
	again := session.resumed(frame)
	a.log.Info("session resumed", "session", session.ID, "sent_again", again)
}

// failed: an error is uncounted, as Counting frames in docs/protocol.md says. Only one that refuses a
// session.resume changes the session's fate; any other is the handler's to read.
func (a *Agent) failed(c *connection, frame *ErrorFrame) {
	a.log.Warn("error", "code", frame.Error.Code, "message", frame.Error.Message)
	aboutResume := (frame.FrameType != nil && *frame.FrameType == "session.resume") ||
		frame.Error.Code == "resume_failed" || frame.Error.Code == "connection_full"
	if !aboutResume || frame.SessionID == nil {
		return
	}
	a.mu.Lock()
	session, ok := c.sessions[*frame.SessionID]
	if ok {
		session.mu.Lock()
		if !session.resuming {
			ok = false
		} else {
			session.resuming = false
			delete(c.sessions, session.ID)
		}
		session.mu.Unlock()
	}
	a.mu.Unlock()
	if !ok {
		return
	}
	if frame.Error.Code == "connection_full" {
		// This connection already carries max_sessions. The token is still good on another one.
		a.open([]*Session{session}, 0)
		return
	}
	a.lost(session)
}

func (a *Agent) forget(session *Session) {
	a.mu.Lock()
	delete(a.live, session.ID)
	for c := range a.connections {
		delete(c.sessions, session.ID)
	}
	a.mu.Unlock()
}

func (a *Agent) lost(session *Session) {
	a.forget(session)
	session.abandon()
	session.inbox.put(nil)
}

func (a *Agent) closed(c *connection) {
	a.mu.Lock()
	if _, ok := a.connections[c]; !ok {
		a.mu.Unlock()
		return
	}
	delete(a.connections, c)
	if a.stopped {
		a.mu.Unlock()
		return
	}
	queued := func(session *Session) bool {
		for other := range a.connections {
			for _, s := range other.pending {
				if s == session {
					return true
				}
			}
		}
		return false
	}
	var lostSessions, movable []*Session
	for _, session := range c.sessions {
		session.mu.Lock()
		canMove := session.token != "" && !session.resuming
		session.mu.Unlock()
		if queued(session) {
			continue
		}
		if canMove {
			movable = append(movable, session)
			continue
		}
		a.log.Warn("session lost", "session", session.ID, "reason", map[bool]string{true: "its resume was not answered", false: "it has no resume token"}[session.token != ""])
		lostSessions = append(lostSessions, session)
	}
	var pending []*Session
	for _, session := range c.pending {
		if a.live[session.ID] == session {
			pending = append(pending, session)
		}
	}
	pending = append(pending, movable...)
	drained := c.draining && len(pending) == 0
	var delay time.Duration
	if !drained {
		delay = a.backoff()
	}
	a.mu.Unlock()
	// Reported whether or not a reconnect follows: a drained connection's leftovers are lost too.
	for _, session := range lostSessions {
		a.lost(session)
	}
	if drained {
		return
	}
	a.log.Info("reconnecting", "in", delay, "resuming", len(pending))
	a.open(pending, delay)
}

// backoff runs under a.mu.
func (a *Agent) backoff() time.Duration {
	var delay time.Duration
	if a.attempt > 0 {
		delay = min(maxBackoff, time.Second<<min(a.attempt-1, 20))
	}
	a.attempt++
	return delay
}

// ----- the session's goroutine -----

func (a *Agent) runSession(ctx context.Context, session *Session) {
	defer a.wg.Done()
	a.safely(ctx, func() { a.handler.OnSessionStart(ctx, session) })
	for {
		item, ok := session.inbox.get(ctx)
		if !ok {
			return
		}
		switch f := item.(type) {
		case nil:
			a.safely(ctx, func() { a.handler.OnSessionLost(ctx, session) })
			return
		case []byte:
			a.safely(ctx, func() { a.handler.OnAudio(ctx, session, SamplesOf(f)) })
		case *UserSaid:
			a.safely(ctx, func() { a.handler.OnUserSaid(ctx, session, f) })
		case *ToolResult:
			a.safely(ctx, func() { a.handler.OnToolResult(ctx, session, f) })
		case *SessionEnd:
			a.safely(ctx, func() { a.handler.OnSessionEnd(ctx, session, f) })
			return
		case *UnknownFrame:
			a.safely(ctx, func() { a.handler.OnFrame(ctx, session, f) })
		}
	}
}

// safely runs a handler; a panic in it is logged and does not take the session or the agent down.
func (a *Agent) safely(_ context.Context, handler func()) {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("a handler failed", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	handler()
}
