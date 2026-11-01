// The generated frames against the protocol's own fixtures, copied here by the lane at render time.

package voiceagents

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func fixtures(t *testing.T, kind string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("tests", "fixtures", kind, "*", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no %s fixtures: %v", kind, err)
	}
	return files
}

func asValue(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("not a JSON object: %v", err)
	}
	return value
}

func TestValidFramesRoundTrip(t *testing.T) {
	for _, file := range fixtures(t, "valid") {
		t.Run(filepath.Base(filepath.Dir(file))+"/"+filepath.Base(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			frame, err := Decode(data)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if _, unknown := frame.(*UnknownFrame); unknown {
				t.Fatal("decoded as unknown")
			}
			original := asValue(t, data)
			if frame.Type() != original["type"] {
				t.Fatalf("type %q, want %q", frame.Type(), original["type"])
			}
			again, err := Encode(frame)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			encoded := asValue(t, again)
			for key, value := range original {
				if !reflect.DeepEqual(encoded[key], value) {
					t.Fatalf("%s changed on the way back: %v, want %v", key, encoded[key], value)
				}
			}
			decoded, err := Decode(again)
			if err != nil {
				t.Fatalf("decode again: %v", err)
			}
			twice, _ := Encode(decoded)
			if !reflect.DeepEqual(asValue(t, twice), encoded) {
				t.Fatal("a second round changed the frame")
			}
		})
	}
}

func TestInvalidFramesAreRefused(t *testing.T) {
	for _, file := range fixtures(t, "invalid") {
		t.Run(filepath.Base(filepath.Dir(file))+"/"+filepath.Base(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Decode(data)
			var frameErr *FrameError
			if !errors.As(err, &frameErr) {
				t.Fatalf("accepted although the schema refuses it (%v)", err)
			}
		})
	}
}

func TestUnknownFramesAndFieldsPass(t *testing.T) {
	frame, err := Decode([]byte(`{"type":"session.note","session_id":"vs_1","x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	unknown, ok := frame.(*UnknownFrame)
	if !ok || unknown.Type() != "session.note" || unknown.Session() != "vs_1" {
		t.Fatalf("got %#v", frame)
	}
	again, _ := Encode(unknown)
	if string(again) != `{"type":"session.note","session_id":"vs_1","x":1}` {
		t.Fatalf("encoded %s", again)
	}
	said, err := Decode([]byte(`{"type":"user.said","session_id":"vs_1","text":"hi","mood":"good"}`))
	if err != nil {
		t.Fatal(err)
	}
	if said.(*UserSaid).Correction {
		t.Fatal("correction should default to false")
	}
	end, err := Decode([]byte(`{"type":"session.end","session_id":"vs_1","reason":"added_later","duration_ms":1}`))
	if err != nil || end.(*SessionEnd).Reason != "added_later" {
		t.Fatalf("got %v, %v", end, err)
	}
}

func TestDefaultsAndFacts(t *testing.T) {
	frame, err := Decode([]byte(`{"type":"agent.say","session_id":"vs_1","text":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	say := frame.(*AgentSay)
	if say.Last == nil || !*say.Last || say.Interruptible == nil || !*say.Interruptible {
		t.Fatal("last and interruptible should default to true")
	}
	data, _ := Encode(&AgentSay{SessionID: "vs_1", Text: "hi, +1 <you>"})
	if string(data) != `{"type":"agent.say","session_id":"vs_1","text":"hi, +1 <you>"}` {
		t.Fatalf("encoded %s", data)
	}
	if Subprotocol != "sent.voice.v1" || RetainedFrames != 32 {
		t.Fatal("facts")
	}
	if _, ok := Uncounted["session.start"]; !ok {
		t.Fatal("session.start is uncounted")
	}
	if _, ok := Uncounted["error"]; !ok {
		t.Fatal("error is uncounted")
	}
	if _, ok := Uncounted["agent.say"]; ok {
		t.Fatal("agent.say counts")
	}
	if _, ok := FromAgent["pong"]; !ok {
		t.Fatal("pong is the agent's")
	}
}

func TestEncodeRefusesABadFrame(t *testing.T) {
	for _, frame := range []Frame{
		&AgentSay{SessionID: "nope", Text: "hi"},
		&ToolCall{SessionID: "vs_1", CallRef: "t1", Name: "transfer"},
	} {
		_, err := Encode(frame)
		var frameErr *FrameError
		if !errors.As(err, &frameErr) {
			t.Fatalf("accepted %#v", frame)
		}
	}
	if _, err := Decode([]byte("not json")); err == nil {
		t.Fatal("accepted non-JSON")
	}
	if _, err := Decode([]byte(`{"type":"ping","ts":1.0}`)); err == nil {
		t.Fatal("accepted 1.0 as an integer")
	}
}
