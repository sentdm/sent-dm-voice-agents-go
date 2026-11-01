package voiceagents

import (
	"fmt"
	"testing"
)

func TestDecide(t *testing.T) {
	shape := func(text string) string {
		var out []string
		for _, step := range Decide(text) {
			if step.Action == "say" {
				s := "say " + step.Text
				if step.Last != nil {
					s += fmt.Sprintf(" last=%v", *step.Last)
				}
				out = append(out, s)
			} else {
				out = append(out, fmt.Sprintf("tool %s %v", step.Name, step.Arguments))
			}
		}
		return fmt.Sprint(out)
	}
	cases := map[string]string{
		"echo hello there":    "[say hello there]",
		"stream Let me think": "[say Let last=false say me think]",
		"transfer":            "[tool transfer map[to:map[kind:user value:conformance-desk]]]",
		"hangup":              "[tool hangup map[]]",
		"dtmf 12#":            "[tool send_dtmf map[digits:12#]]",
		"hold":                "[tool hold map[held:true]]",
		"something else":      "[]",
	}
	for text, want := range cases {
		if got := shape(text); got != want {
			t.Errorf("%q: got %s, want %s", text, got, want)
		}
	}
}
