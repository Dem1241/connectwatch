package ui

import (
	"os"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/Dem1241/connectwatch/internal/demo"
	"github.com/Dem1241/connectwatch/internal/health"
)

// TestScreenshot writes a frame of the demo, a few minutes in, as ANSI to the
// file in CONNECTWATCH_SCREENSHOT. It's how assets/connectors.png is made:
//
//	CONNECTWATCH_SCREENSHOT=frame.ansi go test ./internal/ui -run TestScreenshot
//	freeze frame.ansi -o assets/connectors.png
func TestScreenshot(t *testing.T) {
	out := os.Getenv("CONNECTWATCH_SCREENSHOT")
	if out == "" {
		t.Skip("set CONNECTWATCH_SCREENSHOT to write a frame of the demo")
	}
	start := time.Date(2026, 10, 24, 14, 20, 0, 0, time.Local)
	c := demo.New(start)
	var m tea.Model = New(c, health.DefaultLimits, 2*time.Second, false)
	m, _ = m.Update(tea.WindowSizeMsg{Width: 132, Height: 32})
	for i := range 90 {
		m, _ = m.Update(snapshotMsg{snap: c.At(start.Add(time.Duration(i) * 2 * time.Second))})
	}
	if err := os.WriteFile(out, []byte(m.(Model).View().Content), 0o644); err != nil {
		t.Fatal(err)
	}
}
