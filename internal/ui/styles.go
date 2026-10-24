package ui

import (
	"image/color"
	"strconv"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/Dem1241/connectwatch/internal/connect"
	"github.com/Dem1241/connectwatch/internal/health"
)

// Catppuccin Mocha, like slotwatch and puddle. The terminal's own background
// shows through; only the badges get a background of their own.
var (
	pink     = lipgloss.Color("#f5c2e7")
	mauve    = lipgloss.Color("#cba6f7")
	text     = lipgloss.Color("#cdd6f4")
	subtext  = lipgloss.Color("#a6adc8")
	overlay  = lipgloss.Color("#6c7086")
	surface1 = lipgloss.Color("#45475a")
	surface2 = lipgloss.Color("#585b70")
	base     = lipgloss.Color("#1e1e2e")
	green    = lipgloss.Color("#a6e3a1")
	yellow   = lipgloss.Color("#f9e2af")
	red      = lipgloss.Color("#f38ba8")
	blue     = lipgloss.Color("#89b4fa")
	peach    = lipgloss.Color("#fab387")
)

var (
	sText   = lipgloss.NewStyle().Foreground(text)
	sSub    = lipgloss.NewStyle().Foreground(subtext)
	sDim    = lipgloss.NewStyle().Foreground(overlay)
	sPink   = lipgloss.NewStyle().Foreground(pink)
	sTitle  = lipgloss.NewStyle().Foreground(pink).Bold(true)
	sErr    = lipgloss.NewStyle().Foreground(red)
	sKey    = lipgloss.NewStyle().Foreground(pink)
	sPane   = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(surface1).Padding(0, 1)
	sPaneOn = sPane.BorderForeground(pink)
	sOK     = lipgloss.NewStyle().Foreground(green)
	sWarn   = lipgloss.NewStyle().Foreground(yellow)
	sDanger = lipgloss.NewStyle().Foreground(red)
	sClass  = lipgloss.NewStyle().Foreground(peach)
	sLabel  = lipgloss.NewStyle().Foreground(mauve)
	sTab    = lipgloss.NewStyle().Foreground(base).Background(pink).Padding(0, 1)
	sTabOff = lipgloss.NewStyle().Foreground(subtext).Padding(0, 1)
)

// levelStyle is the colour of a health level.
func levelStyle(l health.Level) lipgloss.Style {
	switch l {
	case health.Danger:
		return sDanger
	case health.Warn:
		return sWarn
	}
	return sOK
}

// stateColor is the colour of a connector or task state.
func stateColor(state string) color.Color {
	switch state {
	case connect.Running:
		return green
	case connect.Failed:
		return red
	case connect.Unassigned:
		return yellow
	case connect.Restarting:
		return blue
	}
	return surface2
}

// label is the word on a connector's badge. Connect says RUNNING for a
// connector whose tasks all failed, so that one says failing instead.
func label(st health.Status) (string, color.Color) {
	switch {
	case st.State == connect.Failed:
		return "failed", red
	case st.Failed > 0:
		return "failing", red
	case st.State == connect.Running && st.Level == health.Warn:
		return "running", yellow
	case st.State == connect.Unassigned:
		return "waiting", yellow
	case st.State == "":
		return "unknown", surface2
	}
	return strings.ToLower(st.State), stateColor(st.State)
}

// badge is the coloured pill in front of every connector.
func badge(st health.Status) string {
	l, bg := label(st)
	fg := base
	if bg == surface2 {
		fg = text
	}
	return lipgloss.NewStyle().Foreground(fg).Background(bg).Width(9).Align(lipgloss.Center).Render(l)
}

// taskDots is a dot per task in the colour of its state.
func taskDots(tasks []connect.Task, most int) string {
	var s string
	for i, t := range tasks {
		if i == most {
			return s + sDim.Render(" +"+strconv.Itoa(len(tasks)-most))
		}
		s += lipgloss.NewStyle().Foreground(stateColor(t.State)).Render("●")
	}
	return s
}
