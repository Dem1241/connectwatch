// Package ui is connectwatch's terminal interface: a list of connectors,
// worst first, and the details of the one you pick, down to the stack trace.
package ui

import (
	"context"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/Dem1241/connectwatch/internal/connect"
	"github.com/Dem1241/connectwatch/internal/health"
)

// Source is where snapshots come from, and what restarts and pauses
// connectors; *connect.Client is one.
type Source interface {
	Snapshot(ctx context.Context) (connect.Snapshot, error)
	Restart(ctx context.Context, name string) error
	Pause(ctx context.Context, name string) error
	Resume(ctx context.Context, name string) error
}

// sortOrder is how the connector list is sorted.
type sortOrder int

const (
	byHealth sortOrder = iota
	byName
	byType
)

func (o sortOrder) String() string {
	switch o {
	case byName:
		return "name"
	case byType:
		return "type"
	}
	return "health"
}

// tab is what the detail pane shows.
type tab int

const (
	overviewTab tab = iota
	traceTab
	configTab
)

var tabNames = []string{"overview", "trace", "config"}

// action is something connectwatch asks Connect to do, once you said yes.
type action struct {
	verb string // restart, pause or resume
	name string
}

func (a action) question() string {
	switch a.verb {
	case "restart":
		return "restart the failed tasks of " + a.name + "?"
	}
	return a.verb + " " + a.name + "?"
}

type Model struct {
	src      Source
	tracker  *health.Tracker
	interval time.Duration
	readOnly bool

	width, height int

	cluster  connect.Cluster
	statuses []health.Status
	updated  time.Time // when the last snapshot was taken
	err      error     // from the last snapshot, cleared by the next good one
	loading  bool

	idx       int // index into the visible (filtered) connectors
	detailTop int // scroll position of the detail pane
	tab       tab
	paused    bool
	sort      sortOrder

	filtering bool
	filter    string

	confirm *action // waiting for a y or n
	notice  string  // what happened to the last action
	failed  bool    // whether that action failed
}

// New creates the interface, refreshing every interval. A read-only one
// never restarts or pauses anything.
func New(src Source, limits health.Limits, interval time.Duration, readOnly bool) Model {
	return Model{src: src, tracker: health.NewTracker(limits), interval: interval, readOnly: readOnly, loading: true}
}

type (
	snapshotMsg struct {
		snap connect.Snapshot
		err  error
	}
	tickMsg   struct{}
	actionMsg struct {
		action
		err error
	}
)

func (m Model) Init() tea.Cmd { return fetch(m.src) }

func fetch(src Source) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		snap, err := src.Snapshot(ctx)
		return snapshotMsg{snap, err}
	}
}

func run(src Source, a action) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var err error
		switch a.verb {
		case "restart":
			err = src.Restart(ctx, a.name)
		case "pause":
			err = src.Pause(ctx, a.name)
		case "resume":
			err = src.Resume(ctx, a.name)
		}
		return actionMsg{a, err}
	}
}

func (m Model) tick() tea.Cmd {
	return tea.Tick(m.interval, func(time.Time) tea.Msg { return tickMsg{} })
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil

	case snapshotMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, m.tick()
		}
		selected := m.selectedName()
		m.err = nil
		m.cluster = msg.snap.Cluster
		m.statuses = m.tracker.Update(msg.snap)
		m.updated = msg.snap.At
		m.sortStatuses()
		m.keepSelection(selected)
		return m, m.tick()

	case tickMsg:
		if m.paused {
			return m, m.tick()
		}
		m.loading = true
		return m, fetch(m.src)

	case actionMsg:
		if msg.err != nil {
			m.notice, m.failed = "couldn't "+msg.verb+" "+msg.name+": "+msg.err.Error(), true
			return m, nil
		}
		m.notice, m.failed = past(msg.verb)+" "+msg.name, false
		// look again straight away rather than at the next tick
		return m, fetch(m.src)

	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

func past(verb string) string {
	switch verb {
	case "pause":
		return "paused"
	case "resume":
		return "resumed"
	}
	return "restarted the failed tasks of"
}

func (m Model) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	s := k.String()
	if s == "ctrl+c" {
		return m, tea.Quit
	}
	if m.confirm != nil {
		a := *m.confirm
		m.confirm = nil
		if s == "y" || s == "Y" {
			m.notice, m.failed = "", false
			return m, run(m.src, a)
		}
		return m, nil
	}
	if m.filtering {
		switch s {
		case "esc":
			m.filtering, m.filter = false, ""
		case "enter":
			m.filtering = false
		case "backspace":
			if r := []rune(m.filter); len(r) > 0 {
				m.filter = string(r[:len(r)-1])
			}
		default:
			if k.Text != "" {
				m.filter += k.Text
			}
		}
		m.idx, m.detailTop = 0, 0
		return m, nil
	}

	m.notice = ""
	n := len(m.visible())
	switch s {
	case "q":
		return m, tea.Quit
	case "/":
		m.filtering = true
	case "esc":
		m.filter = ""
	case "up", "k":
		m.move(-1, n)
	case "down", "j":
		m.move(1, n)
	case "home", "g":
		m.move(-n, n)
	case "end", "G":
		m.move(n, n)
	case "ctrl+d", "J":
		m.detailTop += 5
	case "ctrl+u", "K":
		m.detailTop = max(0, m.detailTop-5)
	case "tab":
		m.tab = (m.tab + 1) % tab(len(tabNames))
		m.detailTop = 0
	case "shift+tab":
		m.tab = (m.tab + tab(len(tabNames)) - 1) % tab(len(tabNames))
		m.detailTop = 0
	case "space":
		m.paused = !m.paused
	case "s":
		selected := m.selectedName()
		m.sort = (m.sort + 1) % 3
		m.sortStatuses()
		m.keepSelection(selected)
	case "r":
		if !m.loading {
			m.loading = true
			return m, fetch(m.src)
		}
	case "R", "p":
		m.ask(s)
	}
	return m, nil
}

// ask sets up the question for R or p on the selected connector.
func (m *Model) ask(key string) {
	st, ok := m.selected()
	switch {
	case !ok:
		return
	case m.readOnly:
		m.notice, m.failed = "connectwatch is read-only (--read-only)", true
		return
	case key == "R" && st.State != connect.Failed && st.Failed == 0:
		m.notice, m.failed = "nothing has failed in "+st.Name, false
		return
	}
	a := action{verb: "restart", name: st.Name}
	if key == "p" {
		a.verb = "pause"
		if st.State == connect.Paused || st.State == connect.Stopped {
			a.verb = "resume"
		}
	}
	m.confirm = &a
}

func (m *Model) move(by, n int) {
	m.idx = clamp(m.idx+by, 0, n-1)
	m.detailTop = 0
}

// visible is the connectors that match the filter.
func (m Model) visible() []health.Status {
	if m.filter == "" {
		return m.statuses
	}
	f := strings.ToLower(m.filter)
	var out []health.Status
	for _, st := range m.statuses {
		l, _ := label(st)
		hay := strings.ToLower(st.Name + " " + st.Class + " " + st.Type + " " + l + " " + st.Worker + " " + st.Level.String())
		if strings.Contains(hay, f) {
			out = append(out, st)
		}
	}
	return out
}

func (m Model) selected() (health.Status, bool) {
	if v := m.visible(); m.idx >= 0 && m.idx < len(v) {
		return v[m.idx], true
	}
	return health.Status{}, false
}

func (m Model) selectedName() string {
	st, _ := m.selected()
	return st.Name
}

// keepSelection keeps the same connector selected when the list changes
// under it.
func (m *Model) keepSelection(name string) {
	v := m.visible()
	if i := slices.IndexFunc(v, func(st health.Status) bool { return st.Name == name }); i >= 0 {
		m.idx = i
	}
	m.idx = clamp(m.idx, 0, len(v)-1)
}

func (m *Model) sortStatuses() {
	switch m.sort {
	case byName:
		slices.SortStableFunc(m.statuses, func(a, b health.Status) int { return strings.Compare(a.Name, b.Name) })
	case byType:
		slices.SortStableFunc(m.statuses, func(a, b health.Status) int {
			if c := strings.Compare(a.Type, b.Type); c != 0 {
				return c
			}
			return strings.Compare(a.Name, b.Name)
		})
	default:
		health.Sort(m.statuses)
	}
}

func (m Model) View() tea.View {
	v := tea.NewView("")
	v.AltScreen = true
	v.WindowTitle = "connectwatch"
	if m.width == 0 {
		return v
	}
	v.Content = lipgloss.JoinVertical(lipgloss.Left, m.header(), m.body(), m.footer())
	return v
}

func (m Model) bodyHeight() int { return max(3, m.height-2) }

func (m Model) footer() string {
	if m.confirm != nil {
		return ansi.Truncate(" "+sWarn.Render(m.confirm.question())+"  "+hints("y", "yes", "any other key", "no"), m.width, "…")
	}
	if m.filtering {
		return " " + sPink.Render("/") + sText.Render(m.filter) + sPink.Render("▏") + "  " + hints("enter", "keep", "esc", "clear")
	}
	pause := "pause"
	if m.paused {
		pause = "resume"
	}
	pairs := []string{"↑↓", "move", "tab", tabNames[(m.tab+1)%tab(len(tabNames))]}
	if !m.readOnly {
		pairs = append(pairs, "R", "restart failed", "p", "pause/resume")
	}
	pairs = append(pairs, "s", "sort by "+((m.sort+1)%3).String(), "space", pause, "r", "refresh", "ctrl+d/u", "scroll", "/", "filter", "q", "quit")
	h := hints(pairs...)
	if m.filter != "" {
		h = sPink.Render("/") + sText.Render(m.filter) + "  " + h
	}
	switch {
	case m.notice != "" && m.failed:
		h = sErr.Render(oneLine(m.notice)) + "  " + h
	case m.notice != "":
		h = sOK.Render(m.notice) + "  " + h
	case m.err != nil:
		h = sErr.Render(oneLine(m.err.Error())) + "  " + h
	}
	return ansi.Truncate(" "+h, m.width, "…")
}

func hints(pairs ...string) string {
	var parts []string
	for i := 0; i+1 < len(pairs); i += 2 {
		parts = append(parts, sKey.Render(pairs[i])+" "+sDim.Render(pairs[i+1]))
	}
	return strings.Join(parts, "   ")
}

// spread puts left and right on one line of the given width.
func spread(left, right string, width int) string {
	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		return ansi.Truncate(left, width, "…")
	}
	return left + strings.Repeat(" ", gap) + right
}

func clamp(v, lo, hi int) int {
	if hi < lo {
		return lo
	}
	return min(max(v, lo), hi)
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }
