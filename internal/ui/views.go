package ui

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/Dem1241/connectwatch/internal/connect"
	"github.com/Dem1241/connectwatch/internal/health"
)

func (m Model) header() string {
	left := " " + sTitle.Render("⋈ connectwatch")
	if m.cluster.Name != "" {
		left += sDim.Render(" · ") + sSub.Render(m.cluster.Name)
		if m.cluster.Version != "" {
			left += sDim.Render(" · ") + sSub.Render("kafka "+m.cluster.Version)
		}
		if m.readOnly {
			left += sDim.Render(" · read-only")
		}
	}

	var right string
	switch {
	case m.paused:
		right = sDim.Render("❚❚ paused")
	case m.err != nil:
		right = sErr.Render("○ can't reach connect")
	default:
		right = sOK.Render("● live")
	}
	if !m.updated.IsZero() {
		right += sDim.Render("  " + m.updated.Format("15:04:05"))
	}
	return spread(left, right+" ", m.width)
}

// sideBySide is whether the list and the detail pane fit next to each other.
func (m Model) sideBySide() bool { return m.width >= 110 }

func (m Model) body() string {
	h := m.bodyHeight()
	listW, listH, detW, detH := m.width, h*45/100, m.width, h-h*45/100
	if m.sideBySide() {
		listW = m.width * 48 / 100
		listH, detW, detH = h, m.width-listW, h
	}

	sts := m.visible()
	rows := listH - 4
	inner := listW - 4
	var lines []string
	lines = append(lines, m.summary(inner), "")
	switch {
	case m.loading && len(m.statuses) == 0 && m.err == nil:
		lines = append(lines, sDim.Render("asking connect…"))
	case m.err != nil && len(m.statuses) == 0:
		lines = append(lines, sErr.Render("couldn't read the connectors"), sSub.Render(oneLine(m.err.Error())))
	case len(m.statuses) == 0:
		lines = append(lines, sDim.Render("no connectors yet"), sDim.Render("they show up here as soon as one is created"))
	case len(sts) == 0:
		lines = append(lines, sDim.Render("nothing matches"))
	}
	start := clamp(m.idx-rows/2, 0, max(0, len(sts)-rows))
	for i := start; i < len(sts) && i < start+rows; i++ {
		lines = append(lines, row(sts[i], i == m.idx, inner))
	}
	list := sPaneOn.Width(listW).Height(listH).Render(strings.Join(fit(lines, listH-2), "\n"))

	inner = detW - 4
	detail := []string{m.tabs(inner), ""}
	if st, ok := m.selected(); ok {
		switch m.tab {
		case traceTab:
			detail = append(detail, traceLines(st, inner)...)
		case configTab:
			detail = append(detail, configLines(st.Config, inner)...)
		default:
			detail = append(detail, detailLines(st, m.readOnly, inner)...)
		}
	}
	// the tabs stay put while the rest scrolls
	body := detail[2:]
	top := clamp(m.detailTop, 0, max(0, len(body)-(detH-4)))
	detail = append(detail[:2:2], body[min(top, len(body)):]...)
	det := sPane.Width(detW).Height(detH).Render(strings.Join(fit(detail, detH-2), "\n"))

	if m.sideBySide() {
		return lipgloss.JoinHorizontal(lipgloss.Top, list, det)
	}
	return lipgloss.JoinVertical(lipgloss.Left, list, det)
}

func (m Model) tabs(width int) string {
	var parts []string
	for i, name := range tabNames {
		if tab(i) == m.tab {
			parts = append(parts, sTab.Render(name))
		} else {
			parts = append(parts, sTabOff.Render(name))
		}
	}
	return ansi.Truncate(strings.Join(parts, " "), width, "…")
}

// summary is the line above the connectors: how many there are and how
// many need a look.
func (m Model) summary(width int) string {
	if len(m.statuses) == 0 {
		return sDim.Render("0 connectors")
	}
	tasks, failing, watch, paused := 0, 0, 0, 0
	for _, st := range m.statuses {
		tasks += len(st.Tasks)
		switch st.Level {
		case health.Danger:
			failing++
		case health.Warn:
			watch++
		}
		if st.State == connect.Paused || st.State == connect.Stopped {
			paused++
		}
	}
	s := sText.Render(fmt.Sprintf("%d connector%s", len(m.statuses), plural(len(m.statuses)))) +
		sSub.Render(fmt.Sprintf(", %d task%s", tasks, plural(tasks)))
	var bits []string
	if failing > 0 {
		bits = append(bits, sDanger.Render(fmt.Sprintf("%d failing", failing)))
	}
	if watch > 0 {
		bits = append(bits, sWarn.Render(fmt.Sprintf("%d to watch", watch)))
	}
	if paused > 0 {
		bits = append(bits, sDim.Render(fmt.Sprintf("%d paused", paused)))
	}
	if failing+watch == 0 {
		bits = append([]string{sOK.Render("all good")}, bits...)
	}
	s += sDim.Render(" · ") + strings.Join(bits, sDim.Render(", "))
	return ansi.Truncate(s, width, "…")
}

// row is one line of the connector list.
func row(st health.Status, selected bool, width int) string {
	marker := "  "
	name := sText.Render(st.Name)
	if selected {
		marker = sPink.Render("▌ ")
		name = sTitle.Render(st.Name)
	}
	right := taskDots(st.Tasks, 8)
	if len(st.Tasks) > 0 {
		right += sDim.Render(fmt.Sprintf(" %d/%d", st.Running, len(st.Tasks)))
	}
	left := marker + badge(st) + " " + name + sDim.Render(" "+health.ShortClass(st.Class))
	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		// drop the class before the tasks
		left = marker + badge(st) + " " + name
		gap = width - lipgloss.Width(left) - lipgloss.Width(right)
		if gap < 1 {
			return ansi.Truncate(left, width, "…")
		}
	}
	return left + strings.Repeat(" ", gap) + right
}

// detailLines describes one connector: what it is, how its tasks are doing,
// why it was flagged and what can be done about it.
func detailLines(st health.Status, readOnly bool, width int) []string {
	var lines []string
	add := func(s string) { lines = append(lines, ansi.Truncate(s, width, "…")) }
	field := func(label, value string) {
		add(sLabel.Render(pad(label, 13)) + value)
	}
	para := func(style lipgloss.Style, indent, s string) {
		for _, l := range wrap(s, width-lipgloss.Width(indent)) {
			add(indent + style.Render(l))
		}
	}

	add(sTitle.Render(st.Name) + "  " + badge(st))
	add("")

	kind := st.Type
	if c := health.ShortClass(st.Class); c != "" {
		if kind != "" {
			kind += sDim.Render(" · ")
		}
		kind += sClass.Render(c)
	}
	field("type", sText.Render(kind))
	if st.Class != "" {
		field("class", sDim.Render(st.Class))
	}
	if st.Worker != "" {
		field("worker", sText.Render(st.Worker))
	}
	if t := topics(st.Config); t != "" {
		field("topics", sText.Render(t))
	}
	if st.FailedFor > 0 {
		field("failing for", levelStyle(st.Level).Render(health.Duration(st.FailedFor)))
	}
	if st.Failures > 1 {
		field("failures", levelStyle(st.Level).Render(fmt.Sprintf("%d recently", st.Failures)))
	}

	if len(st.Tasks) > 0 {
		add("")
		add(sTitle.Render(fmt.Sprintf("tasks · %d/%d running", st.Running, len(st.Tasks))))
		for _, t := range st.Tasks {
			state := lipgloss.NewStyle().Foreground(stateColor(t.State)).Render(pad(strings.ToLower(t.State), 11))
			l := sDim.Render(fmt.Sprintf("#%-3d", t.ID)) + state + sSub.Render(t.Worker)
			if t.State == connect.Failed {
				if c := health.RootCause(t.Trace); c.Class != "" {
					l += sDim.Render("  " + c.Short())
				}
			}
			add(l)
		}
	}

	if len(st.Reasons) > 0 {
		add("")
		add(sTitle.Render("why it's flagged"))
		for _, r := range st.Reasons {
			add(levelStyle(st.Level).Render("• ") + sText.Render(r))
		}
	}
	if st.Cause != (health.Cause{}) {
		add("")
		add(sTitle.Render("what went wrong") + sDim.Render(" · "+st.CauseFrom))
		if st.Cause.Class != "" {
			add(sDanger.Render(st.Cause.Short()))
		}
		if st.Cause.Message != "" {
			para(sText, "", st.Cause.Message)
		}
	}

	var todo []string
	if st.Advice != "" {
		todo = append(todo, st.Advice)
	}
	if st.Level == health.Danger && health.IsPostgresSource(st.Class) {
		slot := st.Config["slot.name"]
		if slot == "" {
			slot = "debezium"
		}
		todo = append(todo, "while it's down, the replication slot "+slot+" holds on to WAL on the database. slotwatch shows how much.")
	}
	if len(todo) > 0 || (st.Level == health.Danger && !readOnly) {
		add("")
		add(sTitle.Render("what you can do"))
		for _, t := range todo {
			para(sSub, "", t)
		}
		if !readOnly && (st.State == connect.Failed || st.Failed > 0) {
			add(sKey.Render("R") + sSub.Render(" restarts what failed. ") + sKey.Render("tab") + sSub.Render(" shows the whole trace."))
		}
	}
	return lines
}

// topics is where a connector reads from or writes to, as far as its config
// says.
func topics(cfg map[string]string) string {
	switch {
	case cfg["topics"] != "":
		return strings.ReplaceAll(cfg["topics"], ",", ", ")
	case cfg["topics.regex"] != "":
		return cfg["topics.regex"]
	case cfg["topic.prefix"] != "":
		return cfg["topic.prefix"] + ".*"
	}
	return ""
}

// traceLines is the stack trace of the failure, with the exceptions picked
// out from the frames.
func traceLines(st health.Status, width int) []string {
	trace := st.Trace
	from := "connector"
	if st.State != connect.Failed || trace == "" {
		trace, from = "", ""
		for _, t := range st.Tasks {
			if t.State == connect.Failed && t.Trace != "" {
				trace, from = t.Trace, fmt.Sprintf("task %d", t.ID)
				break
			}
		}
	}
	if trace == "" {
		return []string{sOK.Render("nothing failed"), sDim.Render("connect only keeps a trace while something is in FAILED")}
	}
	lines := []string{sDim.Render("trace of " + from)}
	for _, l := range strings.Split(strings.TrimRight(trace, "\n"), "\n") {
		t := strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(t, "at ") || strings.HasPrefix(t, "... "):
			lines = append(lines, ansi.Truncate("  "+sDim.Render(t), width, "…"))
		case strings.HasPrefix(t, "Caused by: "):
			lines = append(lines, "")
			for _, w := range wrap(t, width) {
				lines = append(lines, sDanger.Render(w))
			}
		default:
			for _, w := range wrap(t, width) {
				lines = append(lines, sWarn.Render(w))
			}
		}
	}
	return lines
}

// secret is a config key whose value connectwatch doesn't show.
var secret = regexp.MustCompile(`(?i)password|secret|token|credential|jaas|api\.key|private|access\.key|keystore\.key|\.key$`)

// configLines is the connector's config, sorted, with secrets masked.
func configLines(cfg map[string]string, width int) []string {
	if len(cfg) == 0 {
		return []string{sDim.Render("no config")}
	}
	keys := make([]string, 0, len(cfg))
	w := 0
	for k := range cfg {
		keys = append(keys, k)
		w = max(w, len(k))
	}
	slices.Sort(keys)
	w = min(w, width/2)
	var lines []string
	for _, k := range keys {
		v := sText.Render(cfg[k])
		if secret.MatchString(k) && !strings.HasPrefix(cfg[k], "${") {
			v = sDim.Render("••••••")
		}
		lines = append(lines, ansi.Truncate(sLabel.Render(pad(k, w))+"  "+v, width, "…"))
	}
	return lines
}

// wrap breaks text into lines of at most width.
func wrap(s string, width int) []string {
	if width <= 0 || lipgloss.Width(s) <= width {
		return []string{s}
	}
	var lines []string
	line := ""
	for _, w := range strings.Fields(s) {
		for lipgloss.Width(w) > width {
			if line != "" {
				lines = append(lines, line)
				line = ""
			}
			lines = append(lines, w[:width])
			w = w[width:]
		}
		if line != "" && lipgloss.Width(line)+1+lipgloss.Width(w) > width {
			lines = append(lines, line)
			line = w
			continue
		}
		if line != "" {
			line += " "
		}
		line += w
	}
	return append(lines, line)
}

func pad(s string, w int) string {
	return s + strings.Repeat(" ", max(0, w-lipgloss.Width(s)))
}

// fit makes lines exactly n long, so panes keep their size.
func fit(lines []string, n int) []string {
	if len(lines) > n {
		return lines[:n]
	}
	for len(lines) < n {
		lines = append(lines, "")
	}
	return lines
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
