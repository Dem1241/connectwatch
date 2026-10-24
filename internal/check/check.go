// Package check is connectwatch without the interface: one look at the
// connectors, a report, and an exit code that cron, CI or a Kubernetes probe
// can act on.
package check

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/Dem1241/connectwatch/internal/connect"
	"github.com/Dem1241/connectwatch/internal/health"
)

// Exit codes, in the style of monitoring plugins.
const (
	ExitOK      = 0
	ExitWarn    = 1
	ExitDanger  = 2
	ExitUnknown = 3
)

// Code is the exit code for a set of statuses: the worst level wins.
func Code(sts []health.Status) int {
	worst := health.OK
	for _, st := range sts {
		worst = max(worst, st.Level)
	}
	switch worst {
	case health.Danger:
		return ExitDanger
	case health.Warn:
		return ExitWarn
	}
	return ExitOK
}

// Text writes a table of the connectors, with the reasons and the root cause
// under any that aren't ok.
func Text(w io.Writer, snap connect.Snapshot, sts []health.Status) error {
	if len(sts) == 0 {
		_, err := fmt.Fprintf(w, "no connectors on %s\n", snap.Cluster.Name)
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "LEVEL\tCONNECTOR\tTYPE\tCLASS\tSTATE\tTASKS")
	for _, st := range sts {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", st.Level, st.Name, orDash(st.Type), orDash(health.ShortClass(st.Class)), orDash(st.State), tasks(st))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, st := range sts {
		if st.Level == health.OK {
			continue
		}
		fmt.Fprintf(w, "\n%s %s\n", st.Level, st.Name)
		for _, r := range st.Reasons {
			fmt.Fprintf(w, "  - %s\n", r)
		}
		if st.Cause != (health.Cause{}) {
			fmt.Fprintf(w, "  %s: %s\n", st.CauseFrom, st.Cause)
		}
	}
	return nil
}

// tasks is like "2/3 running".
func tasks(st health.Status) string {
	if len(st.Tasks) == 0 {
		return "-"
	}
	return fmt.Sprintf("%d/%d running", st.Running, len(st.Tasks))
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

type taskJSON struct {
	ID     int    `json:"id"`
	State  string `json:"state"`
	Worker string `json:"worker_id,omitempty"`
}

// connectorJSON is how a connector looks in --json output.
type connectorJSON struct {
	Name      string     `json:"name"`
	Level     string     `json:"level"`
	Reasons   []string   `json:"reasons"`
	Type      string     `json:"type,omitempty"`
	Class     string     `json:"class,omitempty"`
	State     string     `json:"state"`
	Worker    string     `json:"worker_id,omitempty"`
	Tasks     []taskJSON `json:"tasks"`
	Cause     string     `json:"cause,omitempty"`
	CauseFrom string     `json:"cause_from,omitempty"`
	Advice    string     `json:"advice,omitempty"`
}

// JSON writes the connectors and the cluster they were read from. Configs
// are left out, they tend to hold passwords.
func JSON(w io.Writer, snap connect.Snapshot, sts []health.Status) error {
	out := struct {
		Cluster    string          `json:"cluster"`
		Version    string          `json:"version,omitempty"`
		ClusterID  string          `json:"kafka_cluster_id,omitempty"`
		CheckedAt  time.Time       `json:"checked_at"`
		Level      string          `json:"level"`
		Connectors []connectorJSON `json:"connectors"`
	}{
		Cluster:    snap.Cluster.Name,
		Version:    snap.Cluster.Version,
		ClusterID:  snap.Cluster.ClusterID,
		CheckedAt:  snap.At,
		Level:      levelOf(Code(sts)),
		Connectors: []connectorJSON{},
	}
	for _, st := range sts {
		c := connectorJSON{
			Name: st.Name, Level: st.Level.String(), Reasons: st.Reasons, Type: st.Type, Class: st.Class,
			State: st.State, Worker: st.Worker, Tasks: []taskJSON{}, CauseFrom: st.CauseFrom, Advice: st.Advice,
		}
		if st.Cause != (health.Cause{}) {
			c.Cause = st.Cause.Class
			if st.Cause.Message != "" {
				c.Cause += ": " + st.Cause.Message
			}
		}
		if c.Reasons == nil {
			c.Reasons = []string{}
		}
		for _, t := range st.Tasks {
			c.Tasks = append(c.Tasks, taskJSON{t.ID, t.State, t.Worker})
		}
		out.Connectors = append(out.Connectors, c)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func levelOf(code int) string {
	return []string{"ok", "warn", "danger", "unknown"}[code]
}
