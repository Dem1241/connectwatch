// connectwatch: watch Kafka Connect connectors in your terminal, see why a
// task failed without digging through worker logs, and restart it.
package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/Dem1241/connectwatch/internal/check"
	"github.com/Dem1241/connectwatch/internal/connect"
	"github.com/Dem1241/connectwatch/internal/demo"
	"github.com/Dem1241/connectwatch/internal/health"
	"github.com/Dem1241/connectwatch/internal/ui"
)

var version = "dev"

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "connectwatch:", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func run() (int, error) {
	addr := flag.String("url", cmp.Or(os.Getenv("CONNECT_URL"), "http://localhost:8083"), "the Kafka Connect REST API (env CONNECT_URL), basic auth goes in the url")
	flag.StringVar(addr, "u", *addr, "shorthand for -url")
	demoMode := flag.Bool("demo", false, "watch a pretend cluster with a few connectors in trouble")
	interval := flag.Duration("interval", 2*time.Second, "how often to look at the connectors")
	readOnly := flag.Bool("read-only", false, "never restart, pause or resume anything")
	flaps := flag.Int("flaps", health.DefaultLimits.Flaps, "failures within -flap-window that make a connector flapping")
	flapWindow := flag.Duration("flap-window", health.DefaultLimits.FlapWindow, "how far back failures are counted")
	checkMode := flag.Bool("check", false, "print a report and exit 0 ok, 1 warn, 2 danger, 3 unknown")
	jsonOut := flag.Bool("json", false, "with -check, print the report as json")
	showVersion := flag.Bool("version", false, "print the version")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, "usage: connectwatch [flags]\n\nWatch Kafka Connect connectors, see why a task failed, and restart it.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *showVersion {
		fmt.Println("connectwatch", buildVersion())
		return 0, nil
	}
	if flag.NArg() > 0 {
		flag.Usage()
		return 2, nil
	}
	if *flaps < 1 {
		return 2, fmt.Errorf("-flaps must be at least 1")
	}
	limits := health.DefaultLimits
	limits.Flaps, limits.FlapWindow = *flaps, *flapWindow

	var src ui.Source
	if *demoMode {
		src = demo.New(time.Now())
	} else {
		client, err := connect.New(*addr)
		if err != nil {
			return 2, err
		}
		src = client
	}

	if *checkMode {
		return runCheck(src, limits, *jsonOut, *demoMode)
	}
	_, err := tea.NewProgram(ui.New(src, limits, *interval, *readOnly)).Run()
	return 0, err
}

// runCheck looks at the connectors once and reports.
func runCheck(src ui.Source, limits health.Limits, asJSON, isDemo bool) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snap, err := src.Snapshot(ctx)
	if err != nil {
		hint := ""
		if !isDemo {
			hint = "\n(no cluster handy? try: connectwatch --demo)"
		}
		return check.ExitUnknown, fmt.Errorf("%w%s", err, hint)
	}
	sts := health.NewTracker(limits).Update(snap)
	if asJSON {
		err = check.JSON(os.Stdout, snap, sts)
	} else {
		err = check.Text(os.Stdout, snap, sts)
	}
	if err != nil {
		return check.ExitUnknown, err
	}
	return check.Code(sts), nil
}

// buildVersion falls back to the module version for go install builds, which
// don't get the release ldflags.
func buildVersion() string {
	if version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}
