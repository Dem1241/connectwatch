package health

import (
	"strings"
)

// Cause is the exception at the bottom of a stack trace, the one that
// actually went wrong.
type Cause struct {
	Class   string // org.postgresql.util.PSQLException
	Message string
}

// Short is the class without its package.
func (c Cause) Short() string {
	if i := strings.LastIndexByte(c.Class, '.'); i >= 0 {
		return c.Class[i+1:]
	}
	return c.Class
}

func (c Cause) String() string {
	switch {
	case c.Class == "":
		return c.Message
	case c.Message == "":
		return c.Short()
	}
	return c.Short() + ": " + c.Message
}

// RootCause finds the innermost "Caused by" of a Java stack trace, or the
// first line when there is none. Connect wraps everything in a
// ConnectException, so the first line rarely says much.
func RootCause(trace string) Cause {
	trace = strings.TrimSpace(trace)
	if trace == "" {
		return Cause{}
	}
	lines := strings.Split(trace, "\n")
	head := 0
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "Caused by: ") {
			head = i
		}
	}
	// the message can run over several lines, until the first frame
	msg := []string{strings.TrimPrefix(strings.TrimSpace(lines[head]), "Caused by: ")}
	for _, l := range lines[head+1:] {
		t := strings.TrimSpace(l)
		if strings.HasPrefix(t, "at ") || strings.HasPrefix(t, "... ") || strings.HasPrefix(t, "Caused by: ") || strings.HasPrefix(t, "Suppressed: ") {
			break
		}
		msg = append(msg, t)
	}
	return parseCause(strings.Join(msg, " "))
}

// parseCause splits "pkg.SomeException: message" into its class and message.
func parseCause(s string) Cause {
	class, msg, found := strings.Cut(s, ": ")
	if !found {
		class, msg = s, ""
	}
	if !isClass(class) {
		return Cause{Message: strings.TrimSpace(s)}
	}
	return Cause{Class: class, Message: strings.TrimSpace(msg)}
}

func isClass(s string) bool {
	if s == "" || strings.ContainsAny(s, " \t") {
		return false
	}
	return strings.Contains(s, ".") || strings.HasSuffix(s, "Exception") || strings.HasSuffix(s, "Error")
}
