package dbwrap

import (
	"path"
	"runtime"
	"strings"
)

const (
	skipCallers = 6
	stackSize   = 30
)

// Caller returns name and package of closest parent function
// that does not belong to skipped packages.
//
// For example the result could be
//    pressly/goose.MySQLDialect.dbVersionQuery
func Caller(skipPackages ...string) string {
	p := ""
	pc := make([]uintptr, stackSize)

	runtime.Callers(skipCallers, pc)

	frames := runtime.CallersFrames(pc)

	for {
		frame, more := frames.Next()

		if !more {
			break
		}

		fn := frame.Function

		// Skip unnamed literals.
		if fn == "" || strings.Contains(fn, "{") {
			continue
		}

		parts := strings.Split(fn, "/")
		parts[len(parts)-1] = strings.Split(parts[len(parts)-1], ".")[0]
		p = strings.Join(parts, "/")

		if p == "database/sql" || p == "github.com/bool64/dbwrap" {
			continue
		}

		skip := false

		for _, sp := range skipPackages {
			if p == sp {
				skip = true

				break
			}
		}

		if skip {
			continue
		}

		p = path.Base(path.Dir(fn)) + "/" + path.Base(fn)

		break
	}

	return p
}
