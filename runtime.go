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
	pc := make([]uintptr, stackSize)

	n := runtime.Callers(skipCallers, pc)
	p := ""

	for i := 0; i < n; i++ {
		f := runtime.FuncForPC(pc[i])

		// Skip unnamed literals.
		if strings.Contains(f.Name(), "{") {
			continue
		}

		parts := strings.Split(f.Name(), "/")
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

		p = path.Base(path.Dir(f.Name())) + "/" + path.Base(f.Name())

		break
	}

	return p
}
