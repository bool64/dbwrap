package dbwrap_test

import (
	"testing"

	"github.com/bool64/dbwrap"
)

func BenchmarkCaller(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = dbwrap.Caller("database/sql", "abc", "def")
	}
}
