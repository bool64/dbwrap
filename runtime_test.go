package dbwrap_test

import (
	"context"
	"testing"

	"github.com/bool64/dbwrap"
	"github.com/stretchr/testify/assert"
)

func BenchmarkCaller(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = dbwrap.Caller("database/sql", "abc", "def")
	}
}

func TestCallerCtx(t *testing.T) {
	ctx := dbwrap.WithCaller(context.Background(), "test")

	assert.Equal(t, "test", dbwrap.CallerCtx(ctx, "abc"))
}
