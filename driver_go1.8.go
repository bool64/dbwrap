//go:build !go1.9
// +build !go1.9

package dbwrap

import (
	"context"
	"database/sql/driver"
	"errors"
)

// Dummy error for setSpanStatus (does exist as sql.ErrConnDone in 1.9+)
var errConnDone = errors.New("database/sql: connection is already closed")

// wDriver implements driver.Driver
type wDriver struct {
	parent  driver.Driver
	options Options
}

func wrapDriver(d driver.Driver, o Options) driver.Driver {
	return wDriver{parent: d, options: o}
}

func wrapConn(c driver.Conn, options Options) driver.Conn {
	return &wConn{parent: c, options: options}
}

func wrapStmt(ctx context.Context, stmt driver.Stmt, query string, options Options) driver.Stmt {
	s := wStmt{ctx: ctx, parent: stmt, query: query, options: options}
	_, hasExeCtx := stmt.(driver.StmtExecContext)
	_, hasQryCtx := stmt.(driver.StmtQueryContext)
	c, hasColCnv := stmt.(driver.ColumnConverter)
	switch {
	case !hasExeCtx && !hasQryCtx && !hasColCnv:
		return struct {
			driver.Stmt
		}{s}
	case !hasExeCtx && hasQryCtx && !hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
		}{s, s}
	case hasExeCtx && !hasQryCtx && !hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtExecContext
		}{s, s}
	case hasExeCtx && hasQryCtx && !hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
		}{s, s, s}
	case !hasExeCtx && !hasQryCtx && hasColCnv:
		return struct {
			driver.Stmt
			driver.ColumnConverter
		}{s, c}
	case !hasExeCtx && hasQryCtx && hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && !hasQryCtx && hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && hasQryCtx && hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, s, c}
	}
	panic("unreachable")
}
