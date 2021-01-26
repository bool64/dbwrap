// +build go1.10

package dbwrap

import (
	"context"
	"database/sql/driver"
)

// Compile time assertion.
var (
	_ driver.DriverContext = &wDriver{}
	_ driver.Connector     = &wDriver{}
)

// WrapConnector allows wrapping a database driver.Connector which eliminates
// the need to register wrap as an available driver.Driver.
func WrapConnector(dc driver.Connector, options ...Option) driver.Connector {
	if o, ok := prepareOptions(options); ok {
		return &wDriver{
			parent:    dc.Driver(),
			connector: dc,
			options:   o,
		}
	}

	return dc
}

// wDriver implements driver.Driver.
type wDriver struct {
	parent    driver.Driver
	connector driver.Connector
	options   Options
}

func wrapDriver(d driver.Driver, o Options) driver.Driver {
	if _, ok := d.(driver.DriverContext); ok {
		return wDriver{parent: d, options: o}
	}

	return struct{ driver.Driver }{wDriver{parent: d, options: o}}
}

func wrapConn(parent driver.Conn, options Options) driver.Conn {
	var (
		n, hasNameValueChecker = parent.(driver.NamedValueChecker)
		s, hasSessionResetter  = parent.(driver.SessionResetter)
	)

	c := &wConn{parent: parent, options: options}

	switch {
	case !hasNameValueChecker && !hasSessionResetter:
		return c
	case hasNameValueChecker && !hasSessionResetter:
		return struct {
			conn
			driver.NamedValueChecker
		}{c, n}
	case !hasNameValueChecker && hasSessionResetter:
		return struct {
			conn
			driver.SessionResetter
		}{c, s}
	case hasNameValueChecker && hasSessionResetter:
		return struct {
			conn
			driver.NamedValueChecker
			driver.SessionResetter
		}{c, n, s}
	}

	panic("unreachable")
}

// nolint:funlen,gocyclo // Large switch is necessary to combine a variety of traits.
func wrapStmt(ctx context.Context, stmt driver.Stmt, query string, options Options) driver.Stmt {
	var (
		_, hasExeCtx    = stmt.(driver.StmtExecContext)
		_, hasQryCtx    = stmt.(driver.StmtQueryContext)
		c, hasColConv   = stmt.(driver.ColumnConverter) // nolint:staticcheck // Deprecated usage for backwards compatibility.
		n, hasNamValChk = stmt.(driver.NamedValueChecker)
	)

	s := wStmt{ctx: ctx, parent: stmt, query: query, options: options}

	switch {
	case !hasExeCtx && !hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
		}{s}
	case !hasExeCtx && hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
		}{s, s}
	case hasExeCtx && !hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
		}{s, s}
	case hasExeCtx && hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
		}{s, s, s}
	case !hasExeCtx && !hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.ColumnConverter
		}{s, c}
	case !hasExeCtx && hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && !hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, s, c}

	case !hasExeCtx && !hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.NamedValueChecker
		}{s, n}
	case !hasExeCtx && hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.NamedValueChecker
		}{s, s, n}
	case hasExeCtx && !hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.NamedValueChecker
		}{s, s, n}
	case hasExeCtx && hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.NamedValueChecker
		}{s, s, s, n}
	case !hasExeCtx && !hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, c, n}
	case !hasExeCtx && hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, c, n}
	case hasExeCtx && !hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, c, n}
	case hasExeCtx && hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, s, c, n}
	}

	panic("unreachable")
}

func (d wDriver) OpenConnector(name string) (driver.Connector, error) {
	var err error

	d.connector, err = d.parent.(driver.DriverContext).OpenConnector(name)
	if err != nil {
		return nil, err
	}

	return d, err
}

func (d wDriver) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := d.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return &wConn{parent: c, options: d.options}, nil
}

func (d wDriver) Driver() driver.Driver {
	return d
}
