package dbwrap

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strconv"
	"sync"
)

// Operation enumerates SQL operations.
type Operation string

// These constants enumerate available SQL operations.
const (
	Ping         = Operation("ping")
	Exec         = Operation("exec")
	Query        = Operation("query")
	Prepare      = Operation("prepare")
	Begin        = Operation("begin")
	LastInsertID = Operation("last_insert_id")
	RowsAffected = Operation("rows_affected")
	StmtExec     = Operation("stmt_exec")
	StmtQuery    = Operation("stmt_query")
	StmtClose    = Operation("stmt_close")
	RowsClose    = Operation("rows_close")
	RowsNext     = Operation("rows_next")
	Commit       = Operation("commit")
	Rollback     = Operation("rollback")
)

var defaultOperations = map[Operation]bool{
	Exec:         true,
	Query:        true,
	Prepare:      true,
	Begin:        true,
	LastInsertID: true,
	RowsAffected: true,
	StmtExec:     true,
	StmtQuery:    true,
	StmtClose:    true,
	RowsClose:    true,
	Commit:       true,
	Rollback:     true,
}

type conn interface {
	driver.Pinger
	driver.Execer //nolint:staticcheck // Deprecated usage for backwards compatibility.
	driver.ExecerContext
	driver.Queryer //nolint:staticcheck // Deprecated usage for backwards compatibility.
	driver.QueryerContext
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
}

var (
	regMu sync.Mutex

	// Compile time assertions.
	_ driver.Driver                         = &wDriver{}
	_ conn                                  = &wConn{}
	_ driver.NamedValueChecker              = &wConn{}
	_ driver.Result                         = &wResult{}
	_ driver.Stmt                           = &wStmt{}
	_ driver.StmtExecContext                = &wStmt{}
	_ driver.StmtQueryContext               = &wStmt{}
	_ driver.Rows                           = &wRows{}
	_ driver.RowsNextResultSet              = &wRows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &wRows{}
	_ driver.RowsColumnTypeLength           = &wRows{}
	_ driver.RowsColumnTypeNullable         = &wRows{}
	_ driver.RowsColumnTypePrecisionScale   = &wRows{}
)

// Register initializes and registers our wrapped database driver
// identified by its driverName and using provided Options. On success it
// returns the generated driverName to use when calling sql.Open.
// It is possible to register multiple wrappers for the same database driver if
// needing different Options for different connections.
func Register(driverName string, options ...Option) (string, error) {
	return RegisterWithSource(driverName, "", options...)
}

// RegisterWithSource initializes and registers our wrapped database driver
// identified by its driverName, using provided Options.
// source is useful if some drivers do not accept the empty string when opening the DB.
// On success it returns the generated driverName to use when calling sql.Open.
// It is possible to register multiple wrappers for the same database driver if
// needing different Options for different connections.
func RegisterWithSource(driverName string, source string, options ...Option) (string, error) {
	// retrieve the driver implementation we need to wrap with instrumentation
	db, err := sql.Open(driverName, source)
	if err != nil {
		return "", err
	}

	dri := db.Driver()

	if err = db.Close(); err != nil {
		return "", err
	}

	regMu.Lock()
	defer regMu.Unlock()

	// Since we might want to register multiple drivers to have different
	// Options, but potentially the same underlying database driver, we
	// cycle through to find available driver names.
	driverName += "-wrap-"

	for i := int64(0); i < 100; i++ {
		var (
			found   = false
			regName = driverName + strconv.FormatInt(i, 10)
		)

		for _, name := range sql.Drivers() {
			if name == regName {
				found = true
			}
		}

		if !found {
			sql.Register(regName, Wrap(dri, options...))

			return regName, nil
		}
	}

	return "", errors.New("unable to register driver, all slots have been taken")
}

// Wrap takes a SQL driver and wraps it with middlewares.
func Wrap(d driver.Driver, options ...Option) driver.Driver {
	if o, ok := prepareOptions(options); ok {
		return wrapDriver(d, o)
	}

	return d
}

// Open implements driver.Driver.
func (d wDriver) Open(name string) (driver.Conn, error) {
	c, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}

	return wrapConn(c, d.options), nil
}

// WrapConn allows an existing driver.Conn to be wrapped.
func WrapConn(c driver.Conn, options ...Option) driver.Conn {
	if o, ok := prepareOptions(options); ok {
		return wrapConn(c, o)
	}

	return c
}

// wConn implements driver.Conn.
type wConn struct {
	parent  driver.Conn
	options Options
}

func apply(
	ctx context.Context,
	mws []Middleware,
	operation Operation,
	statement string,
	args []driver.NamedValue,
) (context.Context, []func(error)) {
	finalizers := make([]func(error), len(mws))
	n := len(mws)

	for i, mw := range mws {
		newCtx, onFinish := mw(ctx, operation, statement, args)
		ctx = newCtx

		if onFinish == nil {
			onFinish = func(err error) {}
		}

		finalizers[n-i-1] = onFinish
	}

	return ctx, finalizers
}

func namedValues(args []driver.Value) []driver.NamedValue {
	var nargs []driver.NamedValue
	if len(args) > 0 {
		nargs = make([]driver.NamedValue, 0, len(args))

		for _, a := range args {
			nargs = append(nargs, driver.NamedValue{Value: a})
		}
	}

	return nargs
}

func values(nargs []driver.NamedValue) []driver.Value {
	var args []driver.Value
	if len(nargs) > 0 {
		args = make([]driver.Value, 0, len(nargs))

		for _, a := range nargs {
			args = append(args, a.Value)
		}
	}

	return args
}

func (c wConn) Ping(ctx context.Context) (err error) {
	if c.options.operations[Ping] {
		newCtx, finalizers := apply(ctx, c.options.Middlewares, Ping, "", nil)
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	if pinger, ok := c.parent.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}

	return errors.New("driver does not implement Ping")
}

func (c wConn) Exec(query string, args []driver.Value) (res driver.Result, err error) {
	ctx := context.Background()

	//nolint:staticcheck // Deprecated usage for backwards compatibility.
	exec, ok := c.parent.(driver.Execer)

	if !ok {
		return nil, driver.ErrSkip
	}

	if c.options.Intercept != nil {
		nctx, nquery, nargs := c.options.Intercept(ctx, Exec, query, namedValues(args))
		ctx = nctx
		args = values(nargs)
		query = nquery
	}

	if c.options.operations[Exec] {
		newCtx, finalizers := apply(ctx, c.options.Middlewares, Exec, query, namedValues(args))
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	if res, err = exec.Exec(query, args); err != nil {
		return nil, err
	}

	return wResult{parent: res, ctx: ctx, options: c.options}, nil
}

func (c wConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (res driver.Result, err error) {
	execCtx, ok := c.parent.(driver.ExecerContext)

	if !ok {
		return nil, driver.ErrSkip
	}

	if c.options.Intercept != nil {
		ctx, query, args = c.options.Intercept(ctx, Exec, query, args)
	}

	if c.options.operations[Exec] {
		newCtx, finalizers := apply(ctx, c.options.Middlewares, Exec, query, args)
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	if res, err = execCtx.ExecContext(ctx, query, args); err != nil {
		return nil, err
	}

	return wResult{parent: res, ctx: ctx, options: c.options}, nil
}

func (c wConn) Query(query string, args []driver.Value) (rows driver.Rows, err error) {
	//nolint:staticcheck // Deprecated usage for backwards compatibility.
	queryer, ok := c.parent.(driver.Queryer)

	if !ok {
		return nil, driver.ErrSkip
	}

	ctx := context.Background()

	if c.options.Intercept != nil {
		nctx, nquery, nargs := c.options.Intercept(ctx, Query, query, namedValues(args))
		ctx = nctx
		query = nquery
		args = values(nargs)
	}

	if c.options.operations[Query] {
		newCtx, finalizers := apply(ctx, c.options.Middlewares, Query, query, namedValues(args))
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	rows, err = queryer.Query(query, args)
	if err != nil {
		return nil, err
	}

	return wrapRows(ctx, rows, c.options), nil
}

func (c wConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	queryerCtx, ok := c.parent.(driver.QueryerContext)

	if !ok {
		return nil, driver.ErrSkip
	}

	if c.options.Intercept != nil {
		ctx, query, args = c.options.Intercept(ctx, Query, query, args)
	}

	if c.options.operations[Query] {
		newCtx, finalizers := apply(ctx, c.options.Middlewares, Query, query, args)
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	rows, err = queryerCtx.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}

	return wrapRows(ctx, rows, c.options), nil
}

func (c wConn) Prepare(query string) (stmt driver.Stmt, err error) {
	ctx := context.Background()

	if c.options.Intercept != nil {
		ctx, query, _ = c.options.Intercept(ctx, Prepare, query, nil)
	}

	if c.options.operations[Prepare] {
		newCtx, finalizers := apply(ctx, c.options.Middlewares, Prepare, query, nil)
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	stmt, err = c.parent.Prepare(query)
	if err != nil {
		return nil, err
	}

	return wrapStmt(ctx, stmt, query, c.options), nil
}

func (c *wConn) Close() error {
	return c.parent.Close()
}

func (c *wConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *wConn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	if c.options.Intercept != nil {
		ctx, query, _ = c.options.Intercept(ctx, Prepare, query, nil)
	}

	if c.options.operations[Prepare] {
		newCtx, finalizers := apply(ctx, c.options.Middlewares, Prepare, query, nil)
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	if prepCtx, ok := c.parent.(driver.ConnPrepareContext); ok {
		if stmt, err = prepCtx.PrepareContext(ctx, query); err != nil {
			return nil, err
		}
	}

	return wrapStmt(ctx, stmt, query, c.options), nil
}

func (c *wConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	if c.options.operations[Begin] {
		newCtx, finalizers := apply(ctx, c.options.Middlewares, Begin, "", nil)
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	if connBeginTx, ok := c.parent.(driver.ConnBeginTx); ok {
		tx, err = connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		return wTx{parent: tx, ctx: ctx, options: c.options}, nil
	}

	tx, err = c.parent.Begin() //nolint:staticcheck // Deprecated usage for backwards compatibility.
	if err != nil {
		return nil, err
	}

	return wTx{parent: tx, ctx: ctx, options: c.options}, nil
}

func (c *wConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nvc, ok := c.parent.(driver.NamedValueChecker)
	if ok {
		return nvc.CheckNamedValue(nv)
	}

	nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)

	return err
}

// wResult implements driver.Result.
type wResult struct {
	parent  driver.Result
	ctx     context.Context
	options Options
}

func (r wResult) LastInsertId() (id int64, err error) {
	if r.options.operations[LastInsertID] {
		_, finalizers := apply(r.ctx, r.options.Middlewares, LastInsertID, "", nil)

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	id, err = r.parent.LastInsertId()

	return
}

func (r wResult) RowsAffected() (cnt int64, err error) {
	if r.options.operations[RowsAffected] {
		_, finalizers := apply(r.ctx, r.options.Middlewares, RowsAffected, "", nil)

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	return r.parent.RowsAffected()
}

// wStmt implements driver.Stmt.
type wStmt struct {
	ctx     context.Context
	parent  driver.Stmt
	query   string
	options Options
}

func (s wStmt) Exec(args []driver.Value) (res driver.Result, err error) {
	if s.options.Intercept != nil {
		ctx, _, nargs := s.options.Intercept(s.ctx, StmtExec, s.query, namedValues(args))
		s.ctx = ctx
		args = values(nargs)
	}

	if s.options.operations[StmtExec] {
		newCtx, finalizers := apply(s.ctx, s.options.Middlewares, StmtExec, s.query, namedValues(args))
		s.ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	res, err = s.parent.Exec(args) //nolint:staticcheck // Deprecated usage for backwards compatibility.
	if err != nil {
		return nil, err
	}

	return wResult{parent: res, ctx: s.ctx, options: s.options}, nil
}

func (s wStmt) Close() (err error) {
	if s.options.operations[StmtClose] {
		_, finalizers := apply(s.ctx, s.options.Middlewares, StmtClose, s.query, nil)

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	return s.parent.Close()
}

func (s wStmt) NumInput() int {
	// NumInput may also return -1, if the driver doesn't know
	// its number of placeholders. In that case, the sql package
	// will not sanity check Exec or Query argument counts.
	if s.parent == nil {
		return -1
	}

	return s.parent.NumInput()
}

func (s wStmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	if s.options.Intercept != nil {
		ctx, _, nargs := s.options.Intercept(s.ctx, StmtQuery, s.query, namedValues(args))
		s.ctx = ctx
		args = values(nargs)
	}

	if s.options.operations[StmtQuery] {
		newCtx, finalizers := apply(s.ctx, s.options.Middlewares, StmtQuery, s.query, namedValues(args))
		s.ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	rows, err = s.parent.Query(args) //nolint:staticcheck // Deprecated usage for backwards compatibility.
	if err != nil {
		return nil, err
	}

	return wrapRows(s.ctx, rows, s.options), nil
}

func (s wStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	if s.options.Intercept != nil {
		ctx, _, args = s.options.Intercept(s.ctx, StmtExec, s.query, args)
	}

	if s.options.operations[StmtExec] {
		newCtx, finalizers := apply(ctx, s.options.Middlewares, StmtExec, s.query, args)
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	execContext, ok := s.parent.(driver.StmtExecContext)
	if !ok {
		return nil, errors.New("driver does not implement ExecContext")
	}

	res, err = execContext.ExecContext(ctx, args)
	if err != nil {
		return nil, err
	}

	return wResult{parent: res, ctx: ctx, options: s.options}, nil
}

func (s wStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	if s.options.Intercept != nil {
		ctx, _, args = s.options.Intercept(ctx, StmtQuery, s.query, args)
	}

	if s.options.operations[StmtQuery] {
		newCtx, finalizers := apply(ctx, s.options.Middlewares, StmtQuery, s.query, args)
		ctx = newCtx

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	queryContext, ok := s.parent.(driver.StmtQueryContext)
	if !ok {
		if !ok {
			return nil, errors.New("driver does not implement QueryContext")
		}
	}

	rows, err = queryContext.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}

	return wrapRows(ctx, rows, s.options), nil
}

// withRowsColumnTypeScanType is the same as the driver.RowsColumnTypeScanType
// interface except it omits the driver.Rows embedded interface.
// If the original driver.Rows wrapped implementation supports
// RowsColumnTypeScanType we enable the original method implementation in the
// returned driver.Rows from wrapRows by doing a composition with wRows.
type withRowsColumnTypeScanType interface {
	ColumnTypeScanType(index int) reflect.Type
}

// wRows implements driver.Rows and all enhancement interfaces except
// driver.RowsColumnTypeScanType.
type wRows struct {
	ctx     context.Context
	parent  driver.Rows
	options Options
}

// HasNextResultSet calls the implements the driver.RowsNextResultSet for wRows.
// It returns the the underlying result of HasNextResultSet from the wRows.parent
// if the parent implements driver.RowsNextResultSet.
func (r wRows) HasNextResultSet() bool {
	if v, ok := r.parent.(driver.RowsNextResultSet); ok {
		return v.HasNextResultSet()
	}

	return false
}

// NextResultsSet calls the implements the driver.RowsNextResultSet for wRows.
// It returns the the underlying result of NextResultSet from the wRows.parent
// if the parent implements driver.RowsNextResultSet.
func (r wRows) NextResultSet() error {
	if v, ok := r.parent.(driver.RowsNextResultSet); ok {
		return v.NextResultSet()
	}

	return io.EOF
}

// ColumnTypeDatabaseTypeName calls the implements the driver.RowsColumnTypeDatabaseTypeName for wRows.
// It returns the the underlying result of ColumnTypeDatabaseTypeName from the wRows.parent
// if the parent implements driver.RowsColumnTypeDatabaseTypeName.
func (r wRows) ColumnTypeDatabaseTypeName(index int) string {
	if v, ok := r.parent.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return v.ColumnTypeDatabaseTypeName(index)
	}

	return ""
}

// ColumnTypeLength calls the implements the driver.RowsColumnTypeLength for wRows.
// It returns the the underlying result of ColumnTypeLength from the wRows.parent
// if the parent implements driver.RowsColumnTypeLength.
func (r wRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if v, ok := r.parent.(driver.RowsColumnTypeLength); ok {
		return v.ColumnTypeLength(index)
	}

	return 0, false
}

// ColumnTypeNullable calls the implements the driver.RowsColumnTypeNullable for wRows.
// It returns the the underlying result of ColumnTypeNullable from the wRows.parent
// if the parent implements driver.RowsColumnTypeNullable.
func (r wRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if v, ok := r.parent.(driver.RowsColumnTypeNullable); ok {
		return v.ColumnTypeNullable(index)
	}

	return false, false
}

// ColumnTypePrecisionScale calls the implements the driver.RowsColumnTypePrecisionScale for wRows.
// It returns the the underlying result of ColumnTypePrecisionScale from the wRows.parent
// if the parent implements driver.RowsColumnTypePrecisionScale.
func (r wRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if v, ok := r.parent.(driver.RowsColumnTypePrecisionScale); ok {
		return v.ColumnTypePrecisionScale(index)
	}

	return 0, 0, false
}

func (r wRows) Columns() []string {
	return r.parent.Columns()
}

func (r wRows) Close() (err error) {
	if r.options.operations[RowsClose] {
		_, finalizers := apply(r.ctx, r.options.Middlewares, RowsClose, "", nil)

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	return r.parent.Close()
}

func (r wRows) Next(dest []driver.Value) (err error) {
	if r.options.operations[RowsNext] {
		_, finalizers := apply(r.ctx, r.options.Middlewares, RowsNext, "", nil)

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	return r.parent.Next(dest)
}

// wrapRows returns a struct which conforms to the driver.Rows interface.
// wRows implements all enhancement interfaces that have no effect on
// sql/database logic in case the underlying parent implementation lacks them.
// Currently the one exception is RowsColumnTypeScanType which does not have a
// valid zero value. This interface is tested for and only enabled in case the
// parent implementation supports it.
func wrapRows(ctx context.Context, parent driver.Rows, options Options) driver.Rows {
	ts, hasColumnTypeScan := parent.(driver.RowsColumnTypeScanType)

	r := wRows{
		parent:  parent,
		ctx:     ctx,
		options: options,
	}

	if hasColumnTypeScan {
		return struct {
			wRows
			withRowsColumnTypeScanType
		}{r, ts}
	}

	return r
}

// wTx implements driver.Tx.
type wTx struct {
	parent  driver.Tx
	ctx     context.Context
	options Options
}

func (t wTx) Commit() (err error) {
	if t.options.operations[Commit] {
		_, finalizers := apply(t.ctx, t.options.Middlewares, Commit, "", nil)

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	return t.parent.Commit()
}

func (t wTx) Rollback() (err error) {
	if t.options.operations[Rollback] {
		_, finalizers := apply(t.ctx, t.options.Middlewares, Rollback, "", nil)

		defer func() {
			for _, onFinish := range finalizers {
				onFinish(err)
			}
		}()
	}

	return t.parent.Rollback()
}
