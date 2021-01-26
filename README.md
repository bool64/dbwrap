# dbwrap

[![Build Status](https://github.com/bool64/dbwrap/workflows/test-unit/badge.svg)](https://github.com/bool64/dbwrap/actions?query=branch%3Amaster+workflow%3Atest-unit)
[![GoDevDoc](https://img.shields.io/badge/dev-doc-00ADD8?logo=go)](https://pkg.go.dev/github.com/bool64/dbwrap)
[![Time Tracker](https://wakatime.com/badge/github/bool64/dbwrap.svg)](https://wakatime.com/badge/github/bool64/dbwrap)
![Code lines](https://sloc.xyz/github/bool64/dbwrap/?category=code)
![Comments](https://sloc.xyz/github/bool64/dbwrap/?category=comments)

SQL database driver wrapper.

Add a wrapper with custom middlewares to your existing database code to instrument the interactions with the database.

## Example

`Connector` instrumentation with `zap` logging and opencensus tracing for MySQL database.

```go
func dbWithQueriesLogging(dbConnector driver.Connector, logger *zap.Logger) driver.Connector {
	return dbwrap.WrapConnector(dbConnector,
		// This interceptor adds extra observability on DB side.
		// The origin of query would be visible with `SHOW PROCESSLIST` in MySQL.
		dbwrap.WithInterceptor(func(
			ctx context.Context,
			operation dbwrap.Operation,
			statement string,
			args []driver.NamedValue,
		) (context.Context, string, []driver.NamedValue) {
			// Closest caller in the stack with package not equal to listed and to "database/sql".
			caller := dbwrap.Caller(
				"github.com/Masterminds/squirrel",
				"github.com/jmoiron/sqlx",
			)

			// Add caller name as statement comment.
			// Example instrumented statement:
			//   SELECT version_id, is_applied from schema_migrations ORDER BY id DESC -- pressly/goose.MySQLDialect.dbVersionQuery
			return ctx, statement + " -- " + caller, args
		}),

		// This option limits middleware applicability.
		dbwrap.WithOperations(dbwrap.Query, dbwrap.StmtQuery, dbwrap.Exec, dbwrap.StmtExec),

		// This middleware logs statements with arguments at DEBUG level.
		dbwrap.WithMiddleware(
			func(
				ctx context.Context,
				operation dbwrap.Operation,
				statement string,
				args []driver.NamedValue,
			) (nCtx context.Context, onFinish func(error)) {
				// Exec and Query with args is upgraded to prepared statement.
				if len(args) != 0 && (operation == dbwrap.Exec || operation == dbwrap.Query) {
					return ctx, nil
				}

				// Closest caller in the stack with package not equal to listed and to "database/sql".
				caller := dbwrap.Caller(
					"github.com/Masterminds/squirrel",
					"github.com/jmoiron/sqlx",
				)

				ctx, span := trace.StartSpan(ctx, caller+":"+string(operation))
				span.AddAttributes(
					trace.StringAttribute("stmt", statement),
					trace.StringAttribute("args", fmt.Sprintf("%v", args)),
				)

				started := time.Now()
				return ctx, func(err error) {
					defer span.End()

					// ErrSkip happens in Exec or Query that is upgraded to prepared statement.
					if err == driver.ErrSkip {
						return
					}

					res := " complete"

					if err != nil {
						span.SetStatus(trace.Status{
							Message: err.Error(),
						})

						res = " failed"
					}

					logger.Debug(
						caller+" "+string(operation)+res,
						zap.String("stmt", statement),
						zap.Any("args", args),
						zap.String("elapsed", time.Since(started).String()),
						zap.Error(err),
					)
				}
			}),
	)
}
```

## Installation

go get -u github.com/bool64/dbwrap

## Initialize

To use dbwrap with your application, register a wrapper of a database driver as shown below.

Example:
```go
import (
    _ "github.com/mattn/go-sqlite3"
    "github.com/bool64/dbwrap"
)

var (
    driverName string
    err        error
    db         *sql.DB
    mw         dbwrap.Middleware
)

// Register our wrapper for the provided SQLite3 driver.
driverName, err = dbwrap.Register(
    "sqlite3",
    dbwrap.WithOperations(dbwrap.Query, dbwrap.StmtQuery, dbwrap.Exec, dbwrap.StmtExec),
    dbwrap.WithMiddleware(mw)
)
if err != nil {
    log.Fatalf("unable to register wrapped driver: %v\n", err)
}

// Connect to a SQLite3 database using the driver wrapper.
db, err = sql.Open(driverName, "resource.db")
```

A more explicit and alternative way to bootstrap the wrapper exists as shown below. This will only work if the actual
database driver has its driver implementation exported.

Example:

```go
import (
    sqlite3 "github.com/mattn/go-sqlite3"
    "github.com/bool64/dbwrap"
)

var (
    driver driver.Driver
    err    error
    db     *sql.DB
    mw     dbwrap.Middleware
)

// Explicitly wrap the SQLite3 driver with dbwrap.
driver = dbwrap.Wrap(
    &sqlite3.SQLiteDriver{},
    dbwrap.WithOperations(dbwrap.Query, dbwrap.StmtQuery, dbwrap.Exec, dbwrap.StmtExec),
    dbwrap.WithMiddleware(mw)
)

// Register wrapper as a database driver.
sql.Register("dbwrap-sqlite3", driver)

// Connect to a SQLite3 database using driver wrapper.
db, err = sql.Open("dbwrap-sqlite3", "resource.db")
```

Projects providing their own abstractions on top of database/sql/driver can also wrap an existing driver.Conn interface
directly with dbwrap.

Example:

```go
import "github.com/bool64/dbwrap"

func GetConn(...) driver.Conn {
    // Create custom driver.Conn.
    conn := initializeConn(...)

    return dbwrap.WrapConn(conn, dbwrap.WithMiddleware(mw))
}
```

Finally database drivers that support the new (Go 1.10+) driver.Connector interface can be wrapped directly by dbwrap
without the need for dbwrap to register a driver.Driver.

Example:

```go
import(
    "github.com/bool64/dbwrap"
    "github.com/lib/pq"
)

var (
    connector driver.Connector
    err       error
    db        *sql.DB
    mw        dbwrap.Middleware
)

// Get a database driver.Connector for a fixed configuration.
connector, err = pq.NewConnector("postgres://user:passt@host:5432/db")
if err != nil {
    log.Fatalf("unable to create our postgres connector: %v\n", err)
}

// Wrap the driver.Connector with dbwrap.
connector = dbwrap.WrapConnector(connector, dbwrap.WithMiddleware(mw))

// Use the wrapped driver.Connector.
db = sql.OpenDB(connector)
```

## Notes on `jmoiron/sqlx`

If using the `sqlx` library with named queries you will need to use the
`sqlx.NewDb` function to wrap an existing `*sql.DB` connection. Do not use the
`sqlx.Open` and `sqlx.Connect` methods.
`sqlx` uses the driver name to figure out which database is being used. It uses this knowledge to convert named queries
to the correct bind type (dollar sign, question mark) if named queries are not supported natively by the database. Since
dbwrap creates a new driver name it will not be recognized by sqlx and named queries will fail.

Use one of the above methods to first create a `*sql.DB` connection and then create a `*sqlx.DB` connection by wrapping
the `*sql.DB` like this:

```go
// Register our wrapper for the provided Postgres driver.
driverName, err := dbwrap.Register("postgres", dbwrap.WithMiddleware(mw))
if err != nil { ... }

// Connect to a Postgres database using driver wrapper.
db, err := sql.Open(driverName, "postgres://localhost:5432/my_database")
if err != nil { ... }

// Wrap our *sql.DB with sqlx. use the original db driver name!!!
dbx := sqlx.NewDB(db, "postgres")
```

## Context

To really take advantage of dbwrap, all database calls should be made using the
*Context methods. Properly propagated context enables powerful middlewares, like tracing or contextualized logging.

| Old            | New                   |
|----------------|-----------------------|
| *DB.Begin      | *DB.BeginTx           |
| *DB.Exec       | *DB.ExecContext       |
| *DB.Ping       | *DB.PingContext       |
| *DB.Prepare    | *DB.PrepareContext    |
| *DB.Query      | *DB.QueryContext      |
| *DB.QueryRow   | *DB.QueryRowContext   |
|                |                       |
| *Stmt.Exec     | *Stmt.ExecContext     |
| *Stmt.Query    | *Stmt.QueryContext    |
| *Stmt.QueryRow | *Stmt.QueryRowContext |
|                |                       |
| *Tx.Exec       | *Tx.ExecContext       |
| *Tx.Prepare    | *Tx.PrepareContext    |
| *Tx.Query      | *Tx.QueryContext      |
| *Tx.QueryRow   | *Tx.QueryRowContext   |

Example:

```go

func (s *svc) GetDevice(ctx context.Context, id int) (*Device, error) {
    // Assume we have instrumented our service transports and ctx holds a span.
    var device Device
    if err := s.db.QueryRowContext(
        ctx, "SELECT * FROM device WHERE id = ?", id,
    ).Scan(&device); err != nil {
        return nil, err
    }
    return device
}
```

## ocsql

This library is built on top of [ocsql](https://github.com/opencensus-integrations/ocsql) foundations, it leverages
maturity of `ocsql` wrapper implementation while extending it for general cases.

Big thanks to all `ocsql` contributors!
