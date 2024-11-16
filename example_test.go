package dbwrap_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"time"

	"github.com/bool64/dbwrap"
)

func ExampleWrapConnector() {
	// Initialize dbConnector with your SQL Driver, for example for MySQL:
	//   dbConnector, err = /*github.com/go-sql-driver*/mysql.NewConnector(myCfg)
	// or for PostgreSQL:
	//   dbConnector, err = /*github.com/lib*/pq.NewConnector("postgres://user:pass@localhost/db")
	var dbConnector driver.Connector

	// Wrap connector to enable statement logging.
	dbConnector = dbwrap.WrapConnector(dbConnector,
		// This interceptor improves observability on DB side.
		dbwrap.WithInterceptor(func(ctx context.Context, operation dbwrap.Operation, statement string, args []driver.NamedValue) (context.Context, string, []driver.NamedValue) {
			// Closest caller in the stack with package not equal to listed and to "database/sql".
			// Put your database helper packages here, so that caller bubbles up to app level.
			caller := dbwrap.CallerCtx(ctx,
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

		// This middleware logs statements with arguments.
		dbwrap.WithMiddleware(
			func(
				ctx context.Context,
				operation dbwrap.Operation,
				statement string,
				args []driver.NamedValue,
			) (nCtx context.Context, onFinish func(error)) {
				// Closest caller in the stack with package not equal to listed and to "database/sql".
				caller := dbwrap.CallerCtx(ctx,
					"github.com/Masterminds/squirrel",
					"github.com/jmoiron/sqlx",
				)

				started := time.Now()

				return ctx, func(err error) {
					res := " complete"

					if err != nil {
						res = " failed"
					}

					log.Println(
						caller+" "+string(operation)+res,
						statement,
						time.Since(started).String(),
						args,
						err,
					)
				}
			}),
	)

	// Open database with instrumented connector.
	db := sql.OpenDB(dbConnector)

	// Use database.
	_, err := db.Query("SELECT * FROM table")
	if err != nil {
		log.Fatal(err)
	}
}
