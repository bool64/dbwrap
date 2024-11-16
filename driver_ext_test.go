package dbwrap_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bool64/dbwrap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ctxKey string

func TestRegister(t *testing.T) {
	_, mock, err := sqlmock.NewWithDSN("mocked1")
	assert.NotNil(t, mock)
	assert.NoError(t, err)

	var l []string

	driverName, err := dbwrap.Register("sqlmock",
		dbwrap.WithOptions(dbwrap.Options{}),
		dbwrap.WithAllOperations(),

		dbwrap.WithMiddleware(
			func(ctx context.Context, operation dbwrap.Operation, statement string, args []driver.NamedValue) (nCtx context.Context, onFinish func(error)) {
				assert.Equal(t, "bool64/dbwrap_test.TestRegister", dbwrap.Caller())

				l = append(l, "mw1 triggered: "+string(operation)+": "+statement)

				return ctx, func(err error) {
					if err == nil {
						l = append(l, "mw1 done")
					} else {
						l = append(l, "mw1 failed: "+err.Error())
					}
				}
			},
			func(ctx context.Context, operation dbwrap.Operation, statement string, args []driver.NamedValue) (nCtx context.Context, onFinish func(error)) {
				l = append(l, "mw2 triggered: "+string(operation)+": "+statement)

				return ctx, func(err error) {
					if err == nil {
						l = append(l, "mw2 done")
					} else {
						l = append(l, "mw2 failed: "+err.Error())
					}
				}
			},
		),

		dbwrap.WithInterceptor(func(ctx context.Context, operation dbwrap.Operation, statement string, args []driver.NamedValue) (context.Context, string, []driver.NamedValue) {
			l = append(l, "intercepted: "+string(operation)+": "+statement)

			if len(args) > 0 {
				args[0].Value = "intercepted"
			}

			return context.WithValue(ctx, ctxKey("intercepted"), true), statement + " #intercepted", args
		}),
	)
	require.NoError(t, err)
	assert.Equal(t, "sqlmock-wrap-0", driverName)

	ctx := context.WithValue(context.Background(), ctxKey("original"), 123)

	db, err := sql.Open(driverName, "mocked1")
	assert.NoError(t, err)
	assert.NotNil(t, db)

	mock.ExpectQuery("SELECT a FROM b WHERE c = \\? #intercepted").
		WillReturnError(errors.New("failed"))

	rows, err := db.Query("SELECT a FROM b WHERE c = ?", 1)
	assert.EqualError(t, err, "failed")
	assert.Nil(t, rows)

	mock.ExpectQuery("SELECT a FROM b WHERE c = \\? #intercepted").
		WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("abc").AddRow("abc"))

	rows, err = db.Query("SELECT a FROM b WHERE c = ?", 1)
	assert.NoError(t, err)
	assert.NotNil(t, rows)

	cols, err := rows.Columns()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, cols)

	var v string

	for rows.Next() {
		assert.NoError(t, rows.Scan(&v))
		assert.Equal(t, "abc", v)
	}

	assert.NoError(t, rows.Close())
	assert.NoError(t, rows.Err())

	mock.ExpectExec("UPDATE b SET a = 1 WHERE c = \\? #intercepted").
		WillReturnError(errors.New("failed"))

	res, err := db.Exec("UPDATE b SET a = 1 WHERE c = ?", 1)
	assert.EqualError(t, err, "failed")
	assert.Nil(t, res)

	mock.ExpectExec("UPDATE b SET a = 1 WHERE c = \\? #intercepted").
		WithArgs("intercepted").
		WillReturnResult(sqlmock.NewResult(10, 2))

	res, err = db.Exec("UPDATE b SET a = 1 WHERE c = ?", 1)
	assert.NoError(t, err)
	lastID, err := res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), lastID)

	aff, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), aff)

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)

	mock.ExpectQuery("SELECT \\? #intercepted").
		WithArgs("intercepted").
		WillReturnRows(sqlmock.NewRows([]string{"a"}))

	rows, err = tx.QueryContext(ctx, "SELECT ?", 1)
	assert.NoError(t, err)
	assert.NoError(t, rows.Close())
	assert.NoError(t, rows.Err())

	mock.ExpectCommit()
	assert.NoError(t, tx.Commit())

	mock.ExpectBegin()

	tx, err = db.BeginTx(ctx, nil)
	assert.NoError(t, err)

	mock.ExpectRollback()
	assert.NoError(t, tx.Rollback())

	mock.ExpectPrepare("DO ?").WillBeClosed()

	stmt, err := db.Prepare("DO ?")
	assert.NoError(t, err)

	mock.ExpectExec("DO \\? #intercepted").
		WithArgs("intercepted").
		WillReturnResult(sqlmock.NewResult(10, 2))

	_, err = stmt.Exec(1)
	assert.NoError(t, err)

	mock.ExpectQuery("DO \\? #intercepted").
		WithArgs("intercepted").
		WillReturnRows(sqlmock.NewRows([]string{"a"}))

	_, err = stmt.Query(1)
	assert.NoError(t, err)

	assert.NoError(t, stmt.Close())

	expectedLog := `intercepted: query: SELECT a FROM b WHERE c = ?
mw1 triggered: query: SELECT a FROM b WHERE c = ? #intercepted
mw2 triggered: query: SELECT a FROM b WHERE c = ? #intercepted
mw2 failed: failed
mw1 failed: failed
intercepted: query: SELECT a FROM b WHERE c = ?
mw1 triggered: query: SELECT a FROM b WHERE c = ? #intercepted
mw2 triggered: query: SELECT a FROM b WHERE c = ? #intercepted
mw2 done
mw1 done
mw1 triggered: rows_next: 
mw2 triggered: rows_next: 
mw2 done
mw1 done
mw1 triggered: rows_next: 
mw2 triggered: rows_next: 
mw2 done
mw1 done
mw1 triggered: rows_next: 
mw2 triggered: rows_next: 
mw2 failed: EOF
mw1 failed: EOF
mw1 triggered: rows_close: 
mw2 triggered: rows_close: 
mw2 done
mw1 done
intercepted: exec: UPDATE b SET a = 1 WHERE c = ?
mw1 triggered: exec: UPDATE b SET a = 1 WHERE c = ? #intercepted
mw2 triggered: exec: UPDATE b SET a = 1 WHERE c = ? #intercepted
mw2 failed: failed
mw1 failed: failed
intercepted: exec: UPDATE b SET a = 1 WHERE c = ?
mw1 triggered: exec: UPDATE b SET a = 1 WHERE c = ? #intercepted
mw2 triggered: exec: UPDATE b SET a = 1 WHERE c = ? #intercepted
mw2 done
mw1 done
mw1 triggered: last_insert_id: 
mw2 triggered: last_insert_id: 
mw2 done
mw1 done
mw1 triggered: rows_affected: 
mw2 triggered: rows_affected: 
mw2 done
mw1 done
mw1 triggered: begin: 
mw2 triggered: begin: 
mw2 done
mw1 done
intercepted: query: SELECT ?
mw1 triggered: query: SELECT ? #intercepted
mw2 triggered: query: SELECT ? #intercepted
mw2 done
mw1 done
mw1 triggered: rows_close: 
mw2 triggered: rows_close: 
mw2 done
mw1 done
mw1 triggered: commit: 
mw2 triggered: commit: 
mw2 done
mw1 done
mw1 triggered: begin: 
mw2 triggered: begin: 
mw2 done
mw1 done
mw1 triggered: rollback: 
mw2 triggered: rollback: 
mw2 done
mw1 done
intercepted: prepare: DO ?
mw1 triggered: prepare: DO ? #intercepted
mw2 triggered: prepare: DO ? #intercepted
mw2 done
mw1 done
intercepted: stmt_exec: DO ? #intercepted
mw1 triggered: stmt_exec: DO ? #intercepted
mw2 triggered: stmt_exec: DO ? #intercepted
mw2 done
mw1 done
intercepted: stmt_query: DO ? #intercepted
mw1 triggered: stmt_query: DO ? #intercepted
mw2 triggered: stmt_query: DO ? #intercepted
mw2 done
mw1 done`

	assert.Equal(t, expectedLog, strings.Join(l, "\n"))
}
