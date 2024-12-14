package db

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"strconv"
	"testing"
	"time"
)

func setupDb(dialect string) *sql.DB {
	var config *Config

	switch dialect {
	case DialectSQLite:
		config = &Config{
			ConnName: "default",
			Driver:   DialectSQLite,
			Database: ":memory:",
			Params:   "cache=shared",
		}
	case DialectMySQL:
		{
			port, _ := strconv.Atoi(os.Getenv("MYSQL_DB_PORT"))
			config = &Config{
				ConnName: "default",
				Driver:   DialectMySQL,
				Database: os.Getenv("MYSQL_DB_DATABASE"),
				Host:     os.Getenv("MYSQL_DB_HOST"),
				Port:     port,
				User:     os.Getenv("MYSQL_DB_USER"),
				Password: os.Getenv("MYSQL_DB_PASSWORD"),
			}
		}
	case DialectPgSQL:
		config = &Config{} // Not implemented yet
	}

	conn := NewConnection(config)
	db := conn.Open()
	DM().Add(config.ConnName, conn)
	return db
}

func createUsersTable(t *testing.T, db *sql.DB) {
	ctb := CreateTableBuilder().CreateTable("users")
	ctb.Define("id", "INTEGER", "PRIMARY KEY")
	ctb.Define("name", "VARCHAR(255)", "NOT NULL")
	ctb.Define("created_at", "DATETIME", "NOT NULL")

	q, _ := ctb.Build()

	_, err := db.Exec(q)

	if err != nil {
		t.Errorf(err.Error())
	}
}

type User struct {
	ID        int64     `db:"id" fieldtag:"pk"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
}

func TestStruct(t *testing.T) {
	db := setupDb(DialectSQLite)
	createUsersTable(t, db)

	ib, args := InsertBuilder().InsertInto("users").
		Cols("id", "name", "created_at").
		Values(1, "Sowren Sen", 1234567890).
		Values(2, "Tanmay Das", 1234567890).Build()

	_, err := db.Exec(ib, args...)
	if err != nil {
		t.Errorf(err.Error())
	}

	userStruct := StructBuilder(new(User))
	sb := userStruct.SelectFrom("users")
	sb.Where(sb.Equal("id", 1))

	q, args := sb.Build()

	rows, err := db.Query(q, args...)

	defer rows.Close()

	if err != nil {
		t.Errorf(err.Error())
	}

	var user User
	if rows.Next() {
		err = rows.Scan(userStruct.Addr(&user)...)
		if err != nil {
			t.Errorf(err.Error())
		}
	}

	if user.Name != "Sowren Sen" || user.CreatedAt.IsZero() {
		t.Errorf("Could not find user")
	}

}

func TestCreateTable(t *testing.T) {
	db := setupDb(DialectSQLite)
	createUsersTable(t, db)
}

func TestSelect(t *testing.T) {
	db := setupDb(DialectSQLite)
	createUsersTable(t, db)
	sb, _ := SelectBuilder().Select("*").From("users").Build()
	rows, err := db.Query(sb)
	defer rows.Close()
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestInsert(t *testing.T) {
	db := setupDb(DialectSQLite)
	createUsersTable(t, db)

	ib, args := InsertBuilder().InsertInto("users").
		Cols("id", "name", "created_at").
		Values(1, "Huan Du", 1234567890).
		Values(2, "Charmy Liu", 1234567890).Build()

	res, err := db.Exec(ib, args...)
	if err != nil {
		t.Errorf(err.Error())
	}

	if rowCount, err := res.RowsAffected(); err != nil || rowCount != 2 {
		t.Errorf(err.Error())
	}
}

func TestUpdate(t *testing.T) {
	// TODO: Update this
	db := setupDb(DialectSQLite)

	ub := UpdateBuilder()
	db.Exec(
		ub.Update("users").Set(ub.Assign("foo", "bar")).Build(),
	)
}

func TestDelete(t *testing.T) {
	// TODO: Update this
	db := setupDb(DialectSQLite)

	db.Exec(
		DeleteBuilder().DeleteFrom("users").Build(),
	)
}
