package db

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"strconv"
	"testing"
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

//func TestStruct(t *testing.T) {
//	// TODO: Update this
//	type User struct {
//		ID uint64 `db:"id"`
//	}
//
//	db := setupDb(DialectSQLite)
//
//	db.Exec(
//		StructBuilder(&User{}).InsertInto("users").Build(),
//	)
//}

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
