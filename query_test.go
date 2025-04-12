package db

import (
	"database/sql"
	"os"
	"strconv"
	"testing"

	"github.com/k0kubun/pp/v3"
	_ "github.com/mattn/go-sqlite3"
)

func setupDb(dialect string) *Connection {
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
	conn.Open()
	DM().Add(config.ConnName, conn)
	createUsersTable(conn.Db())
	createPostsTable(conn.Db())
	createCommentsTable(conn.Db())
	return conn
}

func createUsersTable(db *sql.DB) error {
	db.Exec(`DELETE from users`)
	ctb := CreateTableBuilder().CreateTable("users").IfNotExists()
	ctb.Define("id", "INTEGER", "PRIMARY KEY")
	ctb.Define("name", "VARCHAR(255)", "NOT NULL")
	ctb.Define("created_at", "DATETIME", "NOT NULL")

	q, _ := ctb.Build()

	_, err := db.Exec(q)

	if err != nil {
		return err
	}

	return nil
}

func createPostsTable(db *sql.DB) error {
	db.Exec(`DELETE from posts`)
	ctb := CreateTableBuilder().CreateTable("posts").IfNotExists()
	ctb.Define("id", "INTEGER", "PRIMARY KEY")
	ctb.Define("user_id", "INTEGER", "NOT NULL")
	ctb.Define("title", "VARCHAR(255)", "NOT NULL")
	ctb.Define("body", "TEXT", "NOT NULL")

	q, _ := ctb.Build()

	_, err := db.Exec(q)

	if err != nil {
		return err
	}

	return nil
}

func createCommentsTable(db *sql.DB) error {
	db.Exec(`DELETE from  comments`)
	ctb := CreateTableBuilder().CreateTable("comments").IfNotExists()
	ctb.Define("id", "INTEGER", "PRIMARY KEY")
	ctb.Define("post_id", "INTEGER", "NOT NULL")
	ctb.Define("body", "TEXT", "NOT NULL")

	q, _ := ctb.Build()

	_, err := db.Exec(q)

	if err != nil {
		return err
	}

	return nil
}

func TestFind(t *testing.T) {
	db := setupDb(DialectSQLite)

	ib, args := InsertBuilder().InsertInto("users").
		Cols("id", "name", "created_at").
		Values(1, "John Doe", 1234567890).
		Values(2, "Jane Doe", 1234567890).
		Values(3, "James Doe", 1234567890).Build()

	_, err := db.Exec(ib, args...)
	if err != nil {
		t.Errorf(err.Error())
	}

	ib, args = InsertBuilder().InsertInto("posts").
		Cols("id", "user_id", "title", "body").
		Values(1, 1, "Post 1", "Lorem ipsum dolor sit amet").
		Values(2, 1, "Post 2", "Consectetur adipiscing elit").
		Values(3, 2, "Post 3", "A quick brown fox jumps").Build()

	_, err = db.Exec(ib, args...)

	if err != nil {
		t.Errorf(err.Error())
	}

	ib, args = InsertBuilder().InsertInto("comments").
		Cols("id", "post_id", "body").
		Values(1, 1, "Comment 1").
		Values(2, 1, "Comment 2").
		Values(3, 2, "Comment 3").Build()

	_, err = db.Exec(ib, args...)

	if err != nil {
		t.Errorf(err.Error())
	}

	var users []User

	err = Query().
		Select("*").
		Debug(true).
		Where(Like("name", "%John%")).
		Offset(0).
		Limit(2).
		Find(&users, &Opts{[]*Rel{
			{Name: "posts", Type: OneToMany, Table: "posts", Cols: []string{"id", "title"}, Rel: &Rel{
				Name: "comments", Type: OneToMany, Table: "comments", Cols: []string{"id", "body"},
			}},
		}})

	if err != nil {
		t.Errorf(err.Error())
	}

	pp.Print(users)
}

func TestDatabaseSQL(t *testing.T) {
	db := setupDb(DialectSQLite)

	ib, args := InsertBuilder().InsertInto("users").
		Cols("id", "name", "created_at").
		Values(1, "Sowren Sen", 1234567890).
		Values(2, "Tanmay Tanmay", 1234567890).
		Values(3, "Tanmay Das", 1234567890).Build()

	_, err := db.Exec(ib, args...)
	if err != nil {
		t.Errorf(err.Error())
	}

	ib, args = InsertBuilder().InsertInto("posts").
		Cols("id", "user_id", "title", "body").
		Values(1, 1, "Lorem", "Lorem ipsum dolor sit amet").
		Values(2, 1, "Ipsum", "Consectetur adipiscing elit").
		Values(3, 2, "Dolor", "A quick brown fox jumps").Build()

	_, err = db.Exec(ib, args...)

	if err != nil {
		t.Errorf(err.Error())
	}

	rows, err := db.Query(`
		SELECT users.id as "users.id", users.name as "users.name", users.created_at as "users.created_at", posts.id as "posts.id" FROM users
		LEFT JOIN posts ON users.id = posts.user_id`)
	if err != nil {
		t.Errorf(err.Error())
	}
	defer rows.Close()

	if err != nil {
		t.Errorf(err.Error())
	}

}

func TestFirst(t *testing.T) {
	db := setupDb(DialectSQLite)

	ib, args := InsertBuilder().InsertInto("users").
		Cols("id", "name", "created_at").
		Values(1, "Sowren Sen", 1234567890).
		Values(2, "Tanmay Tanmay", 1234567890).
		Values(3, "Tanmay Das", 1234567890).Build()

	_, err := db.Exec(ib, args...)
	if err != nil {
		t.Errorf(err.Error())
	}

	ib, args = InsertBuilder().InsertInto("posts").
		Cols("id", "user_id", "title", "body").
		Values(1, 1, "Lorem", "Lorem ipsum dolor sit amet").
		Values(2, 1, "Ipsum", "Consectetur adipiscing elit").
		Values(3, 2, "Dolor", "A quick brown fox jumps").Build()

	_, err = db.Exec(ib, args...)

	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestCreateTable(t *testing.T) {
	//db := setupDb(DialectSQLite)
}

func TestSelect(t *testing.T) {
	db := setupDb(DialectSQLite)
	sb, _ := SelectBuilder().Select("*").From("users").Build()
	rows, err := db.Query(sb)
	defer rows.Close()
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestInsert(t *testing.T) {
	db := setupDb(DialectSQLite)

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
