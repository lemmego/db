package db

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/k0kubun/pp/v3"

	_ "github.com/mattn/go-sqlite3"
)

// =======================TestModels=========================

type User struct {
	ID        uint64    `db:"id" fieldtag:"pk"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`

	Posts []*Post `db:"posts" fieldtag:"hasMany"`
}

type Post struct {
	ID     uint64 `db:"id" fieldtag:"pk"`
	UserID uint64 `db:"user_id"`
	Title  string `db:"title"`
	Body   string `db:"body"`

	Comments []*Comment `db:"comments" fieldtag:"hasMany"`
}

type Comment struct {
	ID     uint64 `db:"id" fieldtag:"pk"`
	PostID uint64 `db:"post_id"`
	Body   string `db:"body"`
}

// =======================TestModels=========================

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
	_, err := conn.Open()
	if err != nil {
		panic(err) // In tests it's acceptable to panic, but in production code we'd handle this differently
	}

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
	setupDb(DialectSQLite)
	user := &User{Name: "John Doe", CreatedAt: time.Now()}
	err := Save(user)

	if err != nil {
		t.Errorf(err.Error())
	}

	var foundUser User

	err = Find(&foundUser)

	if err != nil {
		t.Errorf(err.Error())
	}

	if foundUser.Name != user.Name {
		t.Errorf("Expected %s, got %s", user.Name, foundUser.Name)
	}
}

func TestModels(t *testing.T) {
	setupDb(DialectSQLite)

	err := SaveAll([]*User{
		{Name: "John Doe", CreatedAt: time.Now()},
		{Name: "Jane Doe", CreatedAt: time.Now()},
		{Name: "James Doe", CreatedAt: time.Now()},
	})

	if err != nil {
		t.Errorf(err.Error())
	}

	err = SaveAll([]*Post{
		{UserID: 1, Title: "Post 1", Body: "Lorem ipsum dolor sit amet"},
		{UserID: 1, Title: "Post 2", Body: "Consectetur adipiscing elit"},
		{UserID: 2, Title: "Post 3", Body: "A quick brown fox jumps"},
	})

	if err != nil {
		t.Errorf(err.Error())
	}

	err = SaveAll([]*Comment{
		{PostID: 1, Body: "Comment 1"},
		{PostID: 1, Body: "Comment 2"},
		{PostID: 2, Body: "Comment 3"},
	})

	if err != nil {
		t.Errorf(err.Error())
	}

	var users []User

	err = Query().
		Select("*").
		Debug(true).
		ScanAll(context.Background(), &users)

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

	ub := NewQueryBuilder(db, UpdateBuilder()).AsUpdate()

	//ub := UpdateBuilder()
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
