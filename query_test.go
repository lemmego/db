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

func TestModels(t *testing.T) {
	setupDb(DialectSQLite)

	_, err := Query().Debug(true).
		Table("users").
		Insert([]string{"name", "created_at"}, [][]any{
			{"John Doe", time.Now()},
			{"Jane Doe", time.Now()},
			{"James Doe", time.Now()},
		}).Exec(context.Background())

	if err != nil {
		t.Errorf(err.Error())
	}

	_, err = Query().
		Table("posts").
		Insert([]string{"user_id", "title", "body"}, [][]any{
			{1, "Post 1", "Lorem ipsum dolor sit amet"},
			{1, "Post 2", "Consectetur adipiscing elit"},
			{2, "Post 3", "A quick brown fox jumps"},
		}).Exec(context.Background())

	if err != nil {
		t.Errorf(err.Error())
	}

	_, err = Query().
		Table("comments").
		Insert([]string{"post_id", "body"}, [][]any{
			{1, "Comment 1"},
			{1, "Comment 2"},
			{2, "Comment 3"},
		}).Exec(context.Background())

	if err != nil {
		t.Errorf(err.Error())
	}

	var users []User

	err = Query().
		Table("users").
		Select("*").
		Debug(true).
		ScanAll(context.Background(), &users)

	if err != nil {
		t.Errorf(err.Error())
	}

	pp.Print(users)
}

func TestInsertBuilder(t *testing.T) {
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

	result, err := db.Exec(ib, args...)

	if err != nil {
		t.Errorf(err.Error())
	}

	if rowCount, err := result.RowsAffected(); err != nil || rowCount != 3 {
		t.Errorf(err.Error())
	}
}

func TestCreateTable(t *testing.T) {
	db := setupDb(DialectSQLite)
	ctb := CreateTableBuilder().CreateTable("users").IfNotExists()
	ctb.Define("id", "INTEGER", "PRIMARY KEY")
	ctb.Define("name", "VARCHAR(255)", "NOT NULL")
	ctb.Define("created_at", "DATETIME", "NOT NULL")

	q, _ := ctb.Build()

	_, err := db.Exec(q)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestSelect(t *testing.T) {
	db := setupDb(DialectSQLite)
	Query().Table("users").Insert([]string{"id", "name", "created_at"}, [][]any{
		{1, "John Doe", 1234567890},
		{2, "Jane Doe", 1234567890},
		{3, "James Doe", 1234567890},
	}).Exec(context.Background())

	sb, _ := SelectBuilder().Select("*").From("users").Build()
	rows, err := db.Query(sb)
	if err != nil {
		t.Errorf(err.Error())
	}

	defer rows.Close()
}

func TestUpdate(t *testing.T) {
	db := setupDb(DialectSQLite)
	Query().Table("users").Insert([]string{"id", "name", "created_at"}, [][]any{
		{1, "John Doe", 1234567890},
		{2, "Jane Doe", 1234567890},
		{3, "James Doe", 1234567890},
	}).Exec(context.Background())

	ub := UpdateBuilder()

	ub.Update("users").
		Set(ub.Assign("name", "Jennifer Doe")).
		Where(ub.EQ("id", 1))

	query, args := ub.Build()

	_, err := db.Exec(query, args...)
	if err != nil {
		t.Errorf(err.Error())
	}

	sb, _ := SelectBuilder().Select("*").From("users").Build()
	rows, err := db.Query(sb)
	if err != nil {
		t.Errorf(err.Error())
	}

	defer rows.Close()

	var users []User

	err = Query().
		Table("users").
		Select("*").
		Debug(true).
		ScanAll(context.Background(), &users)

	if err != nil {
		t.Errorf(err.Error())
	}

	pp.Print(users)
}

func TestDelete(t *testing.T) {
	db := setupDb(DialectSQLite)
	Query().Table("users").Insert([]string{"id", "name", "created_at"}, [][]any{
		{1, "John Doe", 1234567890},
		{2, "Jane Doe", 1234567890},
		{3, "James Doe", 1234567890},
	}).Exec(context.Background())

	deleteBuilder := DeleteBuilder()
	deleteBuilder.DeleteFrom("users").Where(deleteBuilder.EQ("id", 1))

	query, args := deleteBuilder.Build()

	_, err := db.Exec(query, args...)
	if err != nil {
		t.Errorf(err.Error())
	}

	sb, _ := SelectBuilder().Select("*").From("users").Build()
	rows, err := db.Query(sb)
	if err != nil {
		t.Errorf(err.Error())
	}

	defer rows.Close()

	var users []User

	err = Query().
		Table("users").
		Select("*").
		ScanAll(context.Background(), &users)

	if err != nil {
		t.Errorf(err.Error())
	}

	pp.Print(users)

	if len(users) != 2 {
		t.Errorf("Users should have 2 rows")
	}
}
