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
	setupDb(DialectSQLite)

	// Insert test user
	user := &User{ID: 1, Name: "Original Name", CreatedAt: time.Now()}
	err := Save(user)
	if err != nil {
		t.Errorf("Failed to save user: %v", err)
	}

	// Verify user was inserted
	var savedUser User
	err = Get().Get(&savedUser, "SELECT * FROM users WHERE id = ?", user.ID)
	if err != nil {
		t.Errorf("Failed to get user: %v", err)
	}
	if savedUser.Name != "Original Name" {
		t.Errorf("Expected user name to be 'Original Name', got '%s'", savedUser.Name)
	}

	// Update user
	updatedUser := savedUser
	updatedUser.Name = "Updated Name"
	err = Update(&updatedUser)
	if err != nil {
		t.Errorf("Failed to update user: %v", err)
	}

	// Verify user was updated
	var verifyUser User
	err = Get().Get(&verifyUser, "SELECT * FROM users WHERE id = ?", user.ID)
	if err != nil {
		t.Errorf("Failed to get updated user: %v", err)
	}
	if verifyUser.Name != "Updated Name" {
		t.Errorf("Expected user name to be 'Updated Name', got '%s'", verifyUser.Name)
	}
}

func TestUpdateMany(t *testing.T) {
	setupDb(DialectSQLite)

	// Insert test users
	users := []*User{
		{ID: 10, Name: "User One", CreatedAt: time.Now()},
		{ID: 20, Name: "User Two", CreatedAt: time.Now()},
		{ID: 30, Name: "User Three", CreatedAt: time.Now()},
	}

	err := SaveAll(users)
	if err != nil {
		t.Errorf("Failed to save users: %v", err)
	}

	// Verify users were inserted
	var savedUsers []User
	err = Get().Select(&savedUsers, "SELECT * FROM users WHERE id IN (10, 20, 30) ORDER BY id")
	if err != nil {
		t.Errorf("Failed to get users: %v", err)
	}
	if len(savedUsers) != 3 {
		t.Errorf("Expected 3 users, got %d", len(savedUsers))
	}

	// Update users
	updatedUsers := []User{
		{ID: 10, Name: "Updated One", CreatedAt: savedUsers[0].CreatedAt},
		{ID: 20, Name: "Updated Two", CreatedAt: savedUsers[1].CreatedAt},
		{ID: 30, Name: "Updated Three", CreatedAt: savedUsers[2].CreatedAt},
	}

	err = UpdateMany(updatedUsers)
	if err != nil {
		t.Errorf("Failed to update users: %v", err)
	}

	// Verify users were updated
	var verifyUsers []User
	err = Get().Select(&verifyUsers, "SELECT * FROM users WHERE id IN (10, 20, 30) ORDER BY id")
	if err != nil {
		t.Errorf("Failed to get updated users: %v", err)
	}

	if len(verifyUsers) != 3 {
		t.Errorf("Expected 3 updated users, got %d", len(verifyUsers))
	}

	expectedNames := []string{"Updated One", "Updated Two", "Updated Three"}
	for i, user := range verifyUsers {
		if user.Name != expectedNames[i] {
			t.Errorf("Expected user name to be '%s', got '%s'", expectedNames[i], user.Name)
		}
	}
}

func TestDelete(t *testing.T) {
	setupDb(DialectSQLite)

	// Insert test user
	user := &User{ID: 100, Name: "User To Delete", CreatedAt: time.Now()}
	err := Save(user)
	if err != nil {
		t.Errorf("Failed to save user: %v", err)
	}

	// Verify user was inserted
	var userCount int
	err = Get().Get(&userCount, "SELECT COUNT(*) FROM users WHERE id = ?", user.ID)
	if err != nil {
		t.Errorf("Failed to count users: %v", err)
	}
	if userCount != 1 {
		t.Errorf("Expected 1 user before deletion, got %d", userCount)
	}

	// Delete the user
	err = Delete(user)
	if err != nil {
		t.Errorf("Failed to delete user: %v", err)
	}

	// Verify user was deleted
	err = Get().Get(&userCount, "SELECT COUNT(*) FROM users WHERE id = ?", user.ID)
	if err != nil {
		t.Errorf("Failed to count users after deletion: %v", err)
	}
	if userCount != 0 {
		t.Errorf("Expected 0 users after deletion, got %d", userCount)
	}
}

func TestDeleteMany(t *testing.T) {
	setupDb(DialectSQLite)

	// Insert test users
	users := []*User{
		{ID: 101, Name: "Batch Delete 1", CreatedAt: time.Now()},
		{ID: 102, Name: "Batch Delete 2", CreatedAt: time.Now()},
		{ID: 103, Name: "Batch Delete 3", CreatedAt: time.Now()},
	}

	err := SaveAll(users)
	if err != nil {
		t.Errorf("Failed to save users: %v", err)
	}

	// Verify users were inserted
	var userCount int
	err = Get().Get(&userCount, "SELECT COUNT(*) FROM users WHERE id IN (101, 102, 103)")
	if err != nil {
		t.Errorf("Failed to count users: %v", err)
	}
	if userCount != 3 {
		t.Errorf("Expected 3 users before deletion, got %d", userCount)
	}

	// Delete the users
	typedUsers := make([]User, len(users))
	for i, u := range users {
		typedUsers[i] = User{ID: u.ID, Name: u.Name, CreatedAt: u.CreatedAt}
	}
	err = DeleteMany(typedUsers)
	if err != nil {
		t.Errorf("Failed to delete users: %v", err)
	}

	// Verify users were deleted
	err = Get().Get(&userCount, "SELECT COUNT(*) FROM users WHERE id IN (101, 102, 103)")
	if err != nil {
		t.Errorf("Failed to count users after deletion: %v", err)
	}
	if userCount != 0 {
		t.Errorf("Expected 0 users after deletion, got %d", userCount)
	}
}
