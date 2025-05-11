package db

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
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

func TestPage(t *testing.T) {
	setupDb(DialectSQLite)

	// Insert test data
	_, err := Query().Table("users").Insert([]string{"id", "name", "created_at"}, [][]any{
		{1, "John Doe", time.Now()},
		{2, "Jane Doe", time.Now()},
		{3, "James Doe", time.Now()},
		{4, "Alice Smith", time.Now()},
		{5, "Bob Smith", time.Now()},
	}).Exec(context.Background())

	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	tests := []struct {
		name     string
		page     int
		perPage  int
		expected int // expected number of results
	}{
		{
			name:     "first page with 2 items",
			page:     1,
			perPage:  2,
			expected: 2,
		},
		{
			name:     "second page with 2 items",
			page:     2,
			perPage:  2,
			expected: 2,
		},
		{
			name:     "third page with 2 items",
			page:     3,
			perPage:  2,
			expected: 1,
		},
		{
			name:     "zero page defaults to first page",
			page:     0,
			perPage:  2,
			expected: 2,
		},
		{
			name:     "negative page defaults to first page",
			page:     -1,
			perPage:  2,
			expected: 2,
		},
		{
			name:     "zero perPage defaults to 10",
			page:     1,
			perPage:  0,
			expected: 5,
		},
		{
			name:     "negative perPage defaults to 10",
			page:     1,
			perPage:  -5,
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var users []User
			err := Query().
				Table("users").
				Select("*").
				Page(tt.page, tt.perPage).
				ScanAll(context.Background(), &users)

			if err != nil {
				t.Errorf("Failed to execute query: %v", err)
			}

			if len(users) != tt.expected {
				t.Errorf("Expected %d users, got %d", tt.expected, len(users))
			}

			// For non-default pages, verify the correct items are returned
			if tt.page > 0 && tt.perPage > 0 {
				expectedStartID := (tt.page-1)*tt.perPage + 1
				if len(users) > 0 && users[0].ID != uint64(expectedStartID) {
					t.Errorf("Expected first user ID to be %d, got %d", expectedStartID, users[0].ID)
				}
			}
		})
	}
}

// Helper function to check if a string contains a value, ignoring whitespace
func containsNormalized(query string, value string) bool {
	q := strings.ReplaceAll(query, " ", "")
	q = strings.ReplaceAll(q, "\n", "")
	v := strings.ReplaceAll(value, " ", "")
	v = strings.ReplaceAll(v, "\n", "")
	return strings.Contains(q, v)
}

func TestCursor(t *testing.T) {
	setupDb(DialectSQLite)

	// Insert test data with known IDs for predictable cursor behavior
	_, err := Query().Table("users").Insert([]string{"id", "name", "created_at"}, [][]any{
		{1, "John Doe", time.Now()},
		{2, "Jane Doe", time.Now()},
		{3, "James Doe", time.Now()},
		{4, "Alice Smith", time.Now()},
		{5, "Bob Smith", time.Now()},
	}).Exec(context.Background())

	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	tests := []struct {
		name          string
		cursor        string
		direction     string
		cursorField   string
		expectedIDs   []uint64
		expectedCond  string
		expectedOrder string
	}{
		{
			name:          "empty cursor returns first item",
			cursor:        "",
			direction:     "next",
			cursorField:   "id",
			expectedIDs:   []uint64{1},
			expectedCond:  "",
			expectedOrder: "id",
		},
		{
			name:          "next direction from id 2",
			cursor:        "2",
			direction:     "next",
			cursorField:   "id",
			expectedIDs:   []uint64{3},
			expectedCond:  "id > ?",
			expectedOrder: "id",
		},
		{
			name:          "prev direction from id 4",
			cursor:        "4",
			direction:     "prev",
			cursorField:   "id",
			expectedIDs:   []uint64{3},
			expectedCond:  "id < ?",
			expectedOrder: "id DESC",
		},
		{
			name:          "next direction with last item",
			cursor:        "5",
			direction:     "next",
			cursorField:   "id",
			expectedIDs:   []uint64{},
			expectedCond:  "id > ?",
			expectedOrder: "id",
		},
		{
			name:          "prev direction with first item",
			cursor:        "1",
			direction:     "prev",
			cursorField:   "id",
			expectedIDs:   []uint64{},
			expectedCond:  "id < ?",
			expectedOrder: "id DESC",
		},
		{
			name:          "invalid direction defaults to next",
			cursor:        "2",
			direction:     "invalid",
			cursorField:   "id",
			expectedIDs:   []uint64{3},
			expectedCond:  "id > ?",
			expectedOrder: "id",
		},
		{
			name:          "cursor on non-existent id",
			cursor:        "999",
			direction:     "next",
			cursorField:   "id",
			expectedIDs:   []uint64{},
			expectedCond:  "id > ?",
			expectedOrder: "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var users []User
			err := Query().
				Table("users").
				Select("*").
				Cursor(tt.cursor, tt.direction, tt.cursorField).
				ScanAll(context.Background(), &users)

			if err != nil {
				t.Errorf("Failed to execute query: %v", err)
			}

			// Check number of results
			if len(users) != len(tt.expectedIDs) {
				t.Errorf("Expected %d users, got %d", len(tt.expectedIDs), len(users))
				return
			}

			// Check IDs of returned users
			for i, user := range users {
				if user.ID != tt.expectedIDs[i] {
					t.Errorf("Expected user ID %d, got %d", tt.expectedIDs[i], user.ID)
				}
			}

			// For non-empty cursor, verify the condition and order in the query
			if tt.cursor != "" {
				query, args := Query().
					Table("users").
					Select("*").
					Cursor(tt.cursor, tt.direction, tt.cursorField).
					Build()

				if !containsNormalized(query, tt.expectedCond) {
					t.Errorf("Expected condition '%s' not found in query: %s", tt.expectedCond, query)
				}

				if !containsNormalized(query, tt.expectedOrder) {
					t.Errorf("Expected order '%s' not found in query: %s", tt.expectedOrder, query)
				}

				if !contains(args, tt.cursor) {
					t.Errorf("Expected cursor value '%s' not found in args: %v", tt.cursor, args)
				}
			}
		})
	}
}

// Helper function to check if a slice contains a value
func contains(slice interface{}, value interface{}) bool {
	switch v := slice.(type) {
	case []interface{}:
		for _, item := range v {
			if item == value {
				return true
			}
		}
	case string:
		return v == value.(string)
	}
	return false
}

func TestTransaction(t *testing.T) {
	setupDb(DialectSQLite)

	// Test successful transaction
	err := Query().Transaction(context.Background(), func(qb *QueryBuilder) error {
		// Insert a user
		_, err := qb.Table("users").Insert([]string{"name", "created_at"}, [][]any{
			{"Transaction User", time.Now()},
		}).Exec(context.Background())
		if err != nil {
			return err
		}

		// Update the user - ensure WHERE is after SET
		_, err = qb.Table("users").
			Update([]string{"name"}, [][]any{
				{"Updated Transaction User"},
			}).
			Where(EQ("name", "Transaction User")).
			Exec(context.Background())
		return err
	})

	if err != nil {
		t.Errorf("Transaction failed: %v", err)
	}

	// Verify the changes were committed
	var users []User
	err = Query().
		Table("users").
		Select("*").
		Where(EQ("name", "Updated Transaction User")).
		ScanAll(context.Background(), &users)
	if err != nil {
		t.Errorf("Failed to find updated user: %v", err)
	}
	if len(users) == 0 {
		t.Error("Expected to find updated user")
	}

	// Test failed transaction
	err = Query().Transaction(context.Background(), func(qb *QueryBuilder) error {
		// Insert a user
		_, err := qb.Table("users").Insert([]string{"name", "created_at"}, [][]any{
			{"Failed Transaction User", time.Now()},
		}).Exec(context.Background())
		if err != nil {
			return err
		}

		// Return an error to trigger rollback
		return errors.New("intentional error")
	})

	if err == nil {
		t.Error("Expected transaction to fail")
	}

	// Verify the changes were rolled back
	var failedUsers []User
	err = Query().
		Table("users").
		Select("*").
		Where(EQ("name", "Failed Transaction User")).
		ScanAll(context.Background(), &failedUsers)
	if err != nil {
		t.Errorf("Failed to query for rolled back user: %v", err)
	}
	if len(failedUsers) > 0 {
		t.Error("Expected user to not exist due to rollback")
	}

	// Test panic recovery
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic to be thrown")
			} else if r != "intentional panic" {
				t.Errorf("Expected panic 'intentional panic', got '%v'", r)
			}
		}()

		_ = Query().Transaction(context.Background(), func(qb *QueryBuilder) error {
			// Insert a user
			_, err := qb.Table("users").Insert([]string{"name", "created_at"}, [][]any{
				{"Panic User", time.Now()},
			}).Exec(context.Background())
			if err != nil {
				return err
			}

			// Trigger a panic
			panic("intentional panic")
		})
	}()

	// Verify the changes were rolled back
	var panicUsers []User
	err = Query().
		Table("users").
		Select("*").
		Where(EQ("name", "Panic User")).
		ScanAll(context.Background(), &panicUsers)
	if err != nil {
		t.Errorf("Failed to query for panic rolled back user: %v", err)
	}
	if len(panicUsers) > 0 {
		t.Error("Expected user to not exist due to panic rollback")
	}
}
