package db

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
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
	case DialectMySQL, DialectPgSQL:
		config = &Config{} // Not implemented yet
	}

	conn := NewConnection(config)
	db := conn.Open()
	DM().Add(config.ConnName, conn)
	return db
}

func TestDatabaseManager(t *testing.T) {
	// Create a new SQLite connection for testing
	config := &Config{
		ConnName: "default",
		Driver:   DialectSQLite,
		Database: ":memory:",
		Params:   "cache=shared",
	}

	db := DM()
	conn := NewConnection(config)
	conn.Open()

	// Test Add and Get
	db.Add(config.ConnName, conn)
	_, found := db.Get(config.ConnName)
	if !found {
		t.Errorf("Expected connection to be found after adding")
	}

	// Test Remove
	err := db.Remove(config.ConnName)
	if err != nil {
		t.Errorf("Failed to remove connection: %v", err)
	}
	_, found = db.Get(config.ConnName)
	if found {
		t.Errorf("Connection should not be found after removal")
	}

	// Test RemoveAll
	db.Add(config.ConnName, conn)
	err = db.RemoveAll()
	if err != nil {
		t.Errorf("Failed to remove all connections: %v", err)
	}
	if len(db.All()) != 0 {
		t.Errorf("Expected all connections to be removed")
	}
}

func TestQueryBuilder(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	qb := Query()
	if qb.db == nil {
		t.Error("db should not be nil")
	}

	if qb.config == nil {
		t.Error("config should not be nil")
	}

	if len(qb.conditions) != 0 {
		t.Error("conditions should be empty")
	}
}

func TestQueryBuilderFirst(t *testing.T) {
	// Setup SQLite in-memory database
	db := setupDb(DialectSQLite)
	defer db.Close()

	// Create a test table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, created_at DATETIME)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	now := time.Now().Format(time.RFC3339)

	// Insert sample data
	_, err = db.Exec("INSERT INTO users (name, age, created_at) VALUES ('Alice', 30, ?), ('Bob', 25, ?)",
		now,
		now,
	)
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	user, err := Table("users").Where("name", "Alice").First()
	if err != nil || user["name"] != "Alice" {
		t.Errorf("Failed to fetch user: %v", err)
	}
}

func TestQueryBuilderDML(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	// Create a test table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, created_at DATETIME)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Test INSERT
	result, err := Table("users").Insert([]map[string]interface{}{{"name": "Bob", "age": 25}})
	if err != nil || result == nil {
		t.Errorf("Insert operation failed: %v", err)
	}

	// Test UPDATE
	result, err = Table("users").Where("name", "Bob").Update(map[string]interface{}{"age": 26})
	if err != nil || result == nil {
		t.Errorf("Update operation failed: %v", err)
	}

	// Test DELETE
	result, err = Table("users").Where("name", "Bob").Delete()
	if err != nil || result == nil {
		t.Errorf("Delete operation failed: %v", err)
	}

	// Test TRUNCATE
	err = Table("users").Truncate()
	if err != nil {
		t.Errorf("Truncate operation failed: %v", err)
	}
}

func TestQueryBuilderAggregate(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	// Create a test table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, created_at DATETIME)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert some sample data
	result, err := Table("users").Insert([]map[string]interface{}{{"name": "Bob", "age": 25}, {"name": "Alice", "age": 30}})
	if err != nil || result == nil {
		t.Errorf("Insert operation failed: %v", err)
	}

	// Test COUNT
	count, err := Table("users").Count()
	if err != nil || count != 2 {
		t.Errorf("Count operation failed or returned wrong count: %v", err)
	}

	// Test MAX
	maxAge, err := Table("users").Max("age")
	if err != nil || maxAge != 30 {
		t.Errorf("Max operation failed or returned wrong max: %v", err)
	}

	// Test AVG
	avgAge, err := Table("users").Avg("age")
	if err != nil || avgAge != 27.5 {
		t.Errorf("Avg operation failed or returned wrong avg: %v", err)
	}
}

func TestQueryBuilderExists(t *testing.T) {
	// Setup SQLite in-memory database
	db := setupDb(DialectSQLite)
	defer db.Close()

	// Create a test table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, created_at DATETIME)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert some sample data
	result, err := Table("users").Insert([]map[string]interface{}{{"name": "Alice", "age": 30}})
	if err != nil || result == nil {
		t.Errorf("Insert operation failed: %v", err)
	}

	// Test EXISTS
	exists, err := Table("users").Where("name", "Alice").Exists()
	if err != nil || !exists {
		t.Errorf("Exists check failed: %v", err)
	}
}

func TestDSNGeneration(t *testing.T) {
	// Test various DSN generations
	ds := &DataSource{
		Dialect:  DialectMySQL,
		Host:     "localhost",
		Port:     "3306",
		Username: "root",
		Password: "password",
		Name:     "testdb",
		Params:   "charset=utf8mb4,timeout=5s",
	}

	dsn, err := ds.String()
	if err != nil || dsn != "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4,timeout=5s" {
		t.Errorf("MySQL DSN generation failed or incorrect: %v, DSN: %s", err, dsn)
	}

	// Test for other dialects similarly
}

func TestQueryBuilderGroupBy(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price REAL)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert sample data
	_, err = Table("products").Insert([]map[string]interface{}{
		{"category": "Electronics", "price": 1000},
		{"category": "Electronics", "price": 1500},
		{"category": "Books", "price": 50},
	})
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	// Test GROUP BY with COUNT
	results, err := Table("products").GroupBy("category").Get()
	if err != nil || len(results) != 2 {
		t.Errorf("Failed to group by category: %v", err)
	}

	// Test HAVING clause
	results, err = Table("products").GroupBy("category").Having("SUM(price)", ">", 1000).Get()
	if err != nil || len(results) != 1 {
		t.Errorf("Failed to apply HAVING clause: %v", err)
	}
}

func TestQueryBuilderOrderBy(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert sample data
	_, err = Table("users").Insert([]map[string]interface{}{
		{"name": "Zoe"},
		{"name": "Adam"},
		{"name": "Eve"},
	})
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	// Test ORDER BY
	users, err := Table("users").OrderBy("name ASC").Get()
	if err != nil || len(users) != 3 || users[0]["name"].(string) != "Adam" {
		t.Errorf("Failed to order by name: %v", err)
	}

	// Test ORDER BY DESC
	users, err = Table("users").OrderBy("name DESC").Get()
	if err != nil || len(users) != 3 || users[0]["name"].(string) != "Zoe" {
		t.Errorf("Failed to order by name descending: %v", err)
	}
}

func TestQueryBuilderDistinct(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, color TEXT)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert sample data
	_, err = Table("items").Insert([]map[string]interface{}{
		{"color": "red"},
		{"color": "blue"},
		{"color": "red"},
		{"color": "green"},
	})
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	// Test DISTINCT
	colors, err := Table("items").Distinct().Select("color").Get()
	if err != nil || len(colors) != 3 {
		t.Errorf("Failed to select distinct colors: %v", err)
	}
}

func TestQueryBuilderUpdateOrInsert(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE inventory (id INTEGER PRIMARY KEY, item TEXT, quantity INTEGER)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Test update or insert
	qb := Table("inventory")
	_, err = qb.UpdateOrInsert(map[string]interface{}{"item": "widget"}, map[string]interface{}{"quantity": 10})
	if err != nil {
		t.Errorf("UpdateOrInsert failed: %v", err)
	}

	// Check if inserted or updated
	result, err := qb.Where("item", "widget").First()
	if err != nil || result["quantity"].(int64) != 10 {
		t.Errorf("First UpdateOrInsert failed: %v, expected quantity 10 but got %v", err, result["quantity"])
	}

	// Update existing
	_, err = qb.UpdateOrInsert(map[string]interface{}{"item": "widget"}, map[string]interface{}{"quantity": 20})
	if err != nil {
		t.Errorf("UpdateOrInsert failed on update: %v", err)
	}

	// Check update
	result, err = qb.Where("item", "widget").First()
	if err != nil || result["quantity"].(int64) != 20 {
		t.Errorf("Update failed: %v, quantity should be 20 but is %v", err, result["quantity"])
	}
}

func TestQueryBuilderValue(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE prices (id INTEGER PRIMARY KEY, amount REAL)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	_, err = Table("prices").Insert([]map[string]interface{}{{"amount": 100.5}, {"amount": 200.0}})
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	prices, err := Table("prices").Value("amount")
	if err != nil || len(prices) != 2 {
		t.Errorf("Failed to select values: %v", err)
	}
}

func TestQueryBuilderLimitAndOffset(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE prices (id INTEGER PRIMARY KEY, amount REAL)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	_, err = Table("prices").Insert([]map[string]interface{}{
		{"amount": 100.5},
		{"amount": 200.0},
		{"amount": 300.0},
	})
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	prices, err := Table("prices").Limit(1).Offset(1).Get()

	if err != nil {
		t.Errorf("Failed to execute: %v", err)
	}

	if len(prices) != 1 {
		t.Errorf("Failed to enforce limit, expected 1 row but got %d", len(prices))
	}

	if prices[0]["amount"].(float64) != 200.0 {
		t.Errorf("Failed to enforce offset, expected amount 200.0 but got %v", prices[0]["amount"])
	}
}

func TestQueryBuilderOrWhere(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	_, err = Table("users").Insert([]map[string]interface{}{
		{"name": "Alice", "email": "a5lYH@msn.com", "age": 25},
		{"name": "Bob", "email": "2x2Ht@yahoo.com", "age": 30},
		{"name": "Charlie", "email": "a5lYH@gmail.com", "age": 35},
		{"name": "David", "email": "2x2Ht@msn.com", "age": 40},
	})
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	rows, err := Table("users").
		WhereConds([]*Cond{{"email", "LIKE", "%msn.com"}}).
		OrWhere("age", ">=", 30).
		Get()
	if err != nil {
		t.Errorf("Failed to execute: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows but got %d", len(rows))
	}
}

func TestQueryBuilderOrWhereConds(t *testing.T) {
	db := setupDb(DialectSQLite)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	_, err = Table("users").Insert([]map[string]interface{}{
		{"name": "Alice", "email": "a5lYH@msn.com", "age": 25},
		{"name": "Bob", "email": "2x2Ht@yahoo.com", "age": 30},
		{"name": "Charlie", "email": "a5lYH@gmail.com", "age": 35},
		{"name": "David", "email": "2x2Ht@msn.com", "age": 40},
	})
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	rows, err := Table("users").
		WhereConds([]*Cond{{"email", "LIKE", "%msn.com"}}).
		OrWhereConds([]*Cond{{"age", ">", 30}}).
		Get()
	if err != nil {
		t.Errorf("Failed to execute: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows but got %d", len(rows))
	}
}
