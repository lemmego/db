package db

import (
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

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
	// Setup SQLite in-memory database
	config := &Config{
		ConnName: "default",
		Driver:   DialectSQLite,
		Database: ":memory:",
		Params:   "cache=shared",
	}
	conn := NewConnection(config)
	db := conn.Open()
	DM().Add(config.ConnName, conn)

	// Create a test table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, created_at DATETIME)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert sample data
	_, err = db.Exec("INSERT INTO users (name, age, created_at) VALUES ('Alice', 30, ?)", time.Now().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	// Test QueryBuilder methods
	// Test SELECT
	qb1 := Table("users")
	user, err := qb1.Where("name", "Alice").First()
	if err != nil || user["name"] != "Alice" {
		t.Errorf("Failed to fetch user: %v", err)
	}

	// Test WHERE with multiple conditions
	qb2 := Table("users")
	users, err := qb2.WhereMap(map[string]interface{}{
		"name": "Alice",
		"age":  30,
	}).Get()
	if err != nil || len(users) != 1 {
		t.Errorf("Failed to fetch users with multiple conditions: %v", err)
	}

	// Test INSERT
	qb3 := Table("users")
	result, err := qb3.Insert([]map[string]interface{}{{"name": "Bob", "age": 25}})
	if err != nil || result == nil {
		t.Errorf("Insert operation failed: %v", err)
	}

	// Test UPDATE
	qb4 := Table("users")
	result, err = qb4.Where("name", "Bob").Update(map[string]interface{}{"age": 26})
	if err != nil || result == nil {
		t.Errorf("Update operation failed: %v", err)
	}

	// Test DELETE
	qb5 := Table("users")
	result, err = qb5.Where("name", "Bob").Delete()
	if err != nil || result == nil {
		t.Errorf("Delete operation failed: %v", err)
	}

	// Test COUNT
	qb6 := Table("users")
	count, err := qb6.Count()
	if err != nil || count != 1 {
		t.Errorf("Count operation failed or returned wrong count: %v", err)
	}

	// Test EXISTS
	qb7 := Table("users")
	exists, err := qb7.Where("name", "Alice").Exists()
	if err != nil || !exists {
		t.Errorf("Exists check failed: %v", err)
	}

	// Test MAX, AVG would be similar but with appropriate data for numeric columns

	// Test TRUNCATE
	qb8 := Table("users")
	err = qb8.Truncate()
	if err != nil {
		t.Errorf("Truncate operation failed: %v", err)
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
	config := &Config{
		ConnName: "default",
		Driver:   DialectSQLite,
		Database: ":memory:",
		Params:   "cache=shared",
	}
	conn := NewConnection(config)
	db := conn.Open()
	DM().Add(config.ConnName, conn)

	_, err := db.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price REAL)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert sample data
	_, err = db.Exec("INSERT INTO products (category, price) VALUES ('Electronics', 1000), ('Electronics', 1500), ('Books', 50)")
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	// Test GROUP BY with COUNT
	qb := Table("products").GroupBy("category")
	results, err := qb.Get()
	if err != nil || len(results) != 2 {
		t.Errorf("Failed to group by category: %v", err)
	}

	// Test HAVING clause
	qb = Table("products").GroupBy("category").Having("SUM(price)", ">", 1000)
	results, err = qb.Get()
	if err != nil || len(results) != 1 {
		t.Errorf("Failed to apply HAVING clause: %v", err)
	}
}

func TestQueryBuilderOrderBy(t *testing.T) {
	config := &Config{
		ConnName: "default",
		Driver:   DialectSQLite,
		Database: ":memory:",
		Params:   "cache=shared",
	}
	conn := NewConnection(config)
	db := conn.Open()
	DM().Add(config.ConnName, conn)

	// Drop table if it exists to avoid conflicts
	_, err := db.Exec("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("Could not drop table: %v", err)
	}

	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert sample data
	_, err = db.Exec("INSERT INTO users (name) VALUES ('Zoe'), ('Adam'), ('Eve')")
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	// Test ORDER BY
	qb := Table("users").OrderBy("name ASC")
	users, err := qb.Get()
	if err != nil || len(users) != 3 || users[0]["name"].(string) != "Adam" {
		t.Errorf("Failed to order by name: %v", err)
	}

	// Test ORDER BY DESC
	qb = Table("users").OrderBy("name DESC")
	users, err = qb.Get()
	if err != nil || len(users) != 3 || users[0]["name"].(string) != "Zoe" {
		t.Errorf("Failed to order by name descending: %v", err)
	}
}

func TestQueryBuilderDistinct(t *testing.T) {
	config := &Config{
		ConnName: "default",
		Driver:   DialectSQLite,
		Database: ":memory:",
		Params:   "cache=shared",
	}
	conn := NewConnection(config)
	db := conn.Open()
	DM().Add(config.ConnName, conn)

	_, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, color TEXT)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	// Insert sample data
	_, err = db.Exec("INSERT INTO items (color) VALUES ('red'), ('blue'), ('red'), ('green')")
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	// Test DISTINCT
	qb := Table("items").Distinct().Select("color")
	colors, err := qb.Get()
	if err != nil || len(colors) != 3 {
		t.Errorf("Failed to select distinct colors: %v", err)
	}
}

func TestQueryBuilderUpdateOrInsert(t *testing.T) {
	config := &Config{
		ConnName: "default",
		Driver:   DialectSQLite,
		Database: ":memory:",
		Params:   "cache=shared",
	}
	conn := NewConnection(config)
	db := conn.Open()
	DM().Add(config.ConnName, conn)

	// Drop and recreate table to ensure a clean state
	_, err := db.Exec("DROP TABLE IF EXISTS inventory")
	if err != nil {
		t.Fatalf("Could not drop table: %v", err)
	}
	_, err = db.Exec("CREATE TABLE inventory (id INTEGER PRIMARY KEY, item TEXT, quantity INTEGER)")
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
	config := &Config{
		ConnName: "default",
		Driver:   DialectSQLite,
		Database: ":memory:",
		Params:   "cache=shared",
	}
	conn := NewConnection(config)
	db := conn.Open()
	DM().Add(config.ConnName, conn)

	_, err := db.Exec("CREATE TABLE prices (id INTEGER PRIMARY KEY, amount REAL)")
	if err != nil {
		t.Fatalf("Could not create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO prices (amount) VALUES (100.5), (200.0)")
	if err != nil {
		t.Fatalf("Could not insert data: %v", err)
	}

	qb := Table("prices")
	prices, err := qb.Value("amount")
	if err != nil || len(prices) != 2 {
		t.Errorf("Failed to select values: %v", err)
	}
}
