package db

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"testing"
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
}
