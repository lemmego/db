package db

import "database/sql"

type SQLiteConnection struct {
	config *Config
}

func NewSQLiteConnection(config *Config) *SQLiteConnection {
	return &SQLiteConnection{config: config}
}

func (c *SQLiteConnection) Connect() *sql.DB {
	db, err := sql.Open("sqlite3", c.config.DSN())
	if err != nil {
		panic(err)
	}

	if err := db.Ping(); err != nil {
		panic(err)
	}

	return db
}
