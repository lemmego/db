package db

import "database/sql"

type SQLiteConnection struct {
	config *Config
}

func NewSQLiteConnection(config *Config) *SQLiteConnection {
	return &SQLiteConnection{config: config}
}

func (c *SQLiteConnection) Connect() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", c.config.DSN())
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
