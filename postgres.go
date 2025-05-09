package db

import "database/sql"

type PgSQLConnection struct {
	config *Config
}

func NewPgSQLConnection(config *Config) *PgSQLConnection {
	return &PgSQLConnection{config: config}
}

func (c *PgSQLConnection) Connect() (*sql.DB, error) {
	db, err := sql.Open("postgres", c.config.DSN())
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
