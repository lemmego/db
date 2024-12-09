package db

import "database/sql"

type PgSQLConnection struct {
	config *Config
}

func NewPgSQLConnection(config *Config) *PgSQLConnection {
	return &PgSQLConnection{config: config}
}

func (c *PgSQLConnection) Connect() *sql.DB {
	db, err := sql.Open("postgres", c.config.DSN())
	if err != nil {
		panic(err)
	}

	if err := db.Ping(); err != nil {
		panic(err)
	}

	return db
}
