package db

import "database/sql"

type MySQLConnection struct {
	config *Config
}

func NewMySQLConnection(config *Config) *MySQLConnection {
	return &MySQLConnection{config: config}
}

func (c *MySQLConnection) Connect() (*sql.DB, error) {
	db, err := sql.Open("mysql", c.config.DSN())
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
