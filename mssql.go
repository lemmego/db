package db

import "database/sql"

type MsSQLConnection struct {
	config *Config
}

func NewMsSQLConnection(config *Config) *MsSQLConnection {
	return &MsSQLConnection{config: config}
}

func (c *MsSQLConnection) Connect() (*sql.DB, error) {
	db, err := sql.Open("mssql", c.config.DSN())
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
