package db

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// DBConnector is a common interface for database connections
type DBConnector interface {
	Connect() (*sql.DB, error)
}

type Connection struct {
	*Config
	*sqlx.DB
	stdDb   *sql.DB
	builder Builder
	Error   error
}

type CondFunc func(cond Cond) []string

// NewConnection creates a new Connection with the provided config
func NewConnection(config *Config) *Connection {
	return &Connection{config, nil, nil, nil, nil}
}

// Db returns the standard sql.DB connection
func (c *Connection) Db() *sql.DB {
	return c.stdDb
}

// Open establishes a connection to the database based on the configuration
func (c *Connection) Open() (*sql.DB, error) {
	connector := DBConnectorFactory(c.Config)

	db, err := connector.Connect()
	if err != nil {
		return nil, err
	}

	c.stdDb = db
	c.DB = sqlx.NewDb(c.stdDb, c.Config.Driver)

	return c.Db(), nil
}

// Close closes the database connection
func (c *Connection) Close() error {
	if c.DB != nil {
		return c.DB.Close()
	}
	return nil
}

// Table creates a new query builder for the specified table
func (c *Connection) Table(name string) *QueryBuilder {
	return NewQueryBuilder(c).Table(name)
}
