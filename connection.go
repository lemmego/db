package db

import (
	"context"
	"database/sql"
	"errors"

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
	tx      *sqlx.Tx
}

type CondFunc func(cond Cond) []string

// NewConnection creates a new Connection with the provided config
func NewConnection(config *Config) *Connection {
	return &Connection{Config: config, DB: nil, stdDb: nil, builder: nil, Error: nil, tx: nil}
}

// GetDB returns the standard sql.DB connection
func (c *Connection) GetDB() *sql.DB {
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

	return c.GetDB(), nil
}

// Close closes the database connection
func (c *Connection) Close() error {
	if c.DB != nil {
		return c.DB.Close()
	}
	return nil
}

// BeginTx starts a new transaction
func (c *Connection) BeginTx(ctx context.Context) (*sqlx.Tx, error) {
	if c.tx != nil {
		return nil, errors.New("already in a transaction")
	}
	tx, err := c.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return tx, nil
}

// Commit commits the current transaction
func (c *Connection) Commit() error {
	if c.tx == nil {
		return errors.New("not in a transaction")
	}
	err := c.tx.Commit()
	c.tx = nil
	return err
}

// Rollback rolls back the current transaction
func (c *Connection) Rollback() error {
	if c.tx == nil {
		return errors.New("not in a transaction")
	}
	err := c.tx.Rollback()
	c.tx = nil
	return err
}

// InTransaction returns true if the connection is in a transaction
func (c *Connection) InTransaction() bool {
	return c.tx != nil
}
