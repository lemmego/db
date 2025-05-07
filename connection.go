package db

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type Connection struct {
	*Config
	*sqlx.DB
	baseDb  *sql.DB
	builder Builder
	Error   error
}

type CondFunc func(cond Cond) []string

func NewConnection(config *Config) *Connection {
	return &Connection{config, nil, nil, nil, nil}
}

func (c *Connection) Db() *sql.DB {
	return c.baseDb
}

func (c *Connection) Open() *sql.DB {
	switch c.Config.Driver {
	case DialectSQLite:
		c.baseDb = NewSQLiteConnection(c.Config).Connect()
		c.DB = sqlx.NewDb(c.baseDb, c.Config.Driver)
	case DialectMySQL:
		c.baseDb = NewMySQLConnection(c.Config).Connect()
		c.DB = sqlx.NewDb(c.baseDb, c.Config.Driver)
	case DialectPgSQL:
		c.baseDb = NewPgSQLConnection(c.Config).Connect()
		c.DB = sqlx.NewDb(c.baseDb, c.Config.Driver)
	}

	if c.DB == nil {
		panic("unsupported driver")
	}

	return c.Db()
}

func (c *Connection) Close() error {
	return c.DB.Close()
}

func (c *Connection) Table(name string) *QueryBuilder {
	return NewQueryBuilder(c).Table(name)
}
