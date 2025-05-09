package db

import (
	"github.com/huandu/go-sqlbuilder"
)

// Dialect constants
const (
	DialectSQLite = "sqlite"
	DialectMySQL  = "mysql"
	DialectPgSQL  = "pgsql"
	DialectMsSQL  = "mssql"
)

// SupportedDialects is a list of all supported database dialects
var SupportedDialects = []string{
	DialectSQLite,
	DialectMySQL,
	DialectPgSQL,
	DialectMsSQL,
}

// IsDialectSupported checks if the given dialect is supported
func IsDialectSupported(dialect string) bool {
	for _, d := range SupportedDialects {
		if d == dialect {
			return true
		}
	}
	return false
}

// GetFlavorForDialect returns the appropriate sqlbuilder flavor for the dialect
func GetFlavorForDialect(dialect string) sqlbuilder.Flavor {
	switch dialect {
	case DialectSQLite:
		return sqlbuilder.SQLite
	case DialectMySQL:
		return sqlbuilder.MySQL
	case DialectPgSQL:
		return sqlbuilder.PostgreSQL
	case DialectMsSQL:
		return sqlbuilder.SQLServer
	default:
		panic("unsupported dialect: " + dialect)
	}
}

// DBConnectorFactory creates the appropriate connector for a given dialect
func DBConnectorFactory(config *Config) DBConnector {
	switch config.Driver {
	case DialectSQLite:
		return NewSQLiteConnection(config)
	case DialectMySQL:
		return NewMySQLConnection(config)
	case DialectPgSQL:
		return NewPgSQLConnection(config)
	case DialectMsSQL:
		return NewMsSQLConnection(config)
	default:
		panic("unsupported dialect: " + config.Driver)
	}
}
