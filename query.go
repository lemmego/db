package db

import "github.com/huandu/go-sqlbuilder"

func CreateBuilder(connName ...string) *sqlbuilder.CreateTableBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return sqlbuilder.SQLite.NewCreateTableBuilder()
	case DialectMySQL:
		return sqlbuilder.MySQL.NewCreateTableBuilder()
	case DialectPgSQL:
		return sqlbuilder.PostgreSQL.NewCreateTableBuilder()
	default:
		panic("unsupported driver")
	}
}

func SelectBuilder(connName ...string) *sqlbuilder.SelectBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return sqlbuilder.SQLite.NewSelectBuilder()
	case DialectMySQL:
		return sqlbuilder.MySQL.NewSelectBuilder()
	case DialectPgSQL:
		return sqlbuilder.PostgreSQL.NewSelectBuilder()
	default:
		panic("unsupported driver")
	}
}

func InsertBuilder(connName ...string) *sqlbuilder.InsertBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return sqlbuilder.SQLite.NewInsertBuilder()
	case DialectMySQL:
		return sqlbuilder.MySQL.NewInsertBuilder()
	case DialectPgSQL:
		return sqlbuilder.PostgreSQL.NewInsertBuilder()
	default:
		panic("unsupported driver")
	}
}

func UpdateBuilder(connName ...string) *sqlbuilder.UpdateBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return sqlbuilder.SQLite.NewUpdateBuilder()
	case DialectMySQL:
		return sqlbuilder.MySQL.NewUpdateBuilder()
	case DialectPgSQL:
		return sqlbuilder.PostgreSQL.NewUpdateBuilder()
	default:
		panic("unsupported driver")
	}
}

func DeleteBuilder(connName ...string) *sqlbuilder.DeleteBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return sqlbuilder.SQLite.NewDeleteBuilder()
	case DialectMySQL:
		return sqlbuilder.MySQL.NewDeleteBuilder()
	case DialectPgSQL:
		return sqlbuilder.PostgreSQL.NewDeleteBuilder()
	default:
		panic("unsupported driver")
	}
}
