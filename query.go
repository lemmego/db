package db

import "github.com/huandu/go-sqlbuilder"

type StructBuilder struct {
	*sqlbuilder.Struct
}

type CreateTableBuilder struct {
	*sqlbuilder.CreateTableBuilder
}

type SelectBuilder struct {
	*sqlbuilder.SelectBuilder
}

type InsertBuilder struct {
	*sqlbuilder.InsertBuilder
}

type UpdateBuilder struct {
	*sqlbuilder.UpdateBuilder
}

type DeleteBuilder struct {
	*sqlbuilder.DeleteBuilder
}

func BuildStruct(structValue interface{}, connName ...string) *StructBuilder {
	builder := sqlbuilder.NewStruct(structValue)
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &StructBuilder{builder.For(sqlbuilder.SQLite)}
	case DialectMySQL:
		return &StructBuilder{builder.For(sqlbuilder.MySQL)}
	case DialectPgSQL:
		return &StructBuilder{builder.For(sqlbuilder.PostgreSQL)}
	default:
		panic("unsupported driver")
	}
}

func BuildCreateTable(connName ...string) *CreateTableBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &CreateTableBuilder{sqlbuilder.SQLite.NewCreateTableBuilder()}
	case DialectMySQL:
		return &CreateTableBuilder{sqlbuilder.MySQL.NewCreateTableBuilder()}
	case DialectPgSQL:
		return &CreateTableBuilder{sqlbuilder.PostgreSQL.NewCreateTableBuilder()}
	default:
		panic("unsupported driver")
	}
}

func BuildSelect(connName ...string) *SelectBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &SelectBuilder{sqlbuilder.SQLite.NewSelectBuilder()}
	case DialectMySQL:
		return &SelectBuilder{sqlbuilder.MySQL.NewSelectBuilder()}
	case DialectPgSQL:
		return &SelectBuilder{sqlbuilder.PostgreSQL.NewSelectBuilder()}
	default:
		panic("unsupported driver")
	}
}

func BuildInsert(connName ...string) *InsertBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &InsertBuilder{sqlbuilder.SQLite.NewInsertBuilder()}
	case DialectMySQL:
		return &InsertBuilder{sqlbuilder.MySQL.NewInsertBuilder()}
	case DialectPgSQL:
		return &InsertBuilder{sqlbuilder.PostgreSQL.NewInsertBuilder()}
	default:
		panic("unsupported driver")
	}
}

func BuildUpdate(connName ...string) *UpdateBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &UpdateBuilder{sqlbuilder.SQLite.NewUpdateBuilder()}
	case DialectMySQL:
		return &UpdateBuilder{sqlbuilder.MySQL.NewUpdateBuilder()}
	case DialectPgSQL:
		return &UpdateBuilder{sqlbuilder.PostgreSQL.NewUpdateBuilder()}
	default:
		panic("unsupported driver")
	}
}

func BuildDelete(connName ...string) *DeleteBuilder {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &DeleteBuilder{sqlbuilder.SQLite.NewDeleteBuilder()}
	case DialectMySQL:
		return &DeleteBuilder{sqlbuilder.MySQL.NewDeleteBuilder()}
	case DialectPgSQL:
		return &DeleteBuilder{sqlbuilder.PostgreSQL.NewDeleteBuilder()}
	default:
		panic("unsupported driver")
	}
}
