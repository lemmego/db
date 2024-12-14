package db

import "github.com/huandu/go-sqlbuilder"

type BuilderStruct struct {
	*sqlbuilder.Struct
}

type BuilderCreateTable struct {
	*sqlbuilder.CreateTableBuilder
}

type BuilderSelect struct {
	*sqlbuilder.SelectBuilder
}

type BuilderInsert struct {
	*sqlbuilder.InsertBuilder
}

type BuilderUpdate struct {
	*sqlbuilder.UpdateBuilder
}

type BuilderDelete struct {
	*sqlbuilder.DeleteBuilder
}

func StructBuilder(structValue interface{}, connName ...string) *BuilderStruct {
	builder := sqlbuilder.NewStruct(structValue)
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderStruct{builder.For(sqlbuilder.SQLite)}
	case DialectMySQL:
		return &BuilderStruct{builder.For(sqlbuilder.MySQL)}
	case DialectPgSQL:
		return &BuilderStruct{builder.For(sqlbuilder.PostgreSQL)}
	default:
		panic("unsupported driver")
	}
}

func CreateTableBuilder(connName ...string) *BuilderCreateTable {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderCreateTable{sqlbuilder.SQLite.NewCreateTableBuilder()}
	case DialectMySQL:
		return &BuilderCreateTable{sqlbuilder.MySQL.NewCreateTableBuilder()}
	case DialectPgSQL:
		return &BuilderCreateTable{sqlbuilder.PostgreSQL.NewCreateTableBuilder()}
	default:
		panic("unsupported driver")
	}
}

func SelectBuilder(connName ...string) *BuilderSelect {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderSelect{sqlbuilder.SQLite.NewSelectBuilder()}
	case DialectMySQL:
		return &BuilderSelect{sqlbuilder.MySQL.NewSelectBuilder()}
	case DialectPgSQL:
		return &BuilderSelect{sqlbuilder.PostgreSQL.NewSelectBuilder()}
	default:
		panic("unsupported driver")
	}
}

func InsertBuilder(connName ...string) *BuilderInsert {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderInsert{sqlbuilder.SQLite.NewInsertBuilder()}
	case DialectMySQL:
		return &BuilderInsert{sqlbuilder.MySQL.NewInsertBuilder()}
	case DialectPgSQL:
		return &BuilderInsert{sqlbuilder.PostgreSQL.NewInsertBuilder()}
	default:
		panic("unsupported driver")
	}
}

func UpdateBuilder(connName ...string) *BuilderUpdate {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderUpdate{sqlbuilder.SQLite.NewUpdateBuilder()}
	case DialectMySQL:
		return &BuilderUpdate{sqlbuilder.MySQL.NewUpdateBuilder()}
	case DialectPgSQL:
		return &BuilderUpdate{sqlbuilder.PostgreSQL.NewUpdateBuilder()}
	default:
		panic("unsupported driver")
	}
}

func DeleteBuilder(connName ...string) *BuilderDelete {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderDelete{sqlbuilder.SQLite.NewDeleteBuilder()}
	case DialectMySQL:
		return &BuilderDelete{sqlbuilder.MySQL.NewDeleteBuilder()}
	case DialectPgSQL:
		return &BuilderDelete{sqlbuilder.PostgreSQL.NewDeleteBuilder()}
	default:
		panic("unsupported driver")
	}
}
