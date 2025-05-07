package db

import (
	"github.com/k0kubun/pp/v3"

	"github.com/huandu/go-sqlbuilder"
)

// Cond interface contains the convenience methods for building SQL conditions.
type Cond interface {
	Equal(field string, value interface{}) string
	E(field string, value interface{}) string
	EQ(field string, value interface{}) string
	NotEqual(field string, value interface{}) string
	NE(field string, value interface{}) string
	NEQ(field string, value interface{}) string
	GreaterThan(field string, value interface{}) string
	G(field string, value interface{}) string
	GT(field string, value interface{}) string
	GreaterEqualThan(field string, value interface{}) string
	GE(field string, value interface{}) string
	GTE(field string, value interface{}) string
	LessThan(field string, value interface{}) string
	L(field string, value interface{}) string
	LT(field string, value interface{}) string
	LessEqualThan(field string, value interface{}) string
	LE(field string, value interface{}) string
	LTE(field string, value interface{}) string
	In(field string, values ...interface{}) string
	NotIn(field string, values ...interface{}) string
	Like(field string, value interface{}) string
	ILike(field string, value interface{}) string
	NotLike(field string, value interface{}) string
	NotILike(field string, value interface{}) string
	IsNull(field string) string
	IsNotNull(field string) string
	Between(field string, lower, upper interface{}) string
	NotBetween(field string, lower, upper interface{}) string
	Or(orExpr ...string) string
	And(andExpr ...string) string
	Not(notExpr string) string
	Exists(subquery interface{}) string
	NotExists(subquery interface{}) string
	Any(field, op string, values ...interface{}) string
	All(field, op string, values ...interface{}) string
	Some(field, op string, values ...interface{}) string
	IsDistinctFrom(field string, value interface{}) string
	IsNotDistinctFrom(field string, value interface{}) string
	Var(value interface{}) string
}

// Builder provides the common query builder methods.
type Builder interface {
	sqlbuilder.Builder
}

// QueryBuilder provides many convenient query building functionalities.
type QueryBuilder struct {
	conn      *Connection
	builder   Builder
	tableName string
	debug     bool
}

// BuilderStruct provides common methods for building SQL queries using a struct.
type BuilderStruct struct {
	*sqlbuilder.Struct
}

// BuilderCreateTable provides the query builder methods for creating tables.
type BuilderCreateTable struct {
	*sqlbuilder.CreateTableBuilder
}

// BuilderSelect provides the query builder methods for selecting.
type BuilderSelect struct {
	*sqlbuilder.SelectBuilder
}

// BuilderInsert provides the query builder methods for inserting.
type BuilderInsert struct {
	*sqlbuilder.InsertBuilder
}

// BuilderUpdate provides the query builder methods for updating.
type BuilderUpdate struct {
	*sqlbuilder.UpdateBuilder
}

// BuilderDelete provides the query builder methods for deleting.
type BuilderDelete struct {
	*sqlbuilder.DeleteBuilder
}

// NewQueryBuilder creates a new QueryBuilder instance.
func NewQueryBuilder(conn *Connection, builder ...Builder) *QueryBuilder {
	qb := &QueryBuilder{conn: conn}

	if len(builder) > 0 {
		qb.builder = builder[0]
	} else {
		qb.builder = SelectBuilder(conn.ConnName)
	}

	return qb
}

// Table sets the table name for the query builder.
func (qb *QueryBuilder) Table(name string) *QueryBuilder {
	qb.tableName = name
	return qb
}

// Join adds a JOIN clause to the query builder.
func (qb *QueryBuilder) Join(table string, onExpr ...string) *QueryBuilder {
	qb.builder.(*BuilderSelect).Join(table, onExpr...)
	return qb
}

// Select sets the SELECT clause for the query builder.
func (qb *QueryBuilder) Select(col ...string) *QueryBuilder {
	qb.SetBuilder(SelectBuilder(qb.conn.ConnName))
	qb.builder.(*BuilderSelect).Select(col...).From(qb.tableName)
	return qb
}

// SetBuilder sets the builder for the query builder.
func (qb *QueryBuilder) SetBuilder(builder Builder) *QueryBuilder {
	qb.builder = builder
	return qb
}

// GetBuilder returns the current builder.
func (qb *QueryBuilder) GetBuilder() Builder {
	return qb.builder
}

// AsCreateTable returns the builder as a CreateTable builder.
func (qb *QueryBuilder) AsCreateTable() *BuilderCreateTable {
	return qb.builder.(*BuilderCreateTable)
}

// AsSelect returns the builder as a Select builder.
func (qb *QueryBuilder) AsSelect() *BuilderSelect {
	return qb.builder.(*BuilderSelect)
}

// AsInsert returns the builder as an Insert builder.
func (qb *QueryBuilder) AsInsert() *BuilderInsert {
	return qb.builder.(*BuilderInsert)
}

// AsUpdate returns the builder as an Update builder.
func (qb *QueryBuilder) AsUpdate() *BuilderUpdate {
	return qb.builder.(*BuilderUpdate)
}

// AsDelete returns the builder as a Delete builder.
func (qb *QueryBuilder) AsDelete() *BuilderDelete {
	return qb.builder.(*BuilderDelete)
}

// Build builds the SQL statement and its arguments.
func (qb *QueryBuilder) Build() (string, []interface{}) {
	sqlStmt, args := qb.builder.Build()
	if qb.debug {
		pp.Println(sqlStmt, args)
	}
	return sqlStmt, args
}

// Where adds a WHERE clause to the query builder.
func (qb *QueryBuilder) Where(condFuncs ...ConditionFunc) *QueryBuilder {
	for _, condFunc := range condFuncs {
		switch builder := qb.builder.(type) {
		case *BuilderSelect:
			builder.Where(condFunc(qb.builder))
		case *BuilderUpdate:
			builder.Where(condFunc(qb.builder))
		case *BuilderDelete:
			builder.Where(condFunc(qb.builder))
		}
	}

	return qb
}

// OrderBy adds an ORDER BY clause to the query builder.
func (qb *QueryBuilder) OrderBy(col ...string) *QueryBuilder {
	switch builder := qb.builder.(type) {
	case *BuilderSelect:
		builder.OrderBy(col...)
	case *BuilderUpdate:
		builder.OrderBy(col...)
	case *BuilderDelete:
		builder.OrderBy(col...)
	}

	return qb
}

// Limit adds a LIMIT clause to the query builder.
func (qb *QueryBuilder) Limit(limit int) *QueryBuilder {
	switch builder := qb.builder.(type) {
	case *BuilderSelect:
		builder.Limit(limit)
	case *BuilderUpdate:
		builder.Limit(limit)
	case *BuilderDelete:
		builder.Limit(limit)
	}

	return qb
}

// Offset adds an OFFSET clause to the query builder.
func (qb *QueryBuilder) Offset(offset int) *QueryBuilder {
	switch builder := qb.builder.(type) {
	case *BuilderSelect:
		builder.Offset(offset)
	}

	return qb
}

// GroupBy adds a GROUP BY clause to the query builder.
func (qb *QueryBuilder) GroupBy(col ...string) *QueryBuilder {
	switch builder := qb.builder.(type) {
	case *BuilderSelect:
		builder.GroupBy(col...)
	}

	return qb
}

// Having adds a HAVING clause to the query builder.
func (qb *QueryBuilder) Having(condFuncs ...ConditionFunc) *QueryBuilder {
	for _, condFunc := range condFuncs {
		switch builder := qb.builder.(type) {
		case *BuilderSelect:
			builder.Having(condFunc(qb.builder))
		}
	}

	return qb
}

// Fetch executes the query and returns the results as a slice of maps.
func (qb *QueryBuilder) Fetch() ([]map[string]interface{}, error) {
	sqlStmt, args := qb.builder.Build()
	if qb.debug {
		pp.Println(sqlStmt, args)
	}

	rows, err := qb.conn.Query(sqlStmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create a slice of interface{} pointers to hold the values
	values := make([]interface{}, len(columns))
	for i := range values {
		var v interface{}
		values[i] = &v
	}

	var results []map[string]interface{}

	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		rowData := make(map[string]interface{})
		for i, col := range columns {
			// Dereference the interface{} pointer to get the value
			valPtr := values[i].(*interface{})
			rowData[col] = *valPtr
		}

		results = append(results, rowData)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Debug enables or disables debug mode for the query builder.
func (qb *QueryBuilder) Debug(log bool) *QueryBuilder {
	qb.debug = log
	return qb
}

// Scan executes the query and scans the results into the provided destination.
func (qb *QueryBuilder) Scan(dest interface{}) error {
	query, args := qb.builder.Build()
	if qb.debug {
		pp.Println(query, args)
	}

	return qb.conn.DB.Select(dest, query, args...)
}

// StructBuilder creates a new Struct builder for the given struct value.
func StructBuilder(structValue interface{}, connName ...string) *BuilderStruct {
	builder := sqlbuilder.NewStruct(structValue)
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderStruct{builder.For(sqlbuilder.SQLite)}
	case DialectMySQL:
		return &BuilderStruct{builder.For(sqlbuilder.MySQL)}
	case DialectPgSQL:
		return &BuilderStruct{builder.For(sqlbuilder.PostgreSQL)}
	case DialectMsSQL:
		return &BuilderStruct{builder.For(sqlbuilder.SQLServer)}
	default:
		panic("unsupported driver")
	}
}

// CreateTableBuilder creates a new CreateTable builder.
func CreateTableBuilder(connName ...string) *BuilderCreateTable {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderCreateTable{sqlbuilder.SQLite.NewCreateTableBuilder()}
	case DialectMySQL:
		return &BuilderCreateTable{sqlbuilder.MySQL.NewCreateTableBuilder()}
	case DialectPgSQL:
		return &BuilderCreateTable{sqlbuilder.PostgreSQL.NewCreateTableBuilder()}
	case DialectMsSQL:
		return &BuilderCreateTable{sqlbuilder.SQLServer.NewCreateTableBuilder()}
	default:
		panic("unsupported driver")
	}
}

// SelectBuilder creates a new Select builder.
func SelectBuilder(connName ...string) *BuilderSelect {
	conn := Get(connName...)
	switch conn.Config.Driver {
	case DialectSQLite:
		return &BuilderSelect{sqlbuilder.SQLite.NewSelectBuilder()}
	case DialectMySQL:
		return &BuilderSelect{sqlbuilder.MySQL.NewSelectBuilder()}
	case DialectPgSQL:
		return &BuilderSelect{sqlbuilder.PostgreSQL.NewSelectBuilder()}
	case DialectMsSQL:
		return &BuilderSelect{sqlbuilder.SQLServer.NewSelectBuilder()}
	default:
		panic("unsupported driver")
	}
}

// InsertBuilder creates a new Insert builder.
func InsertBuilder(connName ...string) *BuilderInsert {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderInsert{sqlbuilder.SQLite.NewInsertBuilder()}
	case DialectMySQL:
		return &BuilderInsert{sqlbuilder.MySQL.NewInsertBuilder()}
	case DialectPgSQL:
		return &BuilderInsert{sqlbuilder.PostgreSQL.NewInsertBuilder()}
	case DialectMsSQL:
		return &BuilderInsert{sqlbuilder.SQLServer.NewInsertBuilder()}
	default:
		panic("unsupported driver")
	}
}

// UpdateBuilder creates a new Update builder.
func UpdateBuilder(connName ...string) *BuilderUpdate {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderUpdate{sqlbuilder.SQLite.NewUpdateBuilder()}
	case DialectMySQL:
		return &BuilderUpdate{sqlbuilder.MySQL.NewUpdateBuilder()}
	case DialectPgSQL:
		return &BuilderUpdate{sqlbuilder.PostgreSQL.NewUpdateBuilder()}
	case DialectMsSQL:
		return &BuilderUpdate{sqlbuilder.SQLServer.NewUpdateBuilder()}
	default:
		panic("unsupported driver")
	}
}

// DeleteBuilder creates a new Delete builder.
func DeleteBuilder(connName ...string) *BuilderDelete {
	switch Get(connName...).Config.Driver {
	case DialectSQLite:
		return &BuilderDelete{sqlbuilder.SQLite.NewDeleteBuilder()}
	case DialectMySQL:
		return &BuilderDelete{sqlbuilder.MySQL.NewDeleteBuilder()}
	case DialectPgSQL:
		return &BuilderDelete{sqlbuilder.PostgreSQL.NewDeleteBuilder()}
	case DialectMsSQL:
		return &BuilderDelete{sqlbuilder.SQLServer.NewDeleteBuilder()}
	default:
		panic("unsupported driver")
	}
}
