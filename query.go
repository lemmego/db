package db

import (
	"database/sql"
	"time"

	"github.com/k0kubun/pp/v3"

	"github.com/huandu/go-sqlbuilder"
)

type DBResult struct {
	RowsAffected int64
	LastInsertId int64
	Rows         *sql.Rows
}

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

type Builder interface {
	sqlbuilder.Builder
}

type Struct struct {
	*sqlbuilder.Struct
}

type QueryBuilder struct {
	conn      *Connection
	builder   Builder
	tableName string
	debug     bool
}

// ====================================

type User struct {
	ID        uint64    `db:"id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`

	Posts []Post
}

func (u *User) Columns() map[string]interface{} {
	return map[string]interface{}{
		"id":         u.ID,
		"name":       u.Name,
		"created_at": u.CreatedAt,
	}
}

type Post struct {
	ID     uint64 `db:"id"`
	UserID uint64 `db:"user_id"`
	Title  string `db:"title"`
	Body   string `db:"body"`

	Comments []Comment
}

type Comment struct {
	ID     uint64 `db:"id"`
	PostID uint64 `db:"post_id"`
	Body   string `db:"body"`
}

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

func NewQueryBuilder(conn *Connection, builder ...Builder) *QueryBuilder {
	qb := &QueryBuilder{conn: conn}

	if len(builder) > 0 {
		qb.builder = builder[0]
	} else {
		qb.builder = SelectBuilder(conn.ConnName)
	}

	return qb
}

func (qb *QueryBuilder) Table(name string) *QueryBuilder {
	qb.tableName = name
	return qb
}

func (qb *QueryBuilder) Join(table string, onExpr ...string) *QueryBuilder {
	qb.builder.(*BuilderSelect).Join(table, onExpr...)
	return qb
}

func (qb *QueryBuilder) Select(col ...string) *QueryBuilder {
	qb.SetBuilder(SelectBuilder(qb.conn.ConnName))
	qb.builder.(*BuilderSelect).Select(col...).From(qb.tableName)
	return qb
}

func (qb *QueryBuilder) SetBuilder(builder Builder) *QueryBuilder {
	qb.builder = builder
	return qb
}

func (qb *QueryBuilder) GetBuilder() Builder {
	return qb.builder
}

func (qb *QueryBuilder) AsCreateTable() *BuilderCreateTable {
	return qb.builder.(*BuilderCreateTable)
}

func (qb *QueryBuilder) AsSelect() *BuilderSelect {
	return qb.builder.(*BuilderSelect)
}

func (qb *QueryBuilder) AsInsert() *BuilderInsert {
	return qb.builder.(*BuilderInsert)
}

func (qb *QueryBuilder) AsUpdate() *BuilderUpdate {
	return qb.builder.(*BuilderUpdate)
}

func (qb *QueryBuilder) AsDelete() *BuilderDelete {
	return qb.builder.(*BuilderDelete)
}

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

func (qb *QueryBuilder) Offset(offset int) *QueryBuilder {
	switch builder := qb.builder.(type) {
	case *BuilderSelect:
		builder.Offset(offset)
	}

	return qb
}

func (qb *QueryBuilder) GroupBy(col ...string) *QueryBuilder {
	switch builder := qb.builder.(type) {
	case *BuilderSelect:
		builder.GroupBy(col...)
	}

	return qb
}

func (qb *QueryBuilder) Having(condFuncs ...ConditionFunc) *QueryBuilder {
	for _, condFunc := range condFuncs {
		switch builder := qb.builder.(type) {
		case *BuilderSelect:
			builder.Having(condFunc(qb.builder))
		}
	}

	return qb
}

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

func (qb *QueryBuilder) Debug(log bool) *QueryBuilder {
	qb.debug = log
	return qb
}

func (qb *QueryBuilder) FindOne(columnMap ...map[string]interface{}) {
	sqlStmt, args := qb.builder.Build()
	if qb.debug {
		pp.Println(sqlStmt, args)
	}
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
	conn := Get(connName...)
	switch conn.Config.Driver {
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
