package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/fatih/structs"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jinzhu/inflection"
	"github.com/k0kubun/pp/v3"
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

// SetBuilder sets the builder for the query builder.
func (qb *QueryBuilder) SetBuilder(builder Builder) *QueryBuilder {
	qb.builder = builder
	return qb
}

// GetBuilder returns the current builder.
func (qb *QueryBuilder) GetBuilder() Builder {
	return qb.builder
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
	sb := SelectBuilder(qb.conn.ConnName)
	sb.Select(col...).From(qb.tableName)
	qb.SetBuilder(sb)
	return qb
}

// Insert sets the INSERT clause for the query builder.
func (qb *QueryBuilder) Insert(cols []string, values [][]any) *QueryBuilder {
	ib := InsertBuilder(qb.conn.ConnName)
	ib.InsertInto(qb.tableName).Cols(cols...)
	for _, value := range values {
		ib.Values(value...)
	}
	qb.SetBuilder(ib)
	return qb
}

// Update sets the UPDATE clause for the query builder.
func (qb *QueryBuilder) Update(cols []string, values [][]any) *QueryBuilder {
	ub := UpdateBuilder(qb.conn.ConnName)
	qb.builder.(*BuilderUpdate).Update(qb.tableName)
	//for i, col := range cols {
	//qb.builder.(*BuilderUpdate).Set(col, values[i])
	//}
	qb.SetBuilder(ub)
	return qb
}

// AsCreateTable returns the builder as a CreateTable builder.
func (qb *QueryBuilder) AsCreateTable() *BuilderCreateTable {
	if qb.builder == nil {
		ctb := CreateTableBuilder(qb.conn.ConnName)
		qb.SetBuilder(ctb)
		return ctb
	}
	return qb.builder.(*BuilderCreateTable)
}

// AsSelect returns the builder as a Select builder.
func (qb *QueryBuilder) AsSelect() *BuilderSelect {
	if qb.builder == nil {
		sb := SelectBuilder(qb.conn.ConnName)
		qb.SetBuilder(sb)
		return sb
	}
	return qb.builder.(*BuilderSelect)
}

// AsInsert returns the builder as an Insert builder.
func (qb *QueryBuilder) AsInsert() *BuilderInsert {
	if qb.builder == nil {
		ib := InsertBuilder(qb.conn.ConnName)
		qb.SetBuilder(ib)
		return ib
	}
	return qb.builder.(*BuilderInsert)
}

// AsUpdate returns the builder as an Update builder.
func (qb *QueryBuilder) AsUpdate() *BuilderUpdate {
	if qb.builder == nil {
		ub := UpdateBuilder(qb.conn.ConnName)
		qb.SetBuilder(ub)
		return ub
	}
	return qb.builder.(*BuilderUpdate)
}

// AsDelete returns the builder as a Delete builder.
func (qb *QueryBuilder) AsDelete() *BuilderDelete {
	if qb.builder == nil {
		db := DeleteBuilder(qb.conn.ConnName)
		qb.SetBuilder(db)
		return db
	}
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
func (qb *QueryBuilder) Fetch(ctx context.Context) ([]map[string]interface{}, error) {
	sqlStmt, args := qb.builder.Build()
	if qb.debug {
		pp.Println(sqlStmt, args)
	}

	rows, err := qb.conn.QueryContext(ctx, sqlStmt, args...)
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

// resolveTableNameFromModel resolves the table name from the model struct.
func resolveTableNameFromModel(model any) string {
	name := structs.Name(model)
	tableName := inflection.Plural(name)
	return strings.ToLower(tableName)
}

// resolveTableNameFromModels resolves the table name from the model struct.
func resolveTableNameFromModels(models []any) string {
	name := structs.Name(models[0])
	tableName := inflection.Plural(name)
	return strings.ToLower(tableName)
}

// Scan executes the query and scans the results into the provided destination.
func (qb *QueryBuilder) Scan(ctx context.Context, dest interface{}) error {
	if qb.tableName == "" {
		qb.tableName = resolveTableNameFromModel(dest)
		println("tablename", qb.tableName)
		qb.AsSelect().Select("*").From(qb.tableName)
	}
	query, args := qb.builder.Build()
	if qb.debug {
		pp.Println(query, args)
	}

	return qb.conn.GetContext(ctx, dest, query, args...)
}

// ScanAll executes the query and scans the results into the provided destination.
func (qb *QueryBuilder) ScanAll(ctx context.Context, dest interface{}) error {
	if qb.tableName == "" {
		qb.tableName = resolveTableNameFromModels(dest.([]any))
		println("tablename", qb.tableName)
		qb.AsSelect().Select("*").From(qb.tableName)
	}
	query, args := qb.builder.Build()
	if qb.debug {
		pp.Println(query, args)
	}

	return qb.conn.SelectContext(ctx, dest, query, args...)
}

// Exec executes the query and returns the result.
func (qb *QueryBuilder) Exec(ctx context.Context) (sql.Result, error) {
	query, args := qb.builder.Build()
	if qb.debug {
		pp.Println(query, args)
	}

	return qb.conn.ExecContext(ctx, query, args...)
}

func Find[T any](model T, connName ...string) error {
	name := strings.ToLower(structs.Name(model))
	query, args := NewQueryBuilder(Get(connName...)).Table(inflection.Plural(name)).Select("*").Build()
	err := Get(connName...).Get(model, query, args...)
	return err
}

func FindCtx[T any](ctx context.Context, model T, connName ...string) error {
	name := strings.ToLower(structs.Name(model))
	query, args := NewQueryBuilder(Get(connName...)).Table(inflection.Plural(name)).Select("*").Build()
	err := Get(connName...).GetContext(ctx, model, query, args...)
	return err
}

func FindAll[T any](models []T, connName ...string) error {
	name := strings.ToLower(structs.Name(models[0]))
	query, args := NewQueryBuilder(Get(connName...)).Table(inflection.Plural(name)).Select("*").Build()
	err := Get(connName...).Select(models, query, args...)
	return err
}

func FindAllCtx[T any](ctx context.Context, models []T, connName ...string) error {
	name := strings.ToLower(structs.Name(models[0]))
	query, args := NewQueryBuilder(Get(connName...)).Table(inflection.Plural(name)).Select("*").Build()
	err := Get(connName...).SelectContext(ctx, models, query, args...)
	return err
}

func SaveCtx[T any](ctx context.Context, model T, connName ...string) error {
	name := structs.Name(model)
	query, args := Model[T](connName...).
		WithoutTag("pk").
		WithoutTag("hasMany").
		WithoutTag("hasOne").
		WithoutTag("belongsTo").
		InsertInto(inflection.Plural(name), model).
		Build()
	_, err := Get(connName...).ExecContext(ctx, query, args...)
	return err
}

func SaveAllCtx[T any](ctx context.Context, models []T, connName ...string) error {
	name := structs.Name(models[0])
	query, args := Model[T](connName...).
		WithoutTag("pk").
		WithoutTag("hasMany").
		WithoutTag("hasOne").
		WithoutTag("belongsTo").
		InsertInto(inflection.Plural(name), models).
		Build()
	_, err := Get(connName...).ExecContext(ctx, query, args...)
	return err
}

func Save[T any](model T, connName ...string) error {
	name := structs.Name(model)
	tableName := strings.ToLower(inflection.Plural(name))

	query, args := Model[T](connName...).
		WithoutTag("hasMany").
		WithoutTag("hasOne").
		WithoutTag("belongsTo").
		InsertInto(tableName, model).
		Build()

	_, err := Get(connName...).Exec(query, args...)
	return err
}

func SaveAll[T any](models []T, connName ...string) error {
	if len(models) == 0 {
		return nil
	}

	name := structs.Name(models[0])
	tableName := strings.ToLower(inflection.Plural(name))

	ms := make([]any, len(models))
	for i, model := range models {
		ms[i] = model
	}

	query, args := Model[T](connName...).
		WithoutTag("hasMany").
		WithoutTag("hasOne").
		WithoutTag("belongsTo").
		InsertInto(tableName, ms...).
		Build()

	_, err := Get(connName...).Exec(query, args...)
	return err
}

// Update updates a model instance in the database
func Update[T any](model T, connName ...string) error {
	name := structs.Name(model)
	query, args := Model[T](connName...).
		WithoutTag("pk").
		WithoutTag("hasMany").
		WithoutTag("hasOne").
		WithoutTag("belongsTo").
		Update(inflection.Plural(name), model).
		Build()
	_, err := Get(connName...).Exec(query, args...)
	return err
}

// UpdateMany updates multiple model instances in the database
func UpdateMany[T any](models []T, connName ...string) error {
	name := structs.Name(models[0])
	ms := make([]any, len(models))
	for i, model := range models {
		ms[i] = model
	}
	query, args := Model[T](connName...).
		WithoutTag("pk").
		WithoutTag("hasMany").
		WithoutTag("hasOne").
		WithoutTag("belongsTo").
		Update(inflection.Plural(name), ms).
		Build()
	_, err := Get(connName...).Exec(query, args...)
	return err
}

// getBuilderForDialect returns the appropriate builder flavor based on dialect
func getBuilderForDialect(driver string) sqlbuilder.Flavor {
	return GetFlavorForDialect(driver)
}

// Model creates a new Struct builder for the given struct value.
func Model[T any](connName ...string) *BuilderStruct {
	var structValue T
	builder := sqlbuilder.NewStruct(structValue)
	return &BuilderStruct{builder.For(getBuilderForDialect(Get(connName...).Config.Driver))}
}

// CreateTableBuilder creates a new CreateTable builder.
func CreateTableBuilder(connName ...string) *BuilderCreateTable {
	conn := Get(connName...)
	flavor := getBuilderForDialect(conn.Config.Driver)
	return &BuilderCreateTable{flavor.NewCreateTableBuilder()}
}

// SelectBuilder creates a new Select builder.
func SelectBuilder(connName ...string) *BuilderSelect {
	conn := Get(connName...)
	flavor := getBuilderForDialect(conn.Config.Driver)
	return &BuilderSelect{flavor.NewSelectBuilder()}
}

// InsertBuilder creates a new Insert builder.
func InsertBuilder(connName ...string) *BuilderInsert {
	conn := Get(connName...)
	flavor := getBuilderForDialect(conn.Config.Driver)
	return &BuilderInsert{flavor.NewInsertBuilder()}
}

// UpdateBuilder creates a new Update builder.
func UpdateBuilder(connName ...string) *BuilderUpdate {
	conn := Get(connName...)
	flavor := getBuilderForDialect(conn.Config.Driver)
	return &BuilderUpdate{flavor.NewUpdateBuilder()}
}

// DeleteBuilder creates a new Delete builder.
func DeleteBuilder(connName ...string) *BuilderDelete {
	conn := Get(connName...)
	flavor := getBuilderForDialect(conn.Config.Driver)
	return &BuilderDelete{flavor.NewDeleteBuilder()}
}

// Delete deletes a model instance from the database
func Delete[T any](model T, connName ...string) error {
	name := structs.Name(model)
	tableName := inflection.Plural(strings.ToLower(name))

	// Find the primary key field (assumed to be the field with the "pk" tag)
	s := structs.New(model)
	var pkField *structs.Field
	var pkValue interface{}

	for _, field := range s.Fields() {
		if field.Tag("fieldtag") == "pk" {
			pkField = field
			pkValue = field.Value()
			break
		}
	}

	if pkField == nil {
		return fmt.Errorf("no primary key field found for %s", name)
	}

	// Build and execute delete query
	deleteBuilder := DeleteBuilder(connName...)
	query, args := deleteBuilder.
		DeleteFrom(tableName).
		Where(fmt.Sprintf("%s = ?", pkField.Tag("db"))).
		Build()

	// Add the parameter values
	args = append(args, pkValue)

	_, err := Get(connName...).Exec(query, args...)
	return err
}

// DeleteMany deletes multiple model instances from the database
func DeleteMany[T any](models []T, connName ...string) error {
	if len(models) == 0 {
		return nil
	}

	name := structs.Name(models[0])
	tableName := inflection.Plural(strings.ToLower(name))

	// Find the primary key field from the first model
	s := structs.New(models[0])
	var pkFieldName string

	for _, field := range s.Fields() {
		if field.Tag("fieldtag") == "pk" {
			pkFieldName = field.Tag("db")
			break
		}
	}

	if pkFieldName == "" {
		return fmt.Errorf("no primary key field found for %s", name)
	}

	// Extract primary key values
	pkValues := make([]interface{}, len(models))
	for i, model := range models {
		s := structs.New(model)
		for _, field := range s.Fields() {
			if field.Tag("fieldtag") == "pk" {
				pkValues[i] = field.Value()
				break
			}
		}
	}

	// Generate placeholders for IN clause
	placeholders := make([]string, len(pkValues))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	// Build and execute delete query
	deleteBuilder := DeleteBuilder(connName...)
	query, args := deleteBuilder.
		DeleteFrom(tableName).
		Where(fmt.Sprintf("%s IN (%s)", pkFieldName, strings.Join(placeholders, ", "))).
		Build()

	// Add the parameter values
	args = append(args, pkValues...)

	_, err := Get(connName...).Exec(query, args...)
	return err
}
