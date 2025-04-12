package db

import (
	"database/sql"
	"fmt"
	"github.com/go-viper/mapstructure/v2"
	"github.com/jinzhu/inflection"
	"github.com/k0kubun/pp/v3"
	"log"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/huandu/go-sqlbuilder"
)

func StringToTypeHook(
	from reflect.Type,
	to reflect.Type,
	data interface{},
) (interface{}, error) {
	if data == nil || from.Kind() != reflect.String {
		return data, nil
	}
	str := data.(string)

	switch to.Kind() {
	// Handle all integer types (int, int8, int16, int32, int64)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.ParseInt(str, 10, to.Bits()) // "10" = base 10
	// Handle all unsigned integer types (uint, uint8, uint16, uint32, uint64)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.ParseUint(str, 10, to.Bits())
	// Handle floating-point types (float32, float64)
	case reflect.Float32, reflect.Float64:
		return strconv.ParseFloat(str, to.Bits())
	// Boolean (true/false strings)
	case reflect.Bool:
		return strconv.ParseBool(str) // Handles "true", "false", "1", "0"
	// Handle time.Time (RFC3339 or custom formats)
	case reflect.Struct:
		if to == reflect.TypeOf(time.Time{}) {
			return time.Parse(time.RFC3339, str) // Or time.Parse("2006-01-02", str)
		}
	}
	return data, nil // Default: no conversion
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

type QueryBuilder struct {
	conn      *Connection
	builder   Builder
	tableName string
	debug     bool

	// tableAliasMap is used for storing the unique alias for each table in a query
	// The key is the table name and the value is the alias
	// For a table "users", the alias can be "u0". If there are multiple tables starting with the same letter, the alias can be "u0", "u1", "u2", etc.
	tableAliasMap map[string]string
}

type Modeler interface {
	PrimaryKey() []string
}

type BaseModel struct{}

func (m *BaseModel) PrimaryKey() []string {
	return []string{"id"}
}

type AutoIncr struct {
	ID uint64 `db:"id"`
}

// ====================================

type User struct {
	ID        uint64    `db:"id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`

	Posts []Post `one2many:"posts" fk:"user_id" db:"posts"`
}

type Post struct {
	ID     uint64 `db:"id"`
	UserID uint64 `db:"user_id"`
	Title  string `db:"title"`
	Body   string `db:"body"`

	Comments []Comment `db:"comments"`
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
	conn *Connection
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

// Resolve the table alias based on the first letter of the table name and an integer
// For a table name of "users", the alias would be "u0"
// If a single sql statement has multiple tables starting with the same letter, the alias would be "u0", "u1", "u2", etc.
func (qb *QueryBuilder) resolveTableAlias(tableName string) string {
	// Initialize the tableAliasMap if it is nil
	if qb.tableAliasMap == nil {
		qb.tableAliasMap = make(map[string]string)
	}

	// If the tableName already is in this format: "<tablename> AS alias"
	// then lower case the tableName and split by " as " and return the second part
	tableNameParts := strings.Split(strings.ToLower(tableName), " as ")
	if len(tableNameParts) > 1 {
		qb.tableAliasMap[tableName] = tableNameParts[1]
		return tableNameParts[1]
	}

	currentIndex := 0
	alias := tableName[:1] + fmt.Sprintf("%d", currentIndex)

	// Iterate over the values of the tableAliasMap and check if the alias is already used
	// If it is, increment the integer and try again
	for _, v := range qb.tableAliasMap {
		if v == alias {
			currentIndex++
			alias = tableName[:1] + fmt.Sprintf("%d", currentIndex)
		}
	}

	// Add the alias to the tableAliasMap
	qb.tableAliasMap[tableName] = alias

	return alias
}

func NewQueryBuilder(conn *Connection) *QueryBuilder {
	return &QueryBuilder{conn: conn}
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

	defer func() {
		err = rows.Close()
	}()

	// Get the column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create a slice to hold the values
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(sql.RawBytes)
	}

	// Create a slice to hold the maps
	var results []map[string]interface{}

	// Iterate over the rows
	for rows.Next() {
		// Scan the values into the slice
		err := rows.Scan(values...)
		if err != nil {
			log.Fatal(err)
		}

		// Create a map to hold the row data
		rowData := make(map[string]interface{})

		// Populate the map with column names and values
		for i, col := range columns {
			//fmt.Println("Current column: ", col)
			rowData[col] = string(*values[i].(*sql.RawBytes))
		}

		// Append the map to the results slice
		results = append(results, rowData)
	}

	// Check for errors after iteration
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (qb *QueryBuilder) FetchWithRelations() ([]map[string]interface{}, error) {
	return nil, nil
}

type Rel struct {
	// Name of the relation, typically defined in the model struct
	Name string

	// Type of the relation, e.g., OneToMany, ManyToMany, etc.
	Type string

	// Table name of the relation.
	Table string

	// Cols represents the columns to select from the relation table.
	Cols []string

	// Rel is the nested relation, if any.
	Rel *Rel
}

func (i *Rel) HasNext() bool {
	return i.Rel != nil
}

type Opts struct {
	Includes []*Rel
}

func (qb *QueryBuilder) Debug(log bool) *QueryBuilder {
	qb.debug = log
	return qb
}

// Get fetches the data from the database. It takes a variadic argument called opts.
// If the first argument is not nil, it is expected to be a pointer to an Opts struct.
// The Opts struct contains a slice of Rel structs, which represent the relations to be eager loaded.
//
// For a definition like the following:
//
//	Table("users").
//	Select("*").
//	Limit(10).
//	Offset(0).
//	Get(&Opts{[]*Rel{
//		{Table: "posts", Cols: []string{"id", "title"}, Rel: &Rel{
//			Table: "comments", Cols: []string{"id", "body"},
//		}},
//		{Table: "books", Cols: []string{"id", "title"}},
//	}})
//
// We will treat users table as the parent table, and posts and books as child tables.
// The child tables can have grandchildren and so on.
// The ultimate goal is to fetch the parent table first with limit and offset (for pagination)
// and then fetch the child tables with the ids of the parent table.
// This will go on as long as there are relations to be fetched.
// There will be a single root result slice of maps ([]map[string]interface{}) which
// will contain all the data from the parent table and all the child tables.
// This root result will be returned to the caller.
// In the future, there will be a method called Model() in contrast to Table() which will
// be populated with the data from the root result using mapstructure or similar.
// The definition will then look like:
//
//	Model(&User).
//	Select("*").
//	Limit(10).
//	Offset(0).
//	Get(&Opts{[]*Rel{
//		{Name: "Posts", Cols: []string{"id", "title"}, Rel: &Rel{
//			Name: "Comments", Cols: []string{"id", "body"},
//		}},
//		{Name: "Books", Cols: []string{"id", "title"}},
//	}})
//
// in both cases, under the hood, the following SQL query will run:
// select * from users limit 10 offset 0
// select id, title from posts where user_id in (1, 2, 3)
// select id, body from comments where post_id in (1, 2, 3)
// select id, title from books where user_id in (1, 2, 3)
func (qb *QueryBuilder) Get(opts ...*Opts) ([]map[string]interface{}, error) {
	// Fetch the parent result into results slice
	results, err := qb.Fetch()

	if err != nil {
		panic(err)
	}

	var rootIDs []interface{}

	for _, result := range results {
		if id, ok := result["id"]; ok {
			rootIDs = append(rootIDs, id)
		}
	}

	if len(opts) > 0 && opts[0] != nil {
		// This foreignColumn will be recursively changed for each relation
		foreignColumn := fmt.Sprintf("%s_id", inflection.Singular(qb.tableName))
		for _, inc := range opts[0].Includes {
			if inc != nil {
				qb.loadRelation(results, inc, foreignColumn, rootIDs)
			}
		}
	}

	return results, nil
}

func (qb *QueryBuilder) loadRelation(results []map[string]interface{}, rel *Rel, foreignColumn string, parentIDs []interface{}) {
	if len(parentIDs) == 0 {
		return
	}

	//fmt.Println("Loading relation: ", rel.Table, " with parentIDs: ", parentIDs, " and foreign column", foreignColumn)

	// Build the query for the relation
	tempQb := NewQueryBuilder(qb.conn).Debug(qb.debug)
	tempQb.SetBuilder(SelectBuilder(qb.conn.ConnName))

	var columns []string
	columns = append(columns, rel.Cols...)

	// Ensure foreign key is included
	if !slices.Contains(columns, foreignColumn) {
		columns = append(columns, foreignColumn)
	}

	sb := tempQb.AsSelect()
	sb.Select(columns...).From(rel.Table).Where(sb.In(foreignColumn, parentIDs...))

	newResults, err := tempQb.Fetch()
	if err != nil {
		panic(err)
	}

	// Group the new results by their foreign key
	groupedResults := make(map[interface{}][]map[string]interface{})
	var newIds []interface{}

	for _, newResult := range newResults {
		fkValue := newResult[foreignColumn]
		groupedResults[fkValue] = append(groupedResults[fkValue], newResult)

		if id, ok := newResult["id"]; ok {
			newIds = append(newIds, id)
		}
	}

	// Determine the key name for the relation in the parent
	relationKey := inflection.Plural(rel.Table)
	if rel.Type == OneToOne {
		relationKey = inflection.Singular(rel.Table)
	}

	// Attach the related data to each parent
	for _, parent := range results {
		parentID := parent["id"]
		if related, ok := groupedResults[parentID]; ok {
			if rel.Type == OneToOne {
				// For one-to-one, just take the first result
				if len(related) > 0 {
					parent[relationKey] = related[0]
				}
			} else {
				// For one-to-many, assign all results
				parent[relationKey] = related
			}
		} else {
			// Ensure the relation key exists even if empty
			if rel.Type == OneToOne {
				parent[relationKey] = nil
			} else {
				parent[relationKey] = []map[string]interface{}{}
			}
		}
	}

	// Process nested relations if they exist
	if rel.HasNext() {
		// Create a flattened list of all child records
		var allChildRecords []map[string]interface{}
		for _, parent := range results {
			if children, ok := parent[relationKey]; ok {
				if rel.Type == OneToOne {
					if child, isMap := children.(map[string]interface{}); isMap {
						allChildRecords = append(allChildRecords, child)
					}
				} else if childrenSlice, isSlice := children.([]map[string]interface{}); isSlice {
					allChildRecords = append(allChildRecords, childrenSlice...)
				}
			}
		}

		// Extract IDs from all child records
		var childIDs []interface{}
		for _, child := range allChildRecords {
			if id, ok := child["id"]; ok {
				childIDs = append(childIDs, id)
			}
		}

		if len(childIDs) > 0 {
			nextForeignColumn := fmt.Sprintf("%s_id", inflection.Singular(rel.Table))
			qb.loadRelation(allChildRecords, rel.Rel, nextForeignColumn, childIDs)
		}
	}
}

func (qb *QueryBuilder) getDecoder(dst interface{}) (*mapstructure.Decoder, error) {
	modelType := reflect.TypeOf(dst)
	modelKind := modelType.Kind()

	if modelKind != reflect.Ptr {
		return nil, fmt.Errorf("models must be a pointer to a slice of structs")
	}

	split := strings.Split(modelType.String(), ".")
	modelName := split[len(split)-1]

	if qb.tableName == "" {
		qb.tableName = inflection.Plural(strings.ToLower(modelName))
		qb.AsSelect().From(qb.tableName)
	}

	config := &mapstructure.DecoderConfig{
		DecodeHook: StringToTypeHook,
		Result:     &dst,
		TagName:    "db",
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return nil, err
	}

	return decoder, nil
}

func (qb *QueryBuilder) Find(models interface{}, opts ...*Opts) error {
	decoder, err := qb.getDecoder(models)

	if err != nil {
		return err
	}

	results, err := qb.Get(opts...)
	if err != nil {
		return err
	}

	err = decoder.Decode(results)
	if err != nil {
		return err
	}

	return nil
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
		return &BuilderSelect{conn, sqlbuilder.SQLite.NewSelectBuilder()}
	case DialectMySQL:
		return &BuilderSelect{conn, sqlbuilder.MySQL.NewSelectBuilder()}
	case DialectPgSQL:
		return &BuilderSelect{conn, sqlbuilder.PostgreSQL.NewSelectBuilder()}
	default:
		panic("unsupported driver")
	}
}

func (sb *BuilderSelect) Query(func(sb *BuilderSelect) Builder) *Finisher {
	return &Finisher{sb}
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
