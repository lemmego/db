package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Cond represents a condition for SQL WHERE clauses
type Cond struct {
	Column   string
	Operator string
	Value    interface{}
}

type Join struct {
	Table    string
	First    string
	Operator string
	Second   string
	JoinType string // e.g., "INNER", "LEFT", "RIGHT"
}

// QueryBuilder provides methods for building SQL queries
type QueryBuilder struct {
	config     *Config
	db         *sql.DB
	table      string
	conditions []*Cond
	orderBy    string
	selects    []string
	distinct   bool
	groupBy    []string
	having     []*Cond
	joins      []*Join
	offset     int
	limit      int
}

// Query initializes a new QueryBuilder with a database connection
func Query(connName ...string) *QueryBuilder {
	var name string
	if len(connName) > 0 && connName[0] != "" {
		name = connName[0]
	} else {
		name = "default"
	}

	return &QueryBuilder{
		db:         Get(name).DB,
		config:     Get(name).Config,
		conditions: make([]*Cond, 0),
		groupBy:    make([]string, 0),
		having:     make([]*Cond, 0),
		offset:     0,
		limit:      0,
	}
}

// Table sets the table for the query. This is a wrapper around Query for syntax consistency.
func (qb *QueryBuilder) Table(table string) *QueryBuilder {
	qb.table = table
	return qb
}

// Table sets the table for the query. This is a wrapper around Query for syntax consistency.
func Table(table string, connName ...string) *QueryBuilder {
	var name string
	if len(connName) > 0 && connName[0] != "" {
		name = connName[0]
	} else {
		name = "default"
	}

	qb := Query(name).Table(table)
	return qb
}

// Where adds a condition to the WHERE clause with '=' operator by default
func (qb *QueryBuilder) Where(column string, value interface{}) *QueryBuilder {
	qb.conditions = append(qb.conditions, &Cond{Column: column, Operator: "=", Value: value})
	return qb
}

// WhereMap adds multiple conditions to the WHERE clause using '=' operator for all
func (qb *QueryBuilder) WhereMap(conditions map[string]interface{}) *QueryBuilder {
	for column, value := range conditions {
		qb.conditions = append(qb.conditions, &Cond{Column: column, Operator: "=", Value: value})
	}
	return qb
}

// WhereConds adds multiple conditions to the WHERE clause
func (qb *QueryBuilder) WhereConds(conds []*Cond) *QueryBuilder {
	qb.conditions = append(qb.conditions, conds...)
	return qb
}

// OrWhere adds a condition to the WHERE clause with 'OR' operator
func (qb *QueryBuilder) OrWhere(column, operator string, value interface{}) *QueryBuilder {
	qb.conditions = append(qb.conditions, &Cond{Column: column, Operator: operator, Value: value})
	return qb
}

// OrWhereConds adds multiple conditions to the WHERE clause with 'OR' operator
func (qb *QueryBuilder) OrWhereConds(conds []*Cond) *QueryBuilder {
	qb.conditions = append(qb.conditions, conds...)
	return qb
}

// First fetches the first result matching the conditions
func (qb *QueryBuilder) First() (map[string]interface{}, error) {
	qb.Limit(1) // Ensure only one row is returned
	rows, err := qb.executeSelect()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, errors.New("no rows found")
	}

	result := make(map[string]interface{})
	columns, _ := rows.Columns()
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	for i, col := range columns {
		result[col] = values[i]
	}

	return result, nil
}

// GroupBy adds columns to group by in the SQL query
func (qb *QueryBuilder) GroupBy(columns ...string) *QueryBuilder {
	qb.groupBy = append(qb.groupBy, columns...)
	return qb
}

// Having adds conditions to the HAVING clause in the SQL query
func (qb *QueryBuilder) Having(column, operator string, value interface{}) *QueryBuilder {
	qb.having = append(qb.having, &Cond{Column: column, Operator: operator, Value: value})
	return qb
}

// Get fetches all results matching the conditions, including GROUP BY and HAVING
func (qb *QueryBuilder) Get() ([]map[string]interface{}, error) {
	rows, err := qb.executeSelect()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]map[string]interface{}, 0)
	columns, _ := rows.Columns()
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		result := make(map[string]interface{})
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		for i, col := range columns {
			result[col] = values[i]
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Value fetches values for specified columns
func (qb *QueryBuilder) Value(columns ...string) ([]map[string]interface{}, error) {
	if len(columns) == 0 {
		return nil, errors.New("at least one column must be specified")
	}
	qb.selects = columns
	return qb.Get()
}

// Find fetches a row by its primary key
func (qb *QueryBuilder) Find(id interface{}) ([]map[string]interface{}, error) {
	qb.Where("id", id)
	return qb.Get()
}

// OrderBy sets the column for ORDER BY clause
func (qb *QueryBuilder) OrderBy(column string) *QueryBuilder {
	qb.orderBy = column
	return qb
}

// Count returns the number of rows matching the conditions
func (qb *QueryBuilder) Count() (int, error) {
	query := "SELECT COUNT(*) FROM " + qb.table + qb.buildWhereClause()
	var count int
	err := qb.db.QueryRowContext(context.Background(), query).Scan(&count)
	return count, err
}

// Max gets the maximum value of the specified column
func (qb *QueryBuilder) Max(column string) (float64, error) {
	query := "SELECT MAX(" + column + ") FROM " + qb.table + qb.buildWhereClause()
	var max float64
	err := qb.db.QueryRowContext(context.Background(), query).Scan(&max)
	return max, err
}

// Avg computes the average of the specified column
func (qb *QueryBuilder) Avg(column string) (float64, error) {
	query := "SELECT AVG(" + column + ") FROM " + qb.table + qb.buildWhereClause()
	var avg float64
	err := qb.db.QueryRowContext(context.Background(), query).Scan(&avg)
	return avg, err
}

// Exists checks if any rows match the conditions
func (qb *QueryBuilder) Exists() (bool, error) {
	query := "SELECT COUNT(*) FROM " + qb.table + qb.buildWhereClause()
	var count int
	err := qb.db.QueryRowContext(context.Background(), query, qb.getConditionValues()...).Scan(&count)
	return count > 0, err
}

// Select specifies which columns to return
func (qb *QueryBuilder) Select(columns ...string) *QueryBuilder {
	qb.selects = columns
	return qb
}

// Distinct ensures returned rows are unique
func (qb *QueryBuilder) Distinct() *QueryBuilder {
	qb.distinct = true
	return qb
}

// Skip sets the number of records to skip before fetching results
func (qb *QueryBuilder) Skip(offset int) *QueryBuilder {
	qb.offset = offset
	return qb
}

// Offset is an alias for Skip
func (qb *QueryBuilder) Offset(offset int) *QueryBuilder {
	return qb.Skip(offset)
}

// Take sets the number of records to return from the query
func (qb *QueryBuilder) Take(limit int) *QueryBuilder {
	qb.limit = limit
	return qb
}

// Limit is an alias for Take
func (qb *QueryBuilder) Limit(limit int) *QueryBuilder {
	return qb.Take(limit)
}

func (qb *QueryBuilder) executeSelect() (*sql.Rows, error) {
	query := "SELECT " + qb.buildSelectClause() + " FROM " + qb.table +
		qb.buildJoinClause() +
		qb.buildWhereClause() +
		qb.buildGroupByClause() +
		qb.buildHavingClause() +
		qb.buildOrderByClause()

	if qb.limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", qb.limit)
	}

	if qb.offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", qb.offset)
	}

	return qb.db.QueryContext(context.Background(), query, append(qb.getConditionValues(), qb.getHavingValues()...)...)
}

func (qb *QueryBuilder) buildGroupByClause() string {
	if len(qb.groupBy) == 0 {
		return ""
	}
	return " GROUP BY " + strings.Join(qb.groupBy, ", ")
}

func (qb *QueryBuilder) buildHavingClause() string {
	if len(qb.having) == 0 {
		return ""
	}
	clause := " HAVING "
	for i, cond := range qb.having {
		if i > 0 {
			clause += " AND "
		}
		clause += cond.Column + " " + cond.Operator + " ?"
	}
	return clause
}

func (qb *QueryBuilder) getHavingValues() []interface{} {
	values := make([]interface{}, 0)
	for _, cond := range qb.having {
		values = append(values, cond.Value)
	}
	return values
}

func (qb *QueryBuilder) buildWhereClause() string {
	if len(qb.conditions) == 0 {
		return ""
	}
	clause := " WHERE "
	for i, cond := range qb.conditions {
		if i > 0 {
			clause += " AND "
		}
		clause += cond.Column + " " + cond.Operator + " ?"
	}
	return clause
}

func (qb *QueryBuilder) buildOrderByClause() string {
	if qb.orderBy == "" {
		return ""
	}
	return " ORDER BY " + qb.orderBy
}

func (qb *QueryBuilder) buildSelectClause() string {
	if len(qb.selects) == 0 {
		return "*"
	}
	if qb.distinct {
		return "DISTINCT " + joinSelects(qb.selects)
	}
	return joinSelects(qb.selects)
}

func (qb *QueryBuilder) buildJoinClause() string {
	if len(qb.joins) == 0 {
		return ""
	}
	var joinClause strings.Builder
	for _, join := range qb.joins {
		joinClause.WriteString(fmt.Sprintf(" %s JOIN %s ON %s %s %s", join.JoinType, join.Table, join.First, join.Operator, join.Second))
	}
	return joinClause.String()
}

// Insert adds new records into the table
func (qb *QueryBuilder) Insert(data []map[string]interface{}) (sql.Result, error) {
	if len(data) == 0 {
		return nil, errors.New("no data to insert")
	}

	columns := make([]string, 0)
	values := make([]interface{}, 0)

	// Use the first entry to determine columns
	for key := range data[0] {
		columns = append(columns, key)
	}
	placeholders := make([]string, len(data))
	for i, item := range data {
		rowValues := make([]interface{}, len(columns))
		for j, col := range columns {
			rowValues[j] = item[col]
		}
		values = append(values, rowValues...)
		placeholders[i] = "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	}

	query := "INSERT INTO " + qb.table + " (" + strings.Join(columns, ", ") + ") VALUES " + strings.Join(placeholders, ", ")
	return qb.db.ExecContext(context.Background(), query, values...)
}

// InsertGetId adds a single record and returns the ID of the inserted item
func (qb *QueryBuilder) InsertGetId(data map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, errors.New("no data to insert")
	}

	columns := make([]string, 0)
	values := make([]interface{}, 0)
	placeholders := make([]string, 0)

	for key, value := range data {
		columns = append(columns, key)
		values = append(values, value)
		placeholders = append(placeholders, "?")
	}

	query := "INSERT INTO " + qb.table + " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	result, err := qb.db.ExecContext(context.Background(), query, values...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// Update modifies records in the table based on conditions, including support for JSON column operations
func (qb *QueryBuilder) Update(data map[string]interface{}) (sql.Result, error) {
	if len(data) == 0 {
		return nil, errors.New("no data to update")
	}

	setClauses := make([]string, 0)
	values := make([]interface{}, 0)

	for key, value := range data {
		// Check if the key contains JSON path notation
		if strings.Contains(key, "->") {
			jsonParts := strings.Split(key, "->")
			column := jsonParts[0]
			path := strings.Join(jsonParts[1:], "->")
			switch qb.config.Driver {
			case DialectSQLite:
				// SQLite doesn't support JSON functions in UPDATE statements natively
				// You might need to handle this manually or through triggers
				return nil, errors.New("JSON operations in UPDATE not supported for SQLite")
			case DialectMySQL:
				setClauses = append(setClauses, fmt.Sprintf("JSON_SET(%s, '$."+path+"', ?)", column))
			case DialectPgSQL:
				// PostgreSQL uses -> for access, ->> for text value, and #> for path
				setClauses = append(setClauses, fmt.Sprintf("%s = jsonb_set(%s, '{ %s }', ?)", column, column, path))
			default:
				return nil, errors.New("unsupported dialect for JSON update")
			}
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", key))
		}
		values = append(values, value)
	}

	query := "UPDATE " + qb.table + " SET " + strings.Join(setClauses, ", ") + qb.buildWhereClause()
	args := append(values, qb.getConditionValues()...)

	return qb.db.ExecContext(context.Background(), query, args...)
}

// UpdateOrInsert updates a record if it exists, otherwise inserts a new record
func (qb *QueryBuilder) UpdateOrInsert(unique map[string]interface{}, data map[string]interface{}) (sql.Result, error) {
	// Check if the record exists
	result, err := qb.WhereConds(qb.convertMapToConds(unique)).Get()
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if len(result) > 0 {
		// Update if record exists
		return qb.Update(data)
	} else {
		// Combine unique and data for insert if record doesn't exist
		for k, v := range unique {
			data[k] = v
		}
		return qb.Insert([]map[string]interface{}{data})
	}
}

// Delete removes records from the table based on conditions
func (qb *QueryBuilder) Delete() (sql.Result, error) {
	query := "DELETE FROM " + qb.table + qb.buildWhereClause()
	return qb.db.ExecContext(context.Background(), query, qb.getConditionValues()...)
}

// Truncate removes all records from the table
func (qb *QueryBuilder) Truncate() error {
	query := ""
	switch qb.config.Driver {
	case DialectSQLite:
		// SQLite doesn't have a TRUNCATE statement; we use DELETE instead
		query = "DELETE FROM " + qb.table
	case DialectMySQL:
		// MySQL TRUNCATE TABLE
		query = "TRUNCATE TABLE " + qb.table
	case DialectPgSQL:
		// PostgreSQL TRUNCATE TABLE
		query = "TRUNCATE TABLE " + qb.table + " RESTART IDENTITY"
	default:
		return errors.New("unsupported dialect for truncate operation")
	}

	_, err := qb.db.ExecContext(context.Background(), query)
	return err
}

func (qb *QueryBuilder) Join(table, first, operator, second string) *QueryBuilder {
	qb.joins = append(qb.joins, &Join{
		Table:    table,
		First:    first,
		Operator: operator,
		Second:   second,
		JoinType: "INNER", // Default to INNER JOIN
	})
	return qb
}

func (qb *QueryBuilder) JoinType(joinType string) *QueryBuilder {
	if len(qb.joins) > 0 {
		qb.joins[len(qb.joins)-1].JoinType = joinType
	}
	return qb
}

func (qb *QueryBuilder) getConditionValues() []interface{} {
	values := make([]interface{}, 0)
	for _, cond := range qb.conditions {
		values = append(values, cond.Value)
	}
	return values
}

func (qb *QueryBuilder) convertMapToConds(data map[string]interface{}) []*Cond {
	conds := make([]*Cond, 0)
	for key, value := range data {
		conds = append(conds, &Cond{Column: key, Operator: "=", Value: value})
	}
	return conds
}

func joinSelects(columns []string) string {
	return "`" + strings.Join(columns, "`, `") + "`"
}
