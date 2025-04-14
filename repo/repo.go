package repo

import (
	"context"
	"time"
)

// SortDirection defines the sorting order.
type SortDirection string

const (
	Asc  SortDirection = "ASC"
	Desc SortDirection = "DESC"
)

// Filter represents a single filtering condition.
type Filter struct {
	Field    string
	Operator string // e.g., "=", ">", "<", "LIKE", "IN"
	Value    interface{}
}

// Sort defines sorting criteria.
type Sort struct {
	Field     string
	Direction SortDirection
}

// Pagination defines pagination parameters.
type Pagination struct {
	Page    int
	PerPage int
}

// QueryOptions encapsulates all query modifications.
type QueryOptions struct {
	Filters      []Filter
	Sorts        []Sort
	Pagination   *Pagination
	Preload      []string               // Associations to eager load
	Select       []string               // Fields to select
	Joins        []string               // Custom JOIN clauses
	Conditions   []interface{}          // Raw conditions (e.g., for complex WHERE)
	ExtraOptions map[string]interface{} // For driver-specific options
}

// PaginatedResult holds paginated query results.
type PaginatedResult[T any] struct {
	Items       []T
	TotalCount  int64
	CurrentPage int
	PerPage     int
	TotalPages  int
}

// Repository is the generic interface for data access.
type Repository[T any, ID comparable] interface {
	// Create inserts a new entity.
	Create(ctx context.Context, entity *T) error

	// CreateMany inserts multiple entities.
	CreateMany(ctx context.Context, entities []*T) error

	// Update modifies an existing entity.
	Update(ctx context.Context, entity *T) error

	// UpdateMany modifies multiple entities based on conditions.
	UpdateMany(ctx context.Context, updates map[string]interface{}, opts *QueryOptions) error

	// Delete removes an entity by ID.
	Delete(ctx context.Context, id ID) error

	// DeleteMany removes entities based on conditions.
	DeleteMany(ctx context.Context, opts *QueryOptions) error

	// FindByID retrieves an entity by ID.
	FindByID(ctx context.Context, id ID, opts *QueryOptions) (*T, error)

	// FindOne retrieves a single entity based on conditions.
	FindOne(ctx context.Context, opts *QueryOptions) (*T, error)

	// FindAll retrieves multiple entities with query options.
	FindAll(ctx context.Context, opts *QueryOptions) ([]T, error)

	// FindPaginated retrieves paginated results.
	FindPaginated(ctx context.Context, opts *QueryOptions) (*PaginatedResult[T], error)

	// Count returns the number of entities matching conditions.
	Count(ctx context.Context, opts *QueryOptions) (int64, error)

	// Exists checks if an entity exists based on conditions.
	Exists(ctx context.Context, opts *QueryOptions) (bool, error)

	// Raw executes a raw query and scans results into entities.
	Raw(ctx context.Context, query string, args ...interface{}) ([]T, error)

	// Transaction executes a function within a database transaction.
	Transaction(ctx context.Context, fn func(tx Repository[T, ID]) error) error
}

// WithTimestamps is an optional interface for entities with timestamp fields.
type WithTimestamps interface {
	GetCreatedAt() time.Time
	SetCreatedAt(time.Time)
	GetUpdatedAt() time.Time
	SetUpdatedAt(time.Time)
}

// WithSoftDelete is an optional interface for soft-deletable entities.
type WithSoftDelete interface {
	GetDeletedAt() *time.Time
	SetDeletedAt(*time.Time)
}

// Helper functions to build QueryOptions

// NewQueryOptions creates a new QueryOptions instance.
func NewQueryOptions() *QueryOptions {
	return &QueryOptions{
		Filters:      make([]Filter, 0),
		Sorts:        make([]Sort, 0),
		Preload:      make([]string, 0),
		Select:       make([]string, 0),
		Joins:        make([]string, 0),
		Conditions:   make([]interface{}, 0),
		ExtraOptions: make(map[string]interface{}),
	}
}

// AddFilter adds a filter to QueryOptions.
func (qo *QueryOptions) AddFilter(field, operator string, value interface{}) *QueryOptions {
	qo.Filters = append(qo.Filters, Filter{Field: field, Operator: operator, Value: value})
	return qo
}

// AddSort adds a sort to QueryOptions.
func (qo *QueryOptions) AddSort(field string, direction SortDirection) *QueryOptions {
	qo.Sorts = append(qo.Sorts, Sort{Field: field, Direction: direction})
	return qo
}

// SetPagination sets pagination parameters.
func (qo *QueryOptions) SetPagination(page, perPage int) *QueryOptions {
	qo.Pagination = &Pagination{Page: page, PerPage: perPage}
	return qo
}

// AddPreload adds an association to preload.
func (qo *QueryOptions) AddPreload(association string) *QueryOptions {
	qo.Preload = append(qo.Preload, association)
	return qo
}

// AddSelect adds a field to select.
func (qo *QueryOptions) AddSelect(field string) *QueryOptions {
	qo.Select = append(qo.Select, field)
	return qo
}

// AddJoin adds a JOIN clause.
func (qo *QueryOptions) AddJoin(join string) *QueryOptions {
	qo.Joins = append(qo.Joins, join)
	return qo
}

// AddCondition adds a raw condition.
func (qo *QueryOptions) AddCondition(condition interface{}) *QueryOptions {
	qo.Conditions = append(qo.Conditions, condition)
	return qo
}

// SetExtraOption sets a driver-specific option.
func (qo *QueryOptions) SetExtraOption(key string, value interface{}) *QueryOptions {
	qo.ExtraOptions[key] = value
	return qo
}

// WithoutPagination is a helper method to create QueryOptions without pagination
func (qo *QueryOptions) WithoutPagination() *QueryOptions {
	if qo == nil {
		return nil
	}

	copyOpts := *qo
	copyOpts.Pagination = nil
	return &copyOpts
}
