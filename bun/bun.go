package bun

import (
	"context"
	"fmt"
	"github.com/lemmego/db/repo"
	"time"

	"github.com/uptrace/bun"
)

// BunRepository implements the Repository interface using Bun ORM
type BunRepository[T any, ID comparable] struct {
	db bun.IDB
}

// NewBunRepository creates a new BunRepository instance
func NewBunRepository[T any, ID comparable](db bun.IDB) *BunRepository[T, ID] {
	return &BunRepository[T, ID]{db: db}
}

// Create inserts a new entity
func (r *BunRepository[T, ID]) Create(ctx context.Context, entity *T) error {
	_, err := r.db.NewInsert().Model(entity).Exec(ctx)
	if err != nil {
		return err
	}

	// Handle timestamps if the entity implements WithTimestamps
	if withTS, ok := any(entity).(repo.WithTimestamps); ok {
		now := time.Now()
		withTS.SetCreatedAt(now)
		withTS.SetUpdatedAt(now)
	}

	return nil
}

// CreateMany inserts multiple entities
func (r *BunRepository[T, ID]) CreateMany(ctx context.Context, entities []*T) error {
	if len(entities) == 0 {
		return nil
	}

	// Handle timestamps if the entity implements WithTimestamps
	if _, ok := any(entities[0]).(repo.WithTimestamps); ok {
		now := time.Now()
		for _, entity := range entities {
			if withTS, ok := any(entity).(repo.WithTimestamps); ok {
				withTS.SetCreatedAt(now)
				withTS.SetUpdatedAt(now)
			}
		}
	}

	_, err := r.db.NewInsert().Model(&entities).Exec(ctx)
	return err
}

// Update modifies an existing entity
func (r *BunRepository[T, ID]) Update(ctx context.Context, entity *T) error {
	// Handle timestamps if the entity implements WithTimestamps
	if withTS, ok := any(entity).(repo.WithTimestamps); ok {
		withTS.SetUpdatedAt(time.Now())
	}

	_, err := r.db.NewUpdate().Model(entity).WherePK().Exec(ctx)
	return err
}

// UpdateMany modifies multiple entities based on conditions
func (r *BunRepository[T, ID]) UpdateMany(ctx context.Context, updates map[string]interface{}, opts *repo.QueryOptions) error {
	if len(updates) == 0 {
		return nil
	}

	// Handle timestamps if the entity implements WithTimestamps
	var model T
	if _, ok := any(&model).(repo.WithTimestamps); ok {
		if _, exists := updates["updated_at"]; !exists {
			updates["updated_at"] = time.Now()
		}
	}

	q := r.db.NewUpdate().Model(&model)

	// Apply updates
	for field, value := range updates {
		q = q.Set(field+" = ?", value)
	}

	// Apply query options
	q = applyUpdateQueryOptions(q, opts)

	_, err := q.Exec(ctx)
	return err
}

// Delete removes an entity by ID
func (r *BunRepository[T, ID]) Delete(ctx context.Context, id ID) error {
	var model T
	_, err := r.db.NewDelete().Model(&model).Where("id = ?", id).Exec(ctx)

	// If it's a soft delete and the model implements WithSoftDelete
	if err == nil {
		if withSD, ok := any(&model).(repo.WithSoftDelete); ok {
			now := time.Now()
			_, err = r.db.NewUpdate().Model(&model).
				Set("deleted_at = ?", now).
				Where("id = ?", id).
				Exec(ctx)
			withSD.SetDeletedAt(&now)
		}
	}

	return err
}

// DeleteMany removes entities based on conditions
func (r *BunRepository[T, ID]) DeleteMany(ctx context.Context, opts *repo.QueryOptions) error {
	var model T

	// If soft delete is supported
	if withSD, ok := any(&model).(repo.WithSoftDelete); ok {
		now := time.Now()
		q := r.db.NewUpdate().Model(&model).Set("deleted_at = ?", now)
		q = applyUpdateQueryOptions(q, opts)
		_, err := q.Exec(ctx)
		if err == nil {
			withSD.SetDeletedAt(&now)
		}
		return err
	}

	// Hard delete
	q := r.db.NewDelete().Model(&model)
	q = applyDeleteQueryOptions(q, opts)
	_, err := q.Exec(ctx)
	return err
}

// FindByID retrieves an entity by ID
func (r *BunRepository[T, ID]) FindByID(ctx context.Context, id ID, opts *repo.QueryOptions) (*T, error) {
	var model T
	q := r.db.NewSelect().Model(&model).Where("id = ?", id)

	// Apply query options
	q = applySelectQueryOptions(q, opts)

	// Handle soft delete
	if _, ok := any(&model).(repo.WithSoftDelete); ok {
		q = q.Where("deleted_at IS NULL")
	}

	err := q.Scan(ctx)
	if err != nil {
		return nil, err
	}
	return &model, nil
}

// FindOne retrieves a single entity based on conditions
func (r *BunRepository[T, ID]) FindOne(ctx context.Context, opts *repo.QueryOptions) (*T, error) {
	var model T
	q := r.db.NewSelect().Model(&model).Limit(1)

	// Apply query options
	q = applySelectQueryOptions(q, opts)

	// Handle soft delete
	if _, ok := any(&model).(repo.WithSoftDelete); ok {
		q = q.Where("deleted_at IS NULL")
	}

	err := q.Scan(ctx)
	if err != nil {
		return nil, err
	}
	return &model, nil
}

// FindAll retrieves multiple entities with query options
func (r *BunRepository[T, ID]) FindAll(ctx context.Context, opts *repo.QueryOptions) ([]T, error) {
	var models []T
	q := r.db.NewSelect().Model(&models)

	// Apply query options
	q = applySelectQueryOptions(q, opts)

	// Handle soft delete
	if len(models) > 0 {
		if _, ok := any(&models[0]).(repo.WithSoftDelete); ok {
			q = q.Where("deleted_at IS NULL")
		}
	}

	err := q.Scan(ctx)
	return models, err
}

// FindPaginated retrieves paginated results
func (r *BunRepository[T, ID]) FindPaginated(ctx context.Context, opts *repo.QueryOptions) (*repo.PaginatedResult[T], error) {
	var models []T

	// Create base query
	q := r.db.NewSelect().Model(&models)

	// Apply query options (without pagination for count)
	countQ := q.Clone()
	countQ = applySelectQueryOptions(countQ, opts.WithoutPagination())

	// Handle soft delete
	if len(models) > 0 {
		if _, ok := any(&models[0]).(repo.WithSoftDelete); ok {
			q = q.Where("deleted_at IS NULL")
			countQ = countQ.Where("deleted_at IS NULL")
		}
	}

	// Get total count
	totalCount, err := countQ.Count(ctx)
	if err != nil {
		return nil, err
	}

	// Apply pagination to the main query
	q = applySelectQueryOptions(q, opts)

	// Execute the query
	err = q.Scan(ctx)
	if err != nil {
		return nil, err
	}

	// Calculate pagination details
	var currentPage, perPage, totalPages int
	if opts != nil && opts.Pagination != nil {
		currentPage = opts.Pagination.Page
		perPage = opts.Pagination.PerPage
		if perPage > 0 {
			totalPages = int(totalCount) / perPage
			if int(totalCount)%perPage > 0 {
				totalPages++
			}
		}
	} else {
		currentPage = 1
		perPage = len(models)
		totalPages = 1
	}

	return &repo.PaginatedResult[T]{
		Items:       models,
		TotalCount:  int64(totalCount),
		CurrentPage: currentPage,
		PerPage:     perPage,
		TotalPages:  totalPages,
	}, nil
}

// Count returns the number of entities matching conditions
func (r *BunRepository[T, ID]) Count(ctx context.Context, opts *repo.QueryOptions) (int64, error) {
	var model T
	q := r.db.NewSelect().Model(&model)

	// Apply query options
	q = applySelectQueryOptions(q, opts)

	// Handle soft delete
	if _, ok := any(&model).(repo.WithSoftDelete); ok {
		q = q.Where("deleted_at IS NULL")
	}

	count, err := q.Count(ctx)
	return int64(count), err
}

// Exists checks if an entity exists based on conditions
func (r *BunRepository[T, ID]) Exists(ctx context.Context, opts *repo.QueryOptions) (bool, error) {
	count, err := r.Count(ctx, opts)
	return count > 0, err
}

// Raw executes a raw query and scans results into entities
func (r *BunRepository[T, ID]) Raw(ctx context.Context, query string, args ...interface{}) ([]T, error) {
	var models []T
	err := r.db.NewRaw(query, args...).Scan(ctx, &models)
	return models, err
}

// Transaction executes a function within a database transaction
func (r *BunRepository[T, ID]) Transaction(ctx context.Context, fn func(tx repo.Repository[T, ID]) error) error {
	return r.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		txRepo := &BunRepository[T, ID]{db: tx}
		return fn(txRepo)
	})
}

// Helper functions for different query types

func applySelectQueryOptions(q *bun.SelectQuery, opts *repo.QueryOptions) *bun.SelectQuery {
	if opts == nil {
		return q
	}

	// Apply filters
	for _, filter := range opts.Filters {
		q = q.Where(fmt.Sprintf("%s %s ?", filter.Field, filter.Operator), filter.Value)
	}

	// Apply sorting
	for _, sort := range opts.Sorts {
		q = q.OrderExpr(fmt.Sprintf("%s %s", sort.Field, sort.Direction))
	}

	// Apply pagination
	if opts.Pagination != nil {
		offset := (opts.Pagination.Page - 1) * opts.Pagination.PerPage
		q = q.Limit(opts.Pagination.PerPage).Offset(offset)
	}

	// Apply preload (relations)
	for _, relation := range opts.Preload {
		q = q.Relation(relation)
	}

	// Apply select fields
	if len(opts.Select) > 0 {
		q = q.Column(opts.Select...)
	}

	// Apply joins
	for _, join := range opts.Joins {
		q = q.Join(join)
	}

	// Apply raw conditions
	for _, condition := range opts.Conditions {
		switch cond := condition.(type) {
		case string:
			q = q.Where(cond)
		case []interface{}:
			if len(cond) > 0 {
				if sql, ok := cond[0].(string); ok {
					q = q.Where(sql, cond[1:]...)
				}
			}
		}
	}

	return q
}

func applyUpdateQueryOptions(q *bun.UpdateQuery, opts *repo.QueryOptions) *bun.UpdateQuery {
	if opts == nil {
		return q
	}

	// Apply filters
	for _, filter := range opts.Filters {
		q = q.Where(fmt.Sprintf("%s %s ?", filter.Field, filter.Operator), filter.Value)
	}

	// Apply raw conditions
	for _, condition := range opts.Conditions {
		switch cond := condition.(type) {
		case string:
			q = q.Where(cond)
		case []interface{}:
			if len(cond) > 0 {
				if sql, ok := cond[0].(string); ok {
					q = q.Where(sql, cond[1:]...)
				}
			}
		}
	}

	return q
}

func applyDeleteQueryOptions(q *bun.DeleteQuery, opts *repo.QueryOptions) *bun.DeleteQuery {
	if opts == nil {
		return q
	}

	// Apply filters
	for _, filter := range opts.Filters {
		q = q.Where(fmt.Sprintf("%s %s ?", filter.Field, filter.Operator), filter.Value)
	}

	// Apply raw conditions
	for _, condition := range opts.Conditions {
		switch cond := condition.(type) {
		case string:
			q = q.Where(cond)
		case []interface{}:
			if len(cond) > 0 {
				if sql, ok := cond[0].(string); ok {
					q = q.Where(sql, cond[1:]...)
				}
			}
		}
	}

	return q
}
