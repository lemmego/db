package gorm

import (
	"context"
	"fmt"
	"github.com/lemmego/db/repo"
	"time"

	"gorm.io/gorm"
)

// GormRepository is a GORM-based implementation of the Repository interface.
type GormRepository[T any, ID comparable] struct {
	db         *gorm.DB
	model      *T
	primaryKey string // Name of the primary key field
	softDelete bool   // Whether the model supports soft deletes
	timestamps bool   // Whether the model supports timestamps
}

// NewGormRepo creates a new GORM repository.
func NewGormRepo[T any, ID comparable](db *gorm.DB, primaryKey string) *GormRepository[T, ID] {
	var model T
	gormRepo := &GormRepository[T, ID]{
		db:         db,
		model:      &model,
		primaryKey: primaryKey,
	}

	// Check for soft delete and timestamp support
	if _, ok := any(model).(repo.WithSoftDelete); ok {
		gormRepo.softDelete = true
	}
	if _, ok := any(model).(repo.WithTimestamps); ok {
		gormRepo.timestamps = true
	}

	return gormRepo
}

// applyQueryOptions applies filters, sorts, pagination, preload, etc., to a GORM query.
func (r *GormRepository[T, ID]) applyQueryOptions(query *gorm.DB, opts *repo.QueryOptions) *gorm.DB {
	if opts == nil {
		return query
	}

	// Apply filters
	for _, filter := range opts.Filters {
		switch filter.Operator {
		case "=":
			query = query.Where(fmt.Sprintf("%s = ?", filter.Field), filter.Value)
		case ">":
			query = query.Where(fmt.Sprintf("%s > ?", filter.Field), filter.Value)
		case "<":
			query = query.Where(fmt.Sprintf("%s < ?", filter.Field), filter.Value)
		case ">=":
			query = query.Where(fmt.Sprintf("%s >= ?", filter.Field), filter.Value)
		case "<=":
			query = query.Where(fmt.Sprintf("%s <= ?", filter.Field), filter.Value)
		case "!=":
			query = query.Where(fmt.Sprintf("%s != ?", filter.Field), filter.Value)
		case "LIKE":
			query = query.Where(fmt.Sprintf("%s LIKE ?", filter.Field), filter.Value)
		case "IN":
			query = query.Where(fmt.Sprintf("%s IN ?", filter.Field), filter.Value)
		default:
			// Handle custom operators via ExtraOptions if needed
			if customClause, ok := opts.ExtraOptions[filter.Operator]; ok {
				query = query.Where(customClause, filter.Value)
			}
		}
	}

	// Apply raw conditions
	for _, condition := range opts.Conditions {
		query = query.Where(condition)
	}

	// Apply sorting
	for _, sort := range opts.Sorts {
		query = query.Order(fmt.Sprintf("%s %s", sort.Field, sort.Direction))
	}

	// Apply pagination
	if opts.Pagination != nil {
		offset := (opts.Pagination.Page - 1) * opts.Pagination.PerPage
		query = query.Offset(offset).Limit(opts.Pagination.PerPage)
	}

	// Apply preloading
	for _, preload := range opts.Preload {
		query = query.Preload(preload)
	}

	// Apply field selection
	if len(opts.Select) > 0 {
		query = query.Select(opts.Select)
	}

	// Apply joins
	for _, join := range opts.Joins {
		query = query.Joins(join)
	}

	// Apply extra GORM-specific options
	if len(opts.ExtraOptions) > 0 {
		for key, value := range opts.ExtraOptions {
			if key == "scopes" {
				if scopes, ok := value.([]func(*gorm.DB) *gorm.DB); ok {
					for _, scope := range scopes {
						query = query.Scopes(scope)
					}
				}
			}
			// Add more GORM-specific options as needed
		}
	}

	return query
}

// Create inserts a new entity.
func (r *GormRepository[T, ID]) Create(ctx context.Context, entity *T) error {
	query := r.db.WithContext(ctx)
	if r.timestamps {
		if ts, ok := any(*entity).(repo.WithTimestamps); ok {
			now := time.Now()
			ts.SetCreatedAt(now)
			ts.SetUpdatedAt(now)
		}
	}
	return query.Create(entity).Error
}

// CreateMany inserts multiple entities.
func (r *GormRepository[T, ID]) CreateMany(ctx context.Context, entities []*T) error {
	if len(entities) == 0 {
		return nil
	}
	query := r.db.WithContext(ctx)
	if r.timestamps {
		for _, entity := range entities {
			if ts, ok := any(*entity).(repo.WithTimestamps); ok {
				now := time.Now()
				ts.SetCreatedAt(now)
				ts.SetUpdatedAt(now)
			}
		}
	}
	return query.Create(entities).Error
}

// Update modifies an existing entity.
func (r *GormRepository[T, ID]) Update(ctx context.Context, entity *T) error {
	query := r.db.WithContext(ctx)
	if r.timestamps {
		if ts, ok := any(*entity).(repo.WithTimestamps); ok {
			ts.SetUpdatedAt(time.Now())
		}
	}
	return query.Save(entity).Error
}

// UpdateMany modifies multiple entities based on conditions.
func (r *GormRepository[T, ID]) UpdateMany(ctx context.Context, updates map[string]interface{}, opts *repo.QueryOptions) error {
	query := r.db.WithContext(ctx).Model(r.model)
	query = r.applyQueryOptions(query, opts)
	if r.timestamps {
		updates["updated_at"] = time.Now()
	}
	if r.softDelete {
		query = query.Unscoped()
	}
	return query.Updates(updates).Error
}

// Delete removes an entity by ID.
func (r *GormRepository[T, ID]) Delete(ctx context.Context, id ID) error {
	query := r.db.WithContext(ctx).Where(fmt.Sprintf("%s = ?", r.primaryKey), id)
	if r.softDelete {
		if _, ok := any(*r.model).(repo.WithSoftDelete); ok {
			return query.Update("deleted_at", time.Now()).Error
		}
	}
	return query.Delete(r.model).Error
}

// DeleteMany removes entities based on conditions.
func (r *GormRepository[T, ID]) DeleteMany(ctx context.Context, opts *repo.QueryOptions) error {
	query := r.db.WithContext(ctx).Model(r.model)
	query = r.applyQueryOptions(query, opts)
	if r.softDelete {
		if _, ok := any(*r.model).(repo.WithSoftDelete); ok {
			return query.Update("deleted_at", time.Now()).Error
		}
	}
	return query.Delete(r.model).Error
}

// FindByID retrieves an entity by ID.
func (r *GormRepository[T, ID]) FindByID(ctx context.Context, id ID, opts *repo.QueryOptions) (*T, error) {
	var entity T
	query := r.db.WithContext(ctx).Model(r.model).Where(fmt.Sprintf("%s = ?", r.primaryKey), id)
	query = r.applyQueryOptions(query, opts)
	if err := query.First(&entity).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &entity, nil
}

// FindOne retrieves a single entity based on conditions.
func (r *GormRepository[T, ID]) FindOne(ctx context.Context, opts *repo.QueryOptions) (*T, error) {
	var entity T
	query := r.db.WithContext(ctx).Model(r.model)
	query = r.applyQueryOptions(query, opts)
	if err := query.First(&entity).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &entity, nil
}

// FindAll retrieves multiple entities with query options.
func (r *GormRepository[T, ID]) FindAll(ctx context.Context, opts *repo.QueryOptions) ([]T, error) {
	var entities []T
	query := r.db.WithContext(ctx).Model(r.model)
	query = r.applyQueryOptions(query, opts)
	if err := query.Find(&entities).Error; err != nil {
		return nil, err
	}
	return entities, nil
}

// FindPaginated retrieves paginated results.
func (r *GormRepository[T, ID]) FindPaginated(ctx context.Context, opts *repo.QueryOptions) (*repo.PaginatedResult[T], error) {
	result := &repo.PaginatedResult[T]{}
	query := r.db.WithContext(ctx).Model(r.model)

	// Count total items
	countQuery := r.db.WithContext(ctx).Model(r.model)
	countQuery = r.applyQueryOptions(countQuery, opts)
	if err := countQuery.Count(&result.TotalCount).Error; err != nil {
		return nil, err
	}

	if result.TotalCount == 0 {
		return result, nil
	}

	// Fetch paginated items
	query = r.applyQueryOptions(query, opts)
	var entities []T
	if err := query.Find(&entities).Error; err != nil {
		return nil, err
	}

	result.Items = entities
	if opts.Pagination != nil {
		result.CurrentPage = opts.Pagination.Page
		result.PerPage = opts.Pagination.PerPage
		result.TotalPages = int((result.TotalCount + int64(result.PerPage) - 1) / int64(result.PerPage))
	}

	return result, nil
}

// Count returns the number of entities matching conditions.
func (r *GormRepository[T, ID]) Count(ctx context.Context, opts *repo.QueryOptions) (int64, error) {
	var count int64
	query := r.db.WithContext(ctx).Model(r.model)
	query = r.applyQueryOptions(query, opts)
	if err := query.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// Exists checks if an entity exists based on conditions.
func (r *GormRepository[T, ID]) Exists(ctx context.Context, opts *repo.QueryOptions) (bool, error) {
	count, err := r.Count(ctx, opts)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Raw executes a raw query and scans results into entities.
func (r *GormRepository[T, ID]) Raw(ctx context.Context, query string, args ...interface{}) ([]T, error) {
	var entities []T
	if err := r.db.WithContext(ctx).Raw(query, args...).Scan(&entities).Error; err != nil {
		return nil, err
	}
	return entities, nil
}

// Transaction executes a function within a database transaction.
func (r *GormRepository[T, ID]) Transaction(ctx context.Context, fn func(tx repo.Repository[T, ID]) error) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := &GormRepository[T, ID]{
			db:         tx,
			model:      r.model,
			primaryKey: r.primaryKey,
			softDelete: r.softDelete,
			timestamps: r.timestamps,
		}
		return fn(txRepo)
	})
}
