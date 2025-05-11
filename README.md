# Lemmego DB

A flexible and powerful database package for Go that provides a fluent interface for building SQL queries. This package supports multiple database dialects including SQLite, MySQL, PostgreSQL, and MS SQL Server.

## Features

- Support for multiple database dialects (SQLite, MySQL, PostgreSQL, MS SQL Server)
- Fluent query builder interface
- Transaction support
- Connection pooling
- Prepared statements
- Pagination support (both offset-based and cursor-based)
- Debug mode for query logging
- Type-safe query building
- Support for complex SQL operations (JOINs, GROUP BY, HAVING, etc.)

## Installation

```bash
go get github.com/lemmego/db
```

## Quick Start

### 1. Configure Database Connection

```go
import "github.com/lemmego/db"

// Create a new database configuration
config := &db.Config{
    ConnName: "default",
    Driver:   db.DialectSQLite, // or db.DialectMySQL, db.DialectPgSQL, db.DialectMsSQL
    Database: "your_database",
    Host:     "localhost",
    Port:     3306,
    User:     "your_username",
    Password: "your_password",
    Params:   "charset=utf8mb4&parseTime=True&loc=Local",
}

// Create and open a new connection
conn := db.NewConnection(config)
_, err := conn.Open()
if err != nil {
    panic(err)
}

// Add the connection to the database manager
db.DM().Add(config.ConnName, conn)
```

### 2. Basic Query Operations

#### Select Query

```go
// Simple select
var users []User
err := db.Query().
    Table("users").
    Select("*").
    Where(db.EQ("id", 1)).
    ScanAll(context.Background(), &users)

// Select with joins
err = db.Query().
    Table("users").
    Select("users.id", "users.name", "posts.title").
    Join("posts", "users.id = posts.user_id").
    Where(db.EQ("users.id", 1)).
    ScanAll(context.Background(), &users)
```

#### Insert Query

```go
// Single row insert
_, err := db.Query().
    Table("users").
    Insert([]string{"name", "email"}, [][]any{{"John Doe", "john@example.com"}}).
    Exec(context.Background())

// Multiple rows insert
_, err = db.Query().
    Table("users").
    Insert([]string{"name", "email"}, [][]any{
        {"John Doe", "john@example.com"},
        {"Jane Doe", "jane@example.com"},
    }).
    Exec(context.Background())
```

#### Update Query

```go
// Simple update
_, err := db.Query().
    Table("users").
    Update(map[string]any{
        "name": "Updated Name",
    }).
    Where(db.EQ("id", 1)).
    Exec(context.Background())

// Update multiple columns
_, err = db.Query().
    Table("users").
    Update(map[string]any{
        "name": "John",
        "email": "john@example.com",
    }).
    Where(db.EQ("id", 1)).
    Exec(context.Background())
```

#### Delete Query

```go
// Simple delete
_, err := db.Query().
    Table("users").
    Delete().
    Where(db.EQ("id", 1)).
    Exec(context.Background())
```

### 3. Advanced Features

#### Transactions

```go
err := db.Query().Transaction(context.Background(), func(qb *db.QueryBuilder) error {
    // Insert a user
    _, err := qb.Table("users").
        Insert([]string{"name", "email"}, [][]any{{"John Doe", "john@example.com"}}).
        Exec(context.Background())
    if err != nil {
        return err
    }

    // Update the user
    _, err = qb.Table("users").
        Update([]string{"name"}, []any{"Updated Name"}).
        Where(db.EQ("name", "John Doe")).
        Exec(context.Background())
    return err
})
```

#### Pagination

```go
// Offset-based pagination
var users []User
err := db.Query().
    Table("users").
    Select("*").
    Page(1, 10). // page 1, 10 items per page
    ScanAll(context.Background(), &users)

// Cursor-based pagination
err = db.Query().
    Table("users").
    Select("*").
    Cursor("last_id", "next", "id").
    ScanAll(context.Background(), &users)
```

#### Complex Conditions

```go
// Multiple conditions
err := db.Query().
    Table("users").
    Select("*").
    Where(db.OrCond(
        db.EQ("id", 1),
        db.EQ("name", "John"),
    )).
    ScanAll(context.Background(), &users)

// Complex conditions with AND/OR
err = db.Query().
    Table("users").
    Select("*").
    Where(db.AndCond(
        db.GT("age", 18),
        db.OrCond(
            db.EQ("status", "active"),
            db.EQ("status", "pending"),
        ),
    )).
    ScanAll(context.Background(), &users)
```

### 4. Debug Mode

```go
// Enable debug mode to log queries
err := db.Query().
    Table("users").
    Select("*").
    Debug(true).
    ScanAll(context.Background(), &users)
```

## Supported Database Dialects

- SQLite
- MySQL
- PostgreSQL
- MS SQL Server

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.