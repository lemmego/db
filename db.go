package db

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)

var (
	once                  sync.Once
	instance              *DatabaseManager
	ErrConnectionNotFound = errors.New("database connection not found")
)

// Config contains the configuration for the database connection
type Config struct {
	ConnName string
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Params   string
}

// DataSource represents the data source configuration for a database connection
func (c *Config) DataSource() *DataSource {
	return &DataSource{
		Dialect:  c.Driver,
		Host:     c.Host,
		Port:     strconv.Itoa(c.Port),
		Username: c.User,
		Password: c.Password,
		Name:     c.Database,
		Params:   c.Params,
	}
}

// DSN returns the Data Source Name (DSN) for the database connection
func (c *Config) DSN() string {
	dsn, err := c.DataSource().String()
	if err != nil {
		panic(err)
	}

	return dsn
}

// DatabaseManager holds connections to various database instances
type DatabaseManager struct {
	mutex       sync.RWMutex
	connections map[string]*Connection
}

// DM returns the singleton instance of DatabaseManager
func DM() *DatabaseManager {
	once.Do(func() {
		instance = &DatabaseManager{
			connections: make(map[string]*Connection),
		}
	})
	return instance
}

// Add adds a new database connection to the manager
func (m *DatabaseManager) Add(name string, conn *Connection) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.connections[name] = conn
}

// Get retrieves a database connection from the manager
func (m *DatabaseManager) Get(name ...string) (*Connection, bool) {
	connName := "default"
	if len(name) > 0 {
		connName = name[0]
	}
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	conn, found := m.connections[connName]
	return conn, found
}

// Remove closes and removes a database connection from the manager
func (m *DatabaseManager) Remove(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	conn, found := m.connections[name]
	if !found {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, name)
	}

	err := conn.Close()
	if err != nil {
		return err
	}

	delete(m.connections, name)
	return nil
}

// All returns all the connections
func (m *DatabaseManager) All() map[string]*Connection {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.connections
}

// RemoveAll closes and removes all the existing connections
func (m *DatabaseManager) RemoveAll() error {
	for connName := range m.All() {
		err := m.Remove(connName)
		if err != nil {
			return err
		}
	}
	return nil
}

// Get performs a type check on the retrieved database connection from the singleton instance
// If no name is provided, it defaults to "default"
func Get(name ...string) *Connection {
	connName := "default"
	if len(name) > 0 {
		connName = name[0]
	}

	conn, found := instance.Get(connName)
	if !found {
		panic(fmt.Sprintf("db connection '%s' not found", connName))
	}

	return conn
}

// Conn sets the given connection for the QueryBuilder
func (qb *QueryBuilder) Conn(connName ...string) *QueryBuilder {
	qb.conn = Get(connName...)
	return qb
}

// Query creates a new QueryBuilder instance with the specified connection
func Query(connName ...string) *QueryBuilder {
	conn := Get(connName...)
	return NewQueryBuilder(conn)
}

// QueryFromConn creates a new QueryBuilder instance from an existing connection
func QueryFromConn(conn *Connection) *QueryBuilder {
	return NewQueryBuilder(conn)
}
