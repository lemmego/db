package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

var (
	once     sync.Once
	instance *DatabaseManager
)

func ProvideConfig(cb func() *Config) *Config {
	return cb()
}

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

type Connection struct {
	*Config
	*sql.DB
}

type Model struct {
	ID        uint      `json:"id" gorm:"primaryKey"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func NewConnection(config *Config) *Connection {
	return &Connection{config, nil}
}

func (c *Connection) Open() *sql.DB {
	switch c.Config.Driver {
	case DialectSQLite:
		return NewSQLiteConnection(c.Config).Connect()
	case DialectMySQL:
		return NewMySQLConnection(c.Config).Connect()
	case DialectPgSQL:
		return NewPgSQLConnection(c.Config).Connect()
	}

	panic("unsupported driver")
}

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
	conn, ok := m.Get(name)
	if !ok {
		return errors.New(fmt.Sprintf("database: not found %s", name))
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
	for connName, _ := range m.All() {
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
