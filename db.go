package nap

import (
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
)

// DB is a logical database with multiple underlying physical databases
// forming a single master multiple slaves topology.
// Reads and writes are automatically directed to the correct physical db.
type DB struct {
	pdbs  []*sqlx.DB // Physical databases
	count uint64     // Monotonically incrementing counter on each query
}

// Open concurrently opens each underlying physical db.
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the master and the rest as slaves.
func Open(driverName, dataSourceNames string) (*DB, error) {
	conns := strings.Split(dataSourceNames, ";")
	db := &DB{pdbs: make([]*sqlx.DB, len(conns))}

	err := scatter(len(db.pdbs), func(i int) (err error) {
		db.pdbs[i], err = sqlx.Open(driverName, conns[i])
		return err
	})

	if err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *DB) Close() error {
	return scatter(len(db.pdbs), func(i int) error {
		return db.pdbs[i].Close()
	})
}

// Driver returns the physical database's underlying driver.
func (db *DB) Driver() driver.Driver {
	return db.pdbs[0].Driver()
}

// Begin starts a transaction on the master. The isolation level is dependent on the driver.
func (db *DB) Begin() (*sql.Tx, error) {
	return db.pdbs[0].Begin()
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the master as the underlying physical db.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.pdbs[0].Exec(query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping() error {
	return scatter(len(db.pdbs), func(i int) error {
		return db.pdbs[i].Ping()
	})
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying physical db.
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *DB) SetMaxIdleConns(n int) {
	for i := range db.pdbs {
		db.pdbs[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
	for i := range db.pdbs {
		db.pdbs[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	for i := range db.pdbs {
		db.pdbs[i].SetConnMaxLifetime(d)
	}
}

// Slave returns one of the physical databases which is a slave
func (db *DB) Slave() *sqlx.DB {
	return db.pdbs[db.slave(len(db.pdbs))]
}

// Master returns the master physical database
func (db *DB) Master() *sqlx.DB {
	return db.pdbs[0]
}

func (db *DB) slave(n int) int {
	if n <= 1 {
		return 0
	}
	return int(1 + (atomic.AddUint64(&db.count, 1) % uint64(n-1)))
}

// Preparex prepares a statement that connects to the master.
func (db *DB) Preparex(query string) (*sqlx.Stmt, error) {
	return db.Master().Preparex(query)
}

// PreparexSlave prepares a statement that connects with one of the slaves.
func (db *DB) PreparexSlave(query string) (*sqlx.Stmt, error) {
	return db.Slave().Preparex(query)
}

// Select performs a sqlx select against one of the slaves.
func (db *DB) Select(dest interface{}, query string, args ...interface{}) error {
	return db.Slave().Select(dest, query, args...)
}

// QueryxSlave performs an sqlx Queryx call against one of the slaves.
func (db *DB) QueryxSlave(query string, args ...interface{}) (*sqlx.Rows, error) {
	return db.Slave().Queryx(query, args...)
}
