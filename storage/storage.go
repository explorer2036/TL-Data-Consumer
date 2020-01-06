package storage

import (
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/engine"
	"TL-Data-Consumer/model"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// DefaultRoutinesNum - routines number for inserting
	DefaultRoutinesNum = 2
	// DefaultWaitTime - sleep time for inserting when happens error with database
	DefaultWaitTime = 5
	// DefaultDelayTime - delay time for goroutines consuming the channel
	DefaultDelayTime = 4
	// DefaultMaxOpenConns - max open connections for database
	DefaultMaxOpenConns = 20
	// DefaultMaxIdleConns - max idle connections for database
	DefaultMaxIdleConns = 5
)

var (
	totalInsertCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_insert_count",
		Help: "The total count of insert into database",
	})
)

func init() {
	prometheus.MustRegister(totalInsertCount)
}

// Storage represents the database insertion
type Storage struct {
	settings *config.Config

	db     *sql.DB        // database connection pool
	engine *engine.Engine // the engine object
	ready  chan struct{}  // mark the storage is ready
}

// NewStorage returns a new storage for database insertion
func NewStorage(settings *config.Config, engine *engine.Engine) *Storage {
	if settings.Server.StorageRoutines == 0 {
		settings.Server.StorageRoutines = DefaultRoutinesNum
	}
	if settings.Server.StorageWaitTime == 0 {
		settings.Server.StorageWaitTime = DefaultWaitTime
	}
	if settings.Server.StorageDelayTime == 0 {
		settings.Server.StorageDelayTime = DefaultDelayTime
	}

	// init the database connections
	source := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		settings.DB.Host,
		settings.DB.Port,
		settings.DB.User,
		settings.DB.Passwd,
		settings.DB.Name,
	)
	db, err := sql.Open("postgres", source)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(DefaultMaxOpenConns)
	db.SetMaxIdleConns(DefaultMaxIdleConns)
	if err := db.Ping(); err != nil {
		panic(err)
	}

	return &Storage{
		engine:   engine,
		db:       db,
		settings: settings,
		ready:    make(chan struct{}, settings.Server.StorageRoutines),
	}
}

// 1. handle the left data in stream
// 2. push the buffered schemas
func (s *Storage) afterCare() bool {
	delay := (s.settings.Server.StorageDelayTime)

	select {
	case carrier := <-s.engine.ReadMessages():
		// try to flush the schemas to the database
		s.tryFlush(carrier)

	case <-time.After(time.Second * delay): // delay several seconds
		return false
	}

	return true
}

func (s *Storage) handle(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// mark the goroutine started
	s.ready <- struct{}{}

	for {
		select {
		// read from the messages channel
		case carrier := <-s.engine.ReadMessages():
			// try to flush the schemas to the database
			s.tryFlush(carrier)

		case <-ctx.Done():
			// if the cancel signal is received
			// try to handle the left data, and push the buffered schemas
			for {
				if s.afterCare() == false {
					fmt.Println("storage goroutine exit")
					return
				}
			}
		}
	}
}

// Start create goroutines to parse messages and build structure for database
func (s *Storage) Start(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < s.settings.Server.StorageRoutines; i++ {
		wg.Add(1)
		go s.handle(ctx, wg)
	}
}

// try to flush the schemas, if failed, sleep for a moment
func (s *Storage) tryFlush(carrier *model.SchemasCarrier) {
	for {
		// flush the schemas to database
		if err := s.flush(carrier); err != nil {
			fmt.Printf("flush schemas: %v\n", err)

			// sleep for a seconds and retry flush
			time.Sleep(s.settings.Server.StorageWaitTime * time.Second)
			continue
		}

		// update the total count of insertion
		totalInsertCount.Add(float64(len(carrier.Schemas)))
		break
	}
}

// prepare the columns and values with statemetn
func (s *Storage) prepare(tx *sql.Tx, carrier *model.SchemasCarrier) error {
	// retrieve the table name and columns
	first := carrier.Schemas[0]

	// preapare a copy In statement with table and columns
	stmt, err := tx.Prepare(pq.CopyIn(carrier.Table, first.GetColumns()...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	// process each schema
	for _, schema := range carrier.Schemas {
		if _, err := stmt.Exec(schema.GetValues()...); err != nil {
			return err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		return err
	}

	return nil
}

// flush the batch schemas into database
func (s *Storage) flush(carrier *model.SchemasCarrier) error {
	// begin a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	// 1. for error before commit, it rollback to the origin state
	// 2. for nothing after commit, it close the transaction
	defer tx.Rollback()

	// prepare the columns and values with statement
	if err := s.prepare(tx, carrier); err != nil {
		return err
	}

	// commit the transaction
	return tx.Commit()
}

// IsReady check if the storage is ready
func (s *Storage) IsReady() {
	for i := 0; i < s.settings.Server.StorageRoutines; i++ {
		<-s.ready
	}
	fmt.Println("storage goroutines are ready")
}

// Close releases the hard resources
func (s *Storage) Close() {
	if s.db != nil {
		s.db.Close()
	}
}
