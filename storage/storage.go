package storage

import (
	"context"
	"database/sql"
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/domain"
	"TL-Data-Consumer/engine"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultRoutinesNum  = 2
	defaultWaitTime     = 5
	defaultDelayTime    = 4
	defaultMaxOpenConns = 20
	defaultMaxIdleConns = 5
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
		settings.Server.StorageRoutines = defaultRoutinesNum
	}
	if settings.Server.StorageWaitTime == 0 {
		settings.Server.StorageWaitTime = defaultWaitTime
	}
	if settings.Server.StorageDelayTime == 0 {
		settings.Server.StorageDelayTime = defaultDelayTime
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
	db.SetMaxOpenConns(defaultMaxOpenConns)
	db.SetMaxIdleConns(defaultMaxIdleConns)
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
	case schemas := <-s.engine.ReadMessages():
		// try to flush the schemas to the database
		s.tryFlush(schemas)

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
		case schemas := <-s.engine.ReadMessages():
			// try to flush the schemas to the database
			s.tryFlush(schemas)

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
func (s *Storage) tryFlush(schemas []domain.Schema) {
	for {
		// flush the schemas to database
		if err := s.flush(schemas); err != nil {
			fmt.Printf("flush schemas: %v\n", err)

			// sleep for a seconds and retry flush
			time.Sleep(s.settings.Server.StorageWaitTime * time.Second)
			continue
		}

		// update the total count of insertion
		totalInsertCount.Add(float64(len(schemas)))
		break
	}
}

// prepare the columns and values with statemetn
func (s *Storage) prepare(tx *sql.Tx, schemas []domain.Schema) error {
	// retrieve the table name and columns
	first := schemas[0]

	// preapare a copy In statement with table and columns
	stmt, err := tx.Prepare(pq.CopyIn(first.GetTable(), first.GetColumns()...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	// process each schema
	for _, schema := range schemas {
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
func (s *Storage) flush(schemas []domain.Schema) error {
	// begin a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	// 1. for error before commit, it rollback to the origin state
	// 2. for nothing after commit, it close the transaction
	defer tx.Rollback()

	// prepare the columns and values with statement
	if err := s.prepare(tx, schemas); err != nil {
		return err
	}

	// commit the transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
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
