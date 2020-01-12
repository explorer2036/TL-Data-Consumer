package storage

import (
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/consul"
	"TL-Data-Consumer/engine"
	"TL-Data-Consumer/log"
	"TL-Data-Consumer/model"
	"context"
	"database/sql"
	"fmt"
	"strings"
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

	totalUpdateCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_update_count",
		Help: "The total count of update into database",
	})
)

func init() {
	prometheus.MustRegister(totalInsertCount)
	prometheus.MustRegister(totalUpdateCount)
}

// Storage represents the database insertion
type Storage struct {
	consuler *consul.Consul // consul for getting the fixed columns
	engine   *engine.Engine // the engine object
	settings *config.Config // the settings for storage
	db       *sql.DB        // database connection pool
	ready    chan struct{}  // mark the storage is ready
}

// NewStorage returns a new storage for database insertion
func NewStorage(settings *config.Config, engine *engine.Engine, consuler *consul.Consul) *Storage {
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
		consuler: consuler,
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
					log.Info("storage goroutine exit")
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
			log.Errorf("flush schemas: %v", err)

			// 1. if it happens database error, try flush again
			if strings.Contains(err.Error(), "connection refused") {
				// sleep for a seconds and retry flush
				time.Sleep(s.settings.Server.StorageWaitTime * time.Second)
				continue
			}

			// 2. if it happens batch insertion error, try to insert one by one
			if carrier.Action == model.InsertAction {
				for _, schema := range carrier.Schemas {
					if err := s.insertion(carrier.Table, schema); err != nil {
						log.Errorf("insertion %v: %v", schema, err)
						continue
					}
				}
			}

			// 3. if it happens updation error, output the error log
			if carrier.Action == model.UpdateAction {
				log.Errorf("updation %v: %v", carrier.Schemas, err)
			}
		}

		// update the total count of insertion
		totalInsertCount.Add(float64(len(carrier.Schemas)))
		break
	}
}

// split the json schema to set schema and where schema
func (s *Storage) splitSchema(schema model.JSONSchema, dataType string) (model.JSONSchema, model.JSONSchema) {
	set := model.JSONSchema{}

	// the columns for where
	columns := s.consuler.GetSchema(dataType).Common

	// define the search function
	search := func(keys []string, key string) bool {
		for _, k := range keys {
			if k == key {
				return true
			}
		}
		return false
	}

	where := model.JSONSchema{}
	// init the set and where json schema
	for key, val := range schema {
		if search(columns, key) == true {
			where[key] = val
		} else {
			set[key] = val
		}
	}

	return set, where
}

// format the update sql and args with set and where schemas
func (s *Storage) formatUpdateSQL(table string, set model.JSONSchema, where model.JSONSchema) (string, []interface{}) {
	var offset int
	// get the sorted keys and values for set
	setKeys := set.GetColumns()
	setVals := set.GetValues()

	// get the sorted keys and values for where
	whereKeys := where.GetColumns()
	whereVals := where.GetValues()

	offset++
	// init the begin sql
	begin := "UPDATE " + table + " SET"

	middle := " "
	// init the middle sql
	for _, key := range setKeys {
		middle = middle + fmt.Sprintf("%s = $%d,", key, offset)
		offset++
	}
	middle = strings.TrimRight(middle, ",")

	// init the end sql
	end := " WHERE "
	for _, key := range whereKeys {
		end = end + fmt.Sprintf("%s = $%d AND ", key, offset)
		offset++
	}
	end = strings.TrimRight(end, " AND ")

	// combine the begin, middle, end sql
	query := begin + middle + end

	var args []interface{}
	args = append(args, setVals...)
	args = append(args, whereVals...)

	return query, args
}

// prepare the columns and values with statement for updation
func (s *Storage) updation(tx *sql.Tx, carrier *model.SchemasCarrier) error {
	// loop the schemas to do updation
	for _, schema := range carrier.Schemas {
		// get the set and where schema
		set, where := s.splitSchema(schema, carrier.DataType)
		if len(set) == 0 {
			return fmt.Errorf("set schema are empty for %s", carrier.Table)
		}
		if len(where) == 0 {
			return fmt.Errorf("where schema are empty for %s", carrier.Table)
		}

		// prepare the update sql and args
		query, args := s.formatUpdateSQL(carrier.Table, set, where)

		// prepare a statement with query sql
		stmt, err := tx.Prepare(query)
		if err != nil {
			return err
		}

		// execute the statement with args
		if _, err := stmt.Exec(args...); err != nil {
			return err
		}

		// update the metric for updation operation
		totalUpdateCount.Add(1)
	}

	return nil
}

// prepare the statement for insertion
func (s *Storage) statement(tx *sql.Tx, table string, schema model.JSONSchema) error {
	// preapare a copy In statement with table and columns
	stmt, err := tx.Prepare(pq.CopyIn(table, schema.GetColumns()...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	// process the schema
	if _, err := stmt.Exec(schema.GetValues()...); err != nil {
		return err
	}
	if _, err := stmt.Exec(); err != nil {
		return err
	}

	return nil
}

// insert the schema to database
func (s *Storage) insertion(table string, schema model.JSONSchema) error {
	// begin a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	// 1. for error before commit, it rollback to the origin state
	// 2. for nothing after commit, it close the transaction
	defer tx.Rollback()

	// prepare the statement for the insertion
	if err := s.statement(tx, table, schema); err != nil {
		return err
	}

	// commit the transaction
	return tx.Commit()
}

// prepare the columns and values with statement for batch insertion
func (s *Storage) batchInsertion(tx *sql.Tx, carrier *model.SchemasCarrier) error {
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

	// prepare the columns and values with statement for insertion
	if carrier.Action == model.InsertAction {
		if err := s.batchInsertion(tx, carrier); err != nil {
			return err
		}
	}

	// prepare the columns and values with statement for updation
	if carrier.Action == model.UpdateAction {
		if err := s.updation(tx, carrier); err != nil {
			return err
		}
	}

	// commit the transaction
	return tx.Commit()
}

// IsReady check if the storage is ready
func (s *Storage) IsReady() {
	for i := 0; i < s.settings.Server.StorageRoutines; i++ {
		<-s.ready
	}
	log.Info("storage goroutines are ready")
}

// Close releases the hard resources
func (s *Storage) Close() {
	if s.db != nil {
		s.db.Close()
	}
}
