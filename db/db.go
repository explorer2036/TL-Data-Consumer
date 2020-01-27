package db

import (
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/consul"
	"TL-Data-Consumer/log"
	"TL-Data-Consumer/model"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"
)

const (
	// DefaultMaxOpenConns - max open connections for database
	DefaultMaxOpenConns = 20
	// DefaultMaxIdleConns - max idle connections for database
	DefaultMaxIdleConns = 5
	// DefaultWaitTime - sleep time for inserting when happens error with database
	DefaultWaitTime = 5
)

var (
	// ErrorNoAffected - no affected lines for for updation
	ErrorNoAffected = errors.New("no affected lines for updation")
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

	totalUpsertCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_upsert_count",
		Help: "The total count of upsert into database",
	})

	totalAddCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_add_count",
		Help: "The total count of add into database",
	})

	totalDelCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_del_count",
		Help: "The total count of del from database",
	})
)

func init() {
	prometheus.MustRegister(totalInsertCount)
	prometheus.MustRegister(totalUpdateCount)
	prometheus.MustRegister(totalUpsertCount)
	prometheus.MustRegister(totalAddCount)
	prometheus.MustRegister(totalDelCount)
}

// Handler for db operation
type Handler struct {
	consuler *consul.Consul // consul for getting the fixed columns
	settings *config.Config // the settings for storage
	db       *sql.DB        // database connection pool

}

// NewHandler returns a new db handler
func NewHandler(settings *config.Config, consuler *consul.Consul) *Handler {
	if settings.Server.StorageWaitTime == 0 {
		settings.Server.StorageWaitTime = DefaultWaitTime
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

	return &Handler{
		consuler: consuler,
		db:       db,
		settings: settings,
	}
}

// split the json schema to set schema and where schema
func (d *Handler) splitSchema(schema model.JSONSchema, dataType string) (model.JSONSchema, model.JSONSchema) {
	set := model.JSONSchema{}

	// the columns for where
	columns := d.consuler.GetSchema(dataType).Common

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
func (d *Handler) formatUpdateSQL(table string, set model.JSONSchema, where model.JSONSchema) (string, []interface{}) {
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

// format the delete sql with where schemas
func (d *Handler) formatDeleteSQL(table string, where model.JSONSchema) (string, []interface{}) {
	var offset int

	// get the sorted keys and values for where
	whereKeys := where.GetColumns()
	whereVals := where.GetValues()

	offset++
	// init the begin sql
	begin := "DELETE FROM " + table

	// init the end sql
	end := " WHERE "
	for _, key := range whereKeys {
		end = end + fmt.Sprintf("%s = $%d AND ", key, offset)
		offset++
	}
	end = strings.TrimRight(end, " AND ")

	// combine the begin, end sql
	query := begin + end

	var args []interface{}
	args = append(args, whereVals...)

	return query, args
}

// format the query sql for count(*) with where schemas
func (d *Handler) formatQueryCountSQL(table string, where model.JSONSchema) (string, []interface{}) {
	var offset int

	// get the sorted keys and values for where
	whereKeys := where.GetColumns()
	whereVals := where.GetValues()

	offset++
	// init the begin sql
	begin := "SELECT COUNT(*) FROM " + table

	// init the end sql
	end := " WHERE "
	for _, key := range whereKeys {
		end = end + fmt.Sprintf("%s = $%d AND ", key, offset)
		offset++
	}
	end = strings.TrimRight(end, " AND ")

	// combine the begin, end sql
	query := begin + end

	var args []interface{}
	args = append(args, whereVals...)

	return query, args
}

// prepare the columns and values with statement for upsetion
func (d *Handler) upsertion(tx *sql.Tx, carrier *model.SchemasCarrier) error {
	// loop the schemas to do updation
	for _, schema := range carrier.Schemas {
		// get the set and where schema
		set, where := d.splitSchema(schema, carrier.DataType)
		if len(set) == 0 {
			return fmt.Errorf("set schema are empty for %s", carrier.Table)
		}
		if len(where) == 0 {
			return fmt.Errorf("where schema are empty for %s", carrier.Table)
		}

		// prepare the update sql and args
		query, args := d.formatUpdateSQL(carrier.Table, set, where)

		// prepare a statement with query sql
		stmt, err := tx.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()

		// execute the statement with args
		result, err := stmt.Exec(args...)
		if err != nil {
			return err
		}
		// if the affected rows are zero, which means there is no record for updation
		if affected, _ := result.RowsAffected(); affected == 0 {
			// preapare a copy In statement with table and columns
			stmt, err = tx.Prepare(pq.CopyIn(carrier.Table, schema.GetColumns()...))
			if err != nil {
				return err
			}

			// process the insertion with the schema
			if _, err = stmt.Exec(schema.GetValues()...); err != nil {
				return err
			}
			if _, err = stmt.Exec(); err != nil {
				return err
			}
		}

		// update the metric for upsertion operation
		totalUpsertCount.Add(1)

		// handle only one upsertion every time
		break
	}

	return nil
}

// prepare the columns and values with statement for updation
func (d *Handler) updation(tx *sql.Tx, carrier *model.SchemasCarrier) error {
	// loop the schemas to do updation
	for _, schema := range carrier.Schemas {
		// get the set and where schema
		set, where := d.splitSchema(schema, carrier.DataType)
		if len(set) == 0 {
			return fmt.Errorf("set schema are empty for %s", carrier.Table)
		}
		if len(where) == 0 {
			return fmt.Errorf("where schema are empty for %s", carrier.Table)
		}

		// prepare the update sql and args
		query, args := d.formatUpdateSQL(carrier.Table, set, where)

		// prepare a statement with query sql
		stmt, err := tx.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()

		// execute the statement with args
		result, err := stmt.Exec(args...)
		if err != nil {
			return err
		}
		// if the affected rows are zero, which means there is no record for updation
		if affected, _ := result.RowsAffected(); affected == 0 {
			return ErrorNoAffected
		}

		// update the metric for updation operation
		totalUpdateCount.Add(1)

		// handle only one updation every time
		break
	}

	return nil
}

// prepare the statement for insertion
func (d *Handler) statement(tx *sql.Tx, table string, schema model.JSONSchema) error {
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

// prepare the columns and values with statement for deletion
func (d *Handler) deletion(tx *sql.Tx, carrier *model.SchemasCarrier) error {
	// loop the schemas to do deletion
	for _, schema := range carrier.Schemas {
		// get the where schema
		_, where := d.splitSchema(schema, carrier.DataType)
		if len(where) == 0 {
			return fmt.Errorf("where schema are empty for %s", carrier.Table)
		}

		// prepare the update sql and args
		query, args := d.formatDeleteSQL(carrier.Table, where)

		// prepare a statement with query sql
		stmt, err := tx.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()

		// execute the statement with args
		if _, err := stmt.Exec(args...); err != nil {
			return err
		}

		// update the metric for deletion operation
		totalDelCount.Add(1)

		// handle only one deletion every time
		break
	}

	return nil
}

// prepare the columns and values with statement for addition
func (d *Handler) addition(tx *sql.Tx, carrier *model.SchemasCarrier) error {
	// loop the schemas to do addition
	for _, schema := range carrier.Schemas {
		// get the where schema
		_, where := d.splitSchema(schema, carrier.DataType)
		if len(where) == 0 {
			return fmt.Errorf("where schema are empty for %s", carrier.Table)
		}

		// prepare the update sql and args
		query, args := d.formatQueryCountSQL(carrier.Table, where)

		// prepare a statement with query sql
		stmt, err := tx.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()

		var count int
		// execute the statement with args
		if err := stmt.QueryRow(args...).Scan(&count); err != nil && err != sql.ErrNoRows {
			return err
		}
		if count > 0 {
			return nil
		}

		// prepare the statement for the insertion
		if err := d.statement(tx, carrier.Table, schema); err != nil {
			return err
		}

		// update the metric for addition operation
		totalAddCount.Add(1)

		// handle only one addition every time
		break
	}

	return nil
}

// insert the schema to database
func (d *Handler) insertion(table string, schema model.JSONSchema) error {
	// begin a transaction
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	// 1. for error before commit, it rollback to the origin state
	// 2. for nothing after commit, it close the transaction
	defer tx.Rollback()

	// prepare the statement for the insertion
	if err := d.statement(tx, table, schema); err != nil {
		return err
	}

	// commit the transaction
	return tx.Commit()
}

// prepare the columns and values with statement for batch insertion
func (d *Handler) batchInsertion(tx *sql.Tx, carrier *model.SchemasCarrier) error {
	// retrieve the table name and columns
	first := carrier.Schemas[0]

	// preapare a copy In statement with table and columns
	stmt, err := tx.Prepare(pq.CopyIn(carrier.Table, first.GetColumns()...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	// prepare the statement with the values for each schemas
	for _, schema := range carrier.Schemas {
		if _, err := stmt.Exec(schema.GetValues()...); err != nil {
			return err
		}
	}
	// execute the statement
	if _, err := stmt.Exec(); err != nil {
		return err
	}

	// update the total count of insertion
	totalInsertCount.Add(float64(len(carrier.Schemas)))

	return nil
}

// flush the batch schemas into database
func (d *Handler) flush(carrier *model.SchemasCarrier) error {
	// begin a transaction
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	// 1. for error before commit, it rollback to the origin state
	// 2. for nothing after commit, it close the transaction
	defer tx.Rollback()

	switch carrier.Action {
	case model.InsertAction:
		// prepare the columns and values with statement for insertion
		err = d.batchInsertion(tx, carrier)

	case model.AddAction:
		// prepare the columns and values with statement for addition
		err = d.addition(tx, carrier)

	case model.DelAction:
		// prepare the columns and values with statement for deletion
		err = d.deletion(tx, carrier)

	case model.UpdateAction:
		// prepare the columns and values with statement for updation
		err = d.upsertion(tx, carrier)
	}
	if err != nil {
		return err
	}

	// commit the transaction
	return tx.Commit()
}

// try to flush the schemas, if failed, sleep for a moment
func (d *Handler) TryFlush(carrier *model.SchemasCarrier) {
	for {
		// flush the schemas to database
		if err := d.flush(carrier); err != nil {
			log.Errorf("flush schemas: %v", err)

			// 1. if it happens database error, try flush again
			if strings.Contains(err.Error(), "connection refused") {
				// sleep for a seconds and retry flush
				time.Sleep(d.settings.Server.StorageWaitTime * time.Second)
				continue
			}

			// 2. if it happens batch insertion error, try to insert one by one
			if carrier.Action == model.InsertAction {
				for _, schema := range carrier.Schemas {
					if err := d.insertion(carrier.Table, schema); err != nil {
						log.Errorf("insertion %v: %v", schema, err)
						continue
					}
				}
			}
		}

		break
	}
}

// Lookup try to fetch the id for source and path
func (d *Handler) Lookup(source string, path string) (id int, err error) {
	var tx *sql.Tx

	// begin a transaction
	tx, err = d.db.Begin()
	if err != nil {
		return
	}
	// 1. for error before commit, it rollback to the origin state
	// 2. for nothing after commit, it close the transaction
	defer tx.Rollback()

	// prepare the statement for the insertion
	id, err = d.lookup(tx, source, path)
	if err != nil {
		return
	}

	// commit the transaction
	err = tx.Commit()

	return
}

func (d *Handler) lookup(tx *sql.Tx, source string, path string) (id int, err error) {
	var stmt *sql.Stmt
	// prepare a statement with query sql
	stmt, err = tx.Prepare("select id from tl_source_path where source=$1 and path=$2")
	if err != nil {
		log.Infof("%v", err)
		return
	}
	// execute the statement
	if err = stmt.QueryRow(source, path).Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			// generate the sequence id
			stmt, err = tx.Prepare("select nextval('source_path_id_sequence')")
			if err != nil {
				log.Infof("%v", err)
				return
			}
			if err = stmt.QueryRow().Scan(&id); err != nil {
				log.Infof("%v", err)
				return
			}

			// insert the record into database
			stmt, err = tx.Prepare("insert into tl_source_path(id, source, path) values($1, $2, $3)")
			if err != nil {
				log.Infof("%v", err)
				return
			}
			var result sql.Result
			result, err = stmt.Exec(id, source, path)
			if err != nil {
				log.Infof("%v", err)
				return
			}

			// no rows affected
			if affected, _ := result.RowsAffected(); affected == 0 {
				err = fmt.Errorf("no rows affected")
			}
		}
	}

	return
}

// Close the db
func (d *Handler) Close() {
	if d.db != nil {
		d.db.Close()
	}
}
