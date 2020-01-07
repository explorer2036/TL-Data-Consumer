package engine

import (
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/consul"
	"TL-Data-Consumer/kafka"
	"TL-Data-Consumer/model"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	// DefaultNumberOfRoutines - number of the goroutines, for handling messages(default 2)
	DefaultNumberOfRoutines = 2
	// DefaultBatchCount - batch size for collecting(default 10)
	DefaultBatchCount = 10
	// DefaultStreamBuffer - queue size for all engine goroutines(default 100)
	DefaultStreamBuffer = 100
	// DefaultDelayTime - waiting for reading channel when receive exit signals(default 2s)
	DefaultDelayTime = 2
	// DefaultRefreshTime - use a timer to refresh the unreached buffer to storage periodly(default 5s)
	DefaultRefreshTime = 5

	// SpecialJSONField - the json field uses for nested parsing
	SpecialJSONField = "data"
	// JSONDataTypeField - the json field represents for different type of messages
	JSONDataTypeField = "dtype"
	// JSONActionField - the json field represents for different operation
	JSONActionField = "action"
)

// Engine represents the pipeline for handling messages
type Engine struct {
	consumer *kafka.Consumer // consumer for message stream channel
	consuler *consul.Consul  // consul for parsing the json schema with table schema

	settings *config.Config                   // server configuration
	cache    map[string]*model.SchemasCarrier // cache for different type of data,
	cacheMux sync.RWMutex                     // mutex for cache
	stream   chan *model.SchemasCarrier       // the required json schemas for database
	ready    chan struct{}                    // mark the engine is ready
}

// NewEngine returns a new engine
func NewEngine(settings *config.Config, consumer *kafka.Consumer, consuler *consul.Consul) *Engine {
	if settings.Server.EngineRoutines == 0 {
		settings.Server.EngineRoutines = DefaultNumberOfRoutines
	}
	if settings.Server.EngineBuffer == 0 {
		settings.Server.EngineBuffer = DefaultStreamBuffer
	}
	if settings.Server.EngineBatch == 0 {
		settings.Server.EngineBatch = DefaultBatchCount
	}
	if settings.Server.EngineDelayTime == 0 {
		settings.Server.EngineDelayTime = DefaultDelayTime
	}
	if settings.Server.EngineRefreshTime == 0 {
		settings.Server.EngineRefreshTime = DefaultRefreshTime
	}

	return &Engine{
		settings: settings,
		consumer: consumer,
		consuler: consuler,
		cache:    make(map[string]*model.SchemasCarrier),
		ready:    make(chan struct{}, settings.Server.EngineRoutines),
		stream:   make(chan *model.SchemasCarrier, settings.Server.EngineBuffer),
	}
}

// parse the nested json schema
func (e *Engine) nested(schema model.JSONSchema) (model.JSONSchema, error) {
	// check the json field "data"
	value, exist := schema[SpecialJSONField]
	if !exist {
		return nil, fmt.Errorf("json field %s is not found", SpecialJSONField)
	}

	// check if the reflect type is map[string]interface{}
	subSchema, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid json field: %s, type: %v", SpecialJSONField, reflect.TypeOf(value))
	}

	return subSchema, nil
}

// merge the root json schema and nested json schema
func (e *Engine) merge(root model.JSONSchema, nested model.JSONSchema) model.JSONSchema {
	result := model.JSONSchema{}

	for key, val := range root {
		if key == SpecialJSONField {
			continue
		}
		result[key] = val
	}

	for key, val := range nested {
		result[key] = val
	}

	return result
}

// create a new json schema based on the unmarshalled schema
// - the new json schema should be care of the nested json schema in data field
func (e *Engine) format(schema model.JSONSchema) (model.JSONSchema, error) {
	// parse the nested json schema
	nestedSchema, err := e.nested(schema)
	if err != nil {
		return nil, err
	}

	// merge the root schema and nested schema
	return e.merge(schema, nestedSchema), nil
}

// get json field's value
func (e *Engine) getJSONField(schema model.JSONSchema, name string) (string, error) {
	// get the value of the field name
	value, exist := schema[name]
	if !exist {
		return "", fmt.Errorf("%s is not found", name)
	}

	// check if the field type is string
	if reflect.TypeOf(value).Kind() != reflect.String {
		return "", fmt.Errorf("invalid type for %s", name)
	}

	// check if the value is empty
	s := value.(string)
	if s == "" {
		return "", fmt.Errorf("value for %s is empty", name)
	}

	return s, nil
}

// parse the json message -> json schema -> cached table schema
func (e *Engine) parse(m *kafka.Message) {
	schema := model.JSONSchema{}

	// unmarshal the message to json schema
	if err := json.Unmarshal(m.Data, &schema); err != nil {
		fmt.Printf("unmarshal message %v: %v\n", string(m.Data), err)
		return
	}

	// create a new json schema based on the unmarshaled schema
	newSchema, err := e.format(schema)
	if err != nil {
		fmt.Printf("format json schema %v: %v\n", string(m.Data), err)
		return
	}

	// get the value of json filed 'dtype'
	dataType, err := e.getJSONField(newSchema, JSONDataTypeField)
	if err != nil {
		fmt.Printf("get json field: %v\n", err)
		return
	}

	// get the value of json field 'action'
	action, err := e.getJSONField(newSchema, JSONActionField)
	if err != nil {
		fmt.Printf("get json field: %v\n", err)
		return
	}
	if action != model.InsertAction && action != model.UpdateAction {
		fmt.Printf("invalid action: %s\n", action)
		return
	}

	// get the configuration schema by the data type
	relation := e.consuler.GetSchema(dataType)
	if relation == nil {
		fmt.Printf("relation schema for %v is not found\n", dataType)
		return
	}

	// update the cache of table schemas
	if err := e.update(newSchema, relation, action); err != nil {
		fmt.Printf("update json schema %v: %v\n", string(m.Data), err)
		return
	}
}

// new a schemas carrier based on the table name and action
func (e *Engine) newSchemasCarrier(dataType string, table string, action string) *model.SchemasCarrier {
	return &model.SchemasCarrier{
		DataType: dataType,
		Table:    table,
		Action:   action,
		Schemas:  make([]model.JSONSchema, 0, e.settings.Server.EngineBatch),
	}
}

// update the cache of table schemas, if any data type reaches the threshold, append to channel
func (e *Engine) update(schema model.JSONSchema, relation *model.Relation, action string) error {
	finalSchema := model.JSONSchema{}
	// filter with schema and relation
	for _, column := range relation.Columns {
		v, ok := schema[column]
		if !ok {
			return fmt.Errorf("%s is missed", column)
		}
		finalSchema[column] = v
	}

	// if ready isn't nil, it represents sending the schemas carrier to storage
	var ready *model.SchemasCarrier

	// handle the update action, without slice buffer
	if action == model.UpdateAction {
		e.cacheMux.Lock()
		// new a schemas carrier for update action
		ready = e.newSchemasCarrier(relation.DataType, relation.Table, action)
		// append the new schema to schemas carrier
		ready.Schemas = append(ready.Schemas, finalSchema)
		e.cacheMux.Unlock()
	}

	// handle the insert action
	if action == model.InsertAction {
		e.cacheMux.Lock()
		// update the cache with locker
		carrier, ok := e.cache[relation.DataType]
		if !ok {
			// init the schemas carrier for data type
			carrier = e.newSchemasCarrier(relation.DataType, relation.Table, action)
			e.cache[relation.DataType] = carrier
		}
		// append the new schema to schemas carrier
		carrier.Schemas = append(carrier.Schemas, finalSchema)

		// check if the length of schemas reaches the threshold
		if len(carrier.Schemas) == e.settings.Server.EngineBatch {
			// buffer the schemas carrier
			ready = carrier

			// reset the schemas carrier
			e.cache[relation.DataType] = e.newSchemasCarrier(relation.DataType, relation.Table, action)
		}
		e.cacheMux.Unlock()
	}

	// send the schemas carrier to channel for storage to batch insertion
	if ready != nil {
		e.stream <- ready
	}

	return nil
}

// flush the left data to storage
func (e *Engine) flush() {
	e.cacheMux.Lock()
	defer e.cacheMux.Unlock()

	// flush the left data to storage before exit
	for key, carrier := range e.cache {
		if len(carrier.Schemas) > 0 {
			// buffer the schemas carrier
			buf := carrier

			// reset the schemas carrier
			e.cache[key] = e.newSchemasCarrier(carrier.DataType, carrier.Table, carrier.Action)

			// send the schemas carrier to storage for batch insertion
			e.stream <- buf
		}
	}
}

// 1. handle the left data in stream
// 2. push the buffered schemas
func (e *Engine) afterCare() bool {
	delay := e.settings.Server.EngineDelayTime

	select {
	case m := <-e.consumer.ReadMessages():
		// parse and handle the message
		e.parse(m)

	case <-time.After(time.Second * delay):
		// flush the unreached buffer to storage
		e.flush()

		return false
	}

	return true
}

func (e *Engine) handle(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// mark the goroutine started
	e.ready <- struct{}{}
	for {
		// consider flush the data in channel befer exiting

		// try to read the message first
		select {
		case m := <-e.consumer.ReadMessages():
			// parse and handle the message
			e.parse(m)

		case <-ctx.Done():
			// if the cancel signal is received
			// try to handle the left data, and push the buffered schemas
			for {
				if e.afterCare() == false {
					fmt.Println("engine goroutine exit")
					return
				}
			}
		}
	}
}

// timer refresh the buffered schemas to storage
func (e *Engine) refresh(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	interval := e.settings.Server.EngineRefreshTime
	// new a ticker for refreshing periodly
	ticker := time.NewTicker(time.Second * interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// flush the unreached buffer to storage
			e.flush()

		case <-ctx.Done():
			fmt.Printf("engine refresh goroutine exit\n")
			return
		}
	}
}

// Start create goroutines to parse messages and build structure for database
func (e *Engine) Start(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < e.settings.Server.EngineRoutines; i++ {
		wg.Add(1)
		go e.handle(ctx, wg)
	}

	// start a timer
	wg.Add(1)
	go e.refresh(ctx, wg)
}

// ReadMessages returns the messages from channel
func (e *Engine) ReadMessages() <-chan *model.SchemasCarrier {
	return e.stream
}

// IsReady check if the engine is ready
func (e *Engine) IsReady() {
	for i := 0; i < e.settings.Server.EngineRoutines; i++ {
		<-e.ready
	}
	fmt.Println("engine goroutines are ready")
}
