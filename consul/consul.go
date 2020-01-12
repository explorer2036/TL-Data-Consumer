package consul

import (
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/log"
	"TL-Data-Consumer/model"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	yaml "gopkg.in/yaml.v2"
)

const (
	// DefaultRefreshTime - the default refresh time for loading the configuration from consul
	DefaultRefreshTime = 15
	// DefaultWaitTime - the default wait time for request
	DefaultWaitTime = 5 * time.Second
)

// Consul represents the configuration center
type Consul struct {
	settings *config.Config // settings for consul
	client   *api.Client    // the client handler for consul

	schemas   map[string]*model.Relation // table schemas for different type of data
	schemaMux sync.RWMutex
}

// NewConsul returns the configuration center
func NewConsul(settings *config.Config) *Consul {
	if settings.Consul.RefreshTime == 0 {
		settings.Consul.RefreshTime = DefaultRefreshTime
	}

	// new the consul first
	consul := &Consul{
		settings: settings,
		schemas:  make(map[string]*model.Relation),
	}

	// new the consul client
	config := api.DefaultConfig()
	config.Address = settings.Consul.Address
	config.Scheme = "http"
	client, err := api.NewClient(config)
	if err != nil {
		panic(err)
	}
	consul.client = client

	// try to load the configuration
	if err := consul.update(); err != nil {
		panic(err)
	}

	return consul
}

// update the schema by fetching configuration from consul
func (c *Consul) update() error {
	// load the schema bytes from consul
	pairs, err := c.load()
	if err != nil {
		return err
	}

	// parse the bytes to schema structure
	return c.parse(pairs)
}

// parse the bytes to schema structure
func (c *Consul) parse(pairs api.KVPairs) error {
	schemas := map[string]*model.Relation{}

	// loop the pairs to unmarshal to schema
	for _, pair := range pairs {
		// ignore the directory name
		if len(pair.Value) == 0 {
			continue
		}

		var s model.TableSchemas
		// unmarshal the bytes to config schema structure
		if err := yaml.Unmarshal(pair.Value, &s); err != nil {
			return err
		}

		// collect the relations for data type and table name
		for _, relation := range s.Relations {
			// check if the data type is duplicate
			if _, exist := schemas[relation.DataType]; exist {
				return fmt.Errorf("data type %s is duplicate", relation.DataType)
			}

			// merge base and flexible columns for every data type
			columns := []string{}
			columns = append(columns, s.Columns...)
			columns = append(columns, relation.Columns...)

			schemas[relation.DataType] = &model.Relation{
				DataType: relation.DataType,
				Table:    relation.Table,
				Columns:  columns,
				Common:   s.Columns,
			}
		}
	}
	if len(schemas) == 0 {
		return errors.New("schemas is empty")
	}

	// update the schemas
	c.schemaMux.Lock()
	c.schemas = schemas
	c.schemaMux.Unlock()

	return nil
}

// load the database schema from configuration center
func (c *Consul) load() (api.KVPairs, error) {
	queryOption := api.QueryOptions{
		RequireConsistent: true,
		WaitTime:          DefaultWaitTime,
	}

	key := c.settings.Consul.Directory
	// load the newest configuration from consul
	pairs, _, err := c.client.KV().List(key, &queryOption)
	if err != nil {
		return nil, err
	}
	// if the directory isn't found
	if len(pairs) == 0 {
		return nil, fmt.Errorf("key %s is not found", key)
	}

	return pairs, nil
}

// refresh the configuration periodly
func (c *Consul) refresh(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	interval := c.settings.Consul.RefreshTime
	// new a ticker for refreshing periodly
	ticker := time.NewTicker(time.Second * interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// try to load and update the database schemas
			if err := c.update(); err != nil {
				log.Infof("refresh the database schemas: %v", err)
			}

		case <-ctx.Done():
			log.Info("consul refresh goroutine exit")
			return
		}
	}
}

// Start goroutine to load and update the configuration from consul
func (c *Consul) Start(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	// start the refresh timer
	go c.refresh(ctx, wg)
}

// GetSchema returns the schema for data type
func (c *Consul) GetSchema(dataType string) *model.Relation {
	c.schemaMux.RLock()
	defer c.schemaMux.RUnlock()

	if schema, ok := c.schemas[dataType]; ok {
		return schema
	}
	return nil
}
