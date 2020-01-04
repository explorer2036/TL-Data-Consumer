package engine

import (
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/domain"
	"TL-Data-Consumer/entity"
	"TL-Data-Consumer/kafka"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	redis "github.com/go-redis/redis"
	"github.com/patrickmn/go-cache"
)

const (
	defaultRoutinesNum  = 2
	defaultBatchCount   = 10
	defaultStreamBuffer = 100
	defaultDelayTime    = 2
	defaultRefreshTime  = 5

	topicState  = "topic-state"
	topicMetric = "topic-metric"
)

// Engine represents the pipeline for handling messages
type Engine struct {
	tokens   *cache.Cache    // the local cache for token
	consumer *kafka.Consumer // consumer for message stream channel

	states    []domain.Schema // state schemas for buffer
	stateMux  sync.Mutex
	metrics   []domain.Schema // metric schemas for buffer
	metricMux sync.Mutex

	redisClient *redis.Client        // maintain a redis connection
	stream      chan []domain.Schema // schemas stream channel
	ready       chan struct{}        // mark the engine is ready
	settings    *config.Config       // server configuration
}

// NewEngine returns a new engine
func NewEngine(settings *config.Config, consumer *kafka.Consumer) *Engine {
	if settings.Server.EngineRoutines == 0 {
		settings.Server.EngineRoutines = defaultRoutinesNum
	}
	if settings.Server.EngineBuffer == 0 {
		settings.Server.EngineBuffer = defaultStreamBuffer
	}
	if settings.Server.EngineBatch == 0 {
		settings.Server.EngineBatch = defaultBatchCount
	}
	if settings.Server.EngineDelayTime == 0 {
		settings.Server.EngineDelayTime = defaultDelayTime
	}
	if settings.Server.EngineRefreshTime == 0 {
		settings.Server.EngineRefreshTime = defaultRefreshTime
	}

	// init the redis connection
	redisClient := redis.NewClient(&redis.Options{
		Addr:     settings.Redis.Addr,
		Password: settings.Redis.Passwd, // no password
		DB:       settings.Redis.DB,     // use default DB
	})

	return &Engine{
		settings:    settings,
		consumer:    consumer,
		redisClient: redisClient,
		ready:       make(chan struct{}, settings.Server.EngineRoutines),
		stream:      make(chan []domain.Schema, settings.Server.EngineBuffer),
		states:      make([]domain.Schema, 0, settings.Server.EngineBatch),
		metrics:     make([]domain.Schema, 0, settings.Server.EngineBatch),
		tokens:      cache.New(24*time.Hour, 48*time.Hour),
	}
}

func (e *Engine) isValid(key string, source string) bool {
	splits := strings.Split(key, ";")
	if len(splits) < 2 {
		return false
	}
	userid, token := splits[0], splits[1]
	// validate token
	redisKey := userid + ";" + source

	return token == e.fetchToken(redisKey)
}

func (e *Engine) fetchToken(key string) string {
	// read from token cache first
	token, ok := e.tokens.Get(key)
	if !ok {

		// value will be empty string if key not found
		value, _ := e.redisClient.Get(key).Result()
		if len(value) > 0 {
			// write to cache with default expiration time if value is not empty
			e.tokens.Set(key, value, cache.DefaultExpiration)
		}
		return value
	}

	return token.(string)
}

func (e *Engine) parse(m *kafka.Message) {
	// decode the header
	var header entity.Header
	// unmarshal the data to State entity
	if err := json.Unmarshal(m.Data, &header); err != nil {
		fmt.Printf("unmarshal %v: %v\n", string(m.Data), err)
		return
	}
	// check if the token is valid
	if e.isValid(header.Key, header.Source) == false {
		fmt.Printf("invalid token: %s, %v\n", m.Topic, string(m.Data))
		return
	}

	// decode the message body into domain scheme
	if m.Topic == topicState {
		state, err := e.buildState(&header)
		if err != nil {
			fmt.Printf("build state: %v\n", err)
			return
		}

		// update the new state to slice buffer
		if result := e.updateState(state); len(result) > 0 {
			// if it needs to batch inserting, separate the channel operation out of lock condition
			e.stream <- result
		}
	} else if m.Topic == topicMetric {

	} else {
		fmt.Printf("invalid topic: %s, %v\n", m.Topic, string(m.Data))
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
		e.stateMux.Lock()
		// flush the left data to storage before exit
		if len(e.states) > 0 {
			// buffer the old buffer
			buf := e.states

			// clear the old buffer
			e.states = make([]domain.Schema, 0, e.settings.Server.EngineBatch)

			// send the states to storage
			e.stream <- buf
		}
		e.stateMux.Unlock()

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
			e.stateMux.Lock()
			// flush the unreached buffer to storage
			if len(e.states) > 0 {
				// buffer the old buffer
				buf := e.states

				// clear the old buffer
				e.states = make([]domain.Schema, 0, e.settings.Server.EngineBatch)

				// send the states to storage
				e.stream <- buf
			}
			e.stateMux.Unlock()

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
func (e *Engine) ReadMessages() <-chan []domain.Schema {
	return e.stream
}

// IsReady check if the engine is ready
func (e *Engine) IsReady() {
	for i := 0; i < e.settings.Server.EngineRoutines; i++ {
		<-e.ready
	}
	fmt.Println("engine goroutines are ready")
}

// Close releases the hard resources
func (e *Engine) Close() {
	if e.redisClient != nil {
		e.redisClient.Close()
	}
}
