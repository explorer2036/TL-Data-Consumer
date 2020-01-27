package storage

import (
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/db"
	"TL-Data-Consumer/engine"
	"TL-Data-Consumer/log"
	"context"
	"sync"
	"time"
)

const (
	// DefaultRoutinesNum - routines number for inserting
	DefaultRoutinesNum = 2
	// DefaultDelayTime - delay time for goroutines consuming the channel
	DefaultDelayTime = 4
)

// Storage represents the database insertion
type Storage struct {
	settings *config.Config // the settings for storage
	engine   *engine.Engine // the engine object
	handler  *db.Handler    // the db handler
	ready    chan struct{}  // mark the storage is ready
}

// NewStorage returns a new storage for database insertion
func NewStorage(settings *config.Config, engine *engine.Engine, handler *db.Handler) *Storage {
	if settings.Server.StorageRoutines == 0 {
		settings.Server.StorageRoutines = DefaultRoutinesNum
	}
	if settings.Server.StorageDelayTime == 0 {
		settings.Server.StorageDelayTime = DefaultDelayTime
	}

	return &Storage{
		engine:   engine,
		handler:  handler,
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
		s.handler.TryFlush(carrier)

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
			s.handler.TryFlush(carrier)

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

// IsReady check if the storage is ready
func (s *Storage) IsReady() {
	for i := 0; i < s.settings.Server.StorageRoutines; i++ {
		<-s.ready
	}
	log.Info("storage goroutines are ready")
}
