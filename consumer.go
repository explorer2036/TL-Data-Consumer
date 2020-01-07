package main

import (
	"TL-Data-Consumer/config"
	"TL-Data-Consumer/consul"
	"TL-Data-Consumer/engine"
	"TL-Data-Consumer/kafka"
	"TL-Data-Consumer/server"
	"TL-Data-Consumer/storage"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	var settings config.Config
	// parse the config file
	if err := config.ParseYamlFile("config.yml", &settings); err != nil {
		panic(err)
	}
	// check if some fields value in config file is valid
	if err := settings.Check(); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	// create consul for loading configuration from consul
	consuler := consul.NewConsul(&settings)
	// create consumer for reading from kafka
	consumer := kafka.NewConsumer(&settings)
	// create engine for handling messages
	enginer := engine.NewEngine(&settings, consumer, consuler)
	// create storage for batch inserting data to database
	storager := storage.NewStorage(&settings, enginer, consuler)

	ctx, cancel := context.WithCancel(context.Background())
	// start the goroutines to refresh the configuration from consul
	consuler.Start(ctx, &wg)

	// start the goroutines to batch insert records to database
	storager.Start(ctx, &wg)
	storager.IsReady()

	// start the engine goroutines to pipeline handling first, must keep the goroutines ready
	enginer.Start(ctx, &wg)
	enginer.IsReady()

	// final start the goroutines to retrieve messages from kafka
	consumer.Start(ctx, &wg)
	consumer.IsReady()

	// start the http server
	admin := server.NewServer(&settings)
	admin.Start(&wg)

	fmt.Println("data consumer is started")

	sig := make(chan os.Signal, 1024)
	// subscribe signals: SIGINT & SINGTERM
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case s := <-sig:
			fmt.Printf("receive signal: %v\n", s)

			start := time.Now()
			// cancel the consumer goroutines
			cancel()

			// stop the http server
			admin.Stop()

			// await utnil all the goroutines are exited
			wg.Wait()

			// release the hard resources, like redis or databases
			storager.Close()

			fmt.Printf("shut down takes time: %v\n", time.Now().Sub(start))
			return
		}
	}

}

func struct2JSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
