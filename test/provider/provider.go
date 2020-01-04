package main

import (
	"TL-Data-Consumer/entity"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"
	"TL-Data-Consumer/engine"

	"github.com/Shopify/sarama"
)

var (
	topics    = "topic-state"
	addrs     = ""
	frequency = 1000
	count     = -1
)

func init() {
	flag.IntVar(&frequency, "f", 1000, "the frequency of producing")
	flag.IntVar(&count, "c", -1, "the count of records")
	flag.StringVar(&addrs, "a", "", "the brokers of kafka")

	flag.Parse()
}

func produce(addrs []string, topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	producer, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
	}

	data := entity.Header{
		Source: "tester",
		Path:   "loc",
	}

	i := 0
	for {
		i++

		data.Key = fmt.Sprintf("K%04d;", i)
		data.Data = fmt.Sprintf("D%04d", i)
		data.Time = time.Now().Format(engine.RFC3339Milli)

		bytes, _ := json.Marshal(&data)
		msg.Value = sarama.ByteEncoder(bytes)
		if _, _, err := producer.SendMessage(msg); err != nil {
			time.Sleep(time.Second)
			continue
		}

		if frequency > 0 {
			time.Sleep(time.Millisecond * time.Duration(frequency))
		}

		if count == i {
			break
		}
	}
}

func main() {
	var wg sync.WaitGroup

	brokers := strings.Split(addrs, ",")

	wg.Add(1)
	go produce(brokers, topics, &wg)
	wg.Wait()
}
