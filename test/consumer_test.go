package test

import (
	"TL-Data-Consumer/entity"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"TL-Data-Consumer/engine"

	"github.com/Shopify/sarama"
)

var (
	topics    = "topic-state"
	addrs     = []string{"192.168.0.13:9092"}
	frequency = 1000
	count     = -1
)

func init() {
	flag.IntVar(&frequency, "f", 1000, "the frequency of producing")
	flag.StringVar(&topics, "t", "topic-state", "the kafka topics")
	flag.IntVar(&count, "c", -1, "the count of records")
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

		data.Key = fmt.Sprintf("K%05d;", i)
		data.Data = fmt.Sprintf("D%05d", i)
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

func TestProduce(t *testing.T) {
	var wg sync.WaitGroup
	for _, topic := range strings.Split(topics, ",") {
		wg.Add(1)
		go produce(addrs, topic, &wg)
	}
	wg.Wait()
}
