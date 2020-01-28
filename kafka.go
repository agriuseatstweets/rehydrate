package main

import (
	"os"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer struct {
	Consumer *kafka.Consumer
}

func NewKafkaConsumer() KafkaConsumer {
	topic := os.Getenv("REHYDRATE_TOPIC")
	brokers := os.Getenv("KAFKA_BROKERS")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "rehydrate",
		"auto.offset.reset": "earliest",
		"enable.auto.commit": "false",
		"max.poll.interval.ms": "960000",
	})

	if err != nil {
		// TODO: handle in error channel?
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)
	return KafkaConsumer{c}
}


func chunk(ch chan UBOriginal, size int) chan []UBOriginal {
	var b []UBOriginal
	out := make(chan []UBOriginal)

	go func(){
		for v := range ch {
			b = append(b, v)
			if len(b) == size {
				out <- b
				b = make([]UBOriginal, 0)
			}
		}
		// send the remaining partial buffer
		if len(b) > 0 {
			out <- b
		}
		close(out)
	}()

	return out
}


func (consumer KafkaConsumer) Consume (n int, chunkSize int, errs chan error) chan []UBOriginal {
	messages := make(chan UBOriginal)
	c := consumer.Consumer

	// runs until n messages consumed
	go func() {
		defer close(messages)
		for i := 1; i <= n; i++ {

			msg, err := c.ReadMessage(-1)

			if err != nil {
				errs <- err
				break
			}

			val := msg.Value
			var dat UBOriginal
			err = json.Unmarshal(val, &dat)

			if err != nil {
				errs <- err
				break
			}
			messages <- dat
		}
	}()

	return chunk(messages, chunkSize)
}
