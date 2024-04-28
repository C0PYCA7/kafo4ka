package main

import (
	"github.com/IBM/sarama"
	"log"
	"os"
)

func main() {
	consumer, err := sarama.NewConsumer([]string{":9092"}, nil)
	if err != nil {
		log.Printf("new consumer: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()
	log.Println("consumer created")

	partitionConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("new partition consumer: %v\n", err)
	}
	defer partitionConsumer.Close()
	log.Println("partition consumer created")
	ch := make(chan string)

	go func() {
		for msg := range partitionConsumer.Messages() {
			ch <- string(msg.Value)
		}
	}()

	for value := range ch {
		log.Printf("value: %v\n", value)
	}
}
