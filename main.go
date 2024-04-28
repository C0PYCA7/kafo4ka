package main

import (
	"github.com/IBM/sarama"
	"log"
	"strconv"
)

func main() {
	client, err := sarama.NewClient([]string{":9092"}, nil)
	if err != nil {
		log.Printf("new client: %v", err)
	}
	log.Println("client created")
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Printf("new cluster admin: %v", err)
	}
	log.Println("admin created")
	defer admin.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Printf("new consumer from client: %v", err)
	}
	log.Println("consumer created")
	defer consumer.Close()

	details := sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}
	err = admin.CreateTopic("test-topic", &details, true)
	if err != nil {
		log.Printf("create topic: %v", err)
	}
	log.Println("topic created")
	defer admin.DeleteTopic("test-topic")

	producer, err := sarama.NewSyncProducer([]string{":9092"}, nil)
	if err != nil {
		log.Printf("new producer: %v", err)
	}
	log.Println("producer created")
	defer producer.Close()

	partitionConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("new partition consumer: %v", err)
	}
	log.Println("partition consumer created")
	defer partitionConsumer.Close()

	ch := make(chan string)
	defer close(ch)

	go func() {
		for {
			select {
			case msg, ok := <-partitionConsumer.Messages():
				if !ok {
					log.Println("partition consumer closed")
					return
				}
				ch <- string(msg.Value)
			}
		}
	}()

	go func() {
		str := "hello world"
		for i := range 10 {
			str = str + strconv.Itoa(i)
			msg := &sarama.ProducerMessage{Topic: "test-topic", Value: sarama.StringEncoder(str)}
			producer.SendMessage(msg)
		}
	}()

	for value := range ch {
		log.Println("value:", value)
	}
}
