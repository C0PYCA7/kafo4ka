package main

import (
	"github.com/IBM/sarama"
	"log"
	"strconv"
)

func main() {
	admin, err := sarama.NewClusterAdmin([]string{":9092"}, nil)
	if err != nil {
		log.Printf("new admin: %v\n", err)
	}
	defer admin.Close()
	log.Println("admin created")

	err = admin.CreateTopic("test-topic", &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, true)
	if err != nil {
		log.Printf("create topic: %v\n", err)
	}
	log.Println("topic created")

	producer, err := sarama.NewSyncProducer([]string{":9092"}, nil)
	if err != nil {
		log.Printf("new producer: %v\n", err)
	}
	defer producer.Close()
	log.Println("producer created")

	msg := &sarama.ProducerMessage{Topic: "test-topic"}
	str := "hello world"
	for i := range 10 {
		str = str + strconv.Itoa(i)
		msg.Value = sarama.StringEncoder(str)
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("send message: %v\n", err)
		}
		log.Printf("partition: %d, offset: %d\n", partition, offset)
	}
}
