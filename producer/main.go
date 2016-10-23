package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/Shopify/sarama"
	"net/http"
	"strconv"
	"time"
	"fmt"
)

type Request struct {
	id    int     `json:"id"`
	name  string  `json:"name"`
}

var request Request

const (
	BROKERS_URL string = "192.168.10.101:9092"
	TOPIC_NAME  string = "test"
)

func producer(context *gin.Context) {
	context.Bind(&request)
	marshalRequest, err := json.Marshal(request)

	if err != nil {
		panic(err)
	}

	fmt.Println(marshalRequest)
	reqString := string("this is it")

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	
	brokers := []string{BROKERS_URL}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	
	if err != nil {
		panic(err)
	}

	strTime := strconv.Itoa(int(time.Now().Unix()))

	msg := &sarama.ProducerMessage{
		Topic: TOPIC_NAME,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.StringEncoder("hello this is test string"),
	}

	producer.Input() <- msg

	resp := gin.H{
		"status": http.StatusOK,
		"message": "Message has been sent.",
		"data": reqString,
	}

	context.IndentedJSON(http.StatusOK, resp)
}

func main() {
	router := gin.Default()
	router.POST("/producer", producer)
	router.Run(":3000")
}