package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/Shopify/sarama"
	"net/http"
	"strconv"
	"time"
)

type Request struct {
	ID int `json:"id"`
	Name  string `json:"name"`
}

var request Request

const (
	BROKERS_URL string = "localhost:9092"
	TOPIC_NAME   string = "test"
)

func producer(context *gin.Context) {
	context.Bind(&request)
	marshalRequest, err := json.Marshal(request)

	if err != nil {
		panic(err)
	}

	reqString := string(marshalRequest)

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	
	brokers := []string{BROKERS_URL}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	strTime := strconv.Itoa(int(time.Now().Unix()))

	msg := &sarama.ProducerMessage{
		Topic: TOPIC_NAME,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.StringEncoder(reqString),
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