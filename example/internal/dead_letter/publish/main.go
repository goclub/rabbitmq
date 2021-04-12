package main

import (
	rabmq "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/") ; if err != nil {
		panic(err)
	}
	mqCh, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	// 发布消息到业务用交换机
	text := "time(" + time.Now().String() + ")"
	log.Print("publish:", text)
	err = mqCh.Publish(rabmq.Publish{
		Exchange: "x_example_time",
		RoutingKey: "", // fanout 不需要 key
		Msg: amqp.Publishing{
			ContentType: "text/plain",
			Body: []byte(text),
		},
	}.Flat()) ; if err != nil {
		panic(err)
	}
}
