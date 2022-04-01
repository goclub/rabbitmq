package main

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func main() {
	conn, err := rab.Dial("amqp://guest:guest@localhost:5672/") ; if err != nil {
		panic(err)
	}
	mqCh,mqChClose, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	defer mqChClose()
	// 发布消息到业务用交换机
	text := "time(" + time.Now().String() + ")"
	log.Print("publish:", text)
	err = mqCh.Publish(rab.Publish{
		Exchange: "x_example_time",
		RoutingKey: "", // fanout 不需要 key
		Msg: amqp.Publishing{
			ContentType: "text/plain",
			Body: []byte(text),
		},
	}) ; if err != nil {
		panic(err)
	}
}
