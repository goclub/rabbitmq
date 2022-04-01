package main

import (
	rab "github.com/goclub/rabbitmq"
	"log"
)

func main() {
	log.Print("consume dl")
	conn, err := rab.Dial("amqp://guest:guest@localhost:5672/") ; if err != nil {
		panic(err)
	}
	mqCh, mqChClose, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	defer mqChClose()
	// 消费死信队列消息
	msgs, err := mqCh.Consume(rab.Consume{
		Queue: rab.QueueName("dlq_example_time"),
	}) ; if err != nil {
		panic(err)
	}
	for d := range msgs {
		// 记录死信队列到数据库，通过人工干预解决死信问题
		log.Print("record dlx: " + string(d.Body))
		err  = d.Ack(false) ; if err != nil {
			log.Print(err) // 记录错误到日志或监控系统而不是退出，单个消息消费失败并不一定要让消费端停止工作
		}
	}
}

