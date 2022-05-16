package main

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"log"
)

// 故意解绑队列和交换机来测试 Mandatory 和 NotifyReturn
func main() {
	log.Print("rabbitmq unbind queue")
	conn, err := emailMessageQueue.NewConnect() ; if err != nil {
		panic(err)
	}
	mqCh, mqChClose, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	defer mqChClose()
	err = mqCh.QueueUnbind(rab.QueueUnbind{
		Queue:      emailMessageQueue.Framework().Queue.SendEmail.Name,
		RoutingKey: "",
		Exchange:   emailMessageQueue.Framework().Exchange.SendEmail.Name,
	}.Flat()) ; if err != nil {
		panic(err)
	}
	log.Print("rabbitmq unbind done")
}
