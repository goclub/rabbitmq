package main

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/action/model"
	"log"
)

// 故意解绑队列和交换机来测试 Mandatory 和 NotifyReturn
func main() {
	log.Print("rabbitmq unbind queue")
	conn, err := m.NewConnect() ; if err != nil {
		panic(err)
	}
	mqCh, mqChClose, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	defer mqChClose()
	f := m.Framework()
	err = mqCh.QueueUnbind(rab.QueueUnbind{
		Queue:      f.UserSignUp.WelcomeEmail.Queue.Name,
		RoutingKey: "",
		Exchange: f.UserSignUp.Exchange.Name,
	}.Flat()) ; if err != nil {
		panic(err)
	}
	err = mqCh.QueueUnbind(rab.QueueUnbind{
		Queue:      f.DeadLetterHumanIntervention.SaveToSQL.Queue.Name,
		RoutingKey: "",
		Exchange: f.DeadLetterHumanIntervention.Exchange.Name,
	}.Flat()) ; if err != nil {
		panic(err)
	}
	log.Print("rabbitmq unbind done")
}
