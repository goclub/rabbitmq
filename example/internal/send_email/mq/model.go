package emailMessageQueue

import (
	rabmq "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
)

func Model() (m struct {
	FanoutExchange struct{
		SendEmail rabmq.ExchangeDeclare
	}
	Queue struct {
		SendEmail rabmq.QueueDeclare
		SendEmailBind rabmq.QueueBind
	}
}) {
	m.FanoutExchange.SendEmail = rabmq.ExchangeDeclare{
		Name: "x_send_email",
		Kind: amqp.ExchangeFanout,
		Durable: true,
	}
	m.Queue.SendEmail = rabmq.QueueDeclare{
		Name: "q_send_email",
		Durable: true,
	}
	m.Queue.SendEmailBind = rabmq.QueueBind{
		Queue: m.Queue.SendEmail.Name,
		RoutingKey: "", // fanout 不需要 routing key
		Exchange: m.FanoutExchange.SendEmail.Name,
	}
	return
}

func NewConnect() (conn *amqp.Connection, err error) {
	return amqp.Dial("amqp://guest:guest@localhost:5672/")
}

func InitDeclareAndBind(mqCh *amqp.Channel) (err error) {
	err = mqCh.ExchangeDeclare(Model().FanoutExchange.SendEmail.Flat()) ; if err != nil {
		return
	}
	_, err = mqCh.QueueDeclare(Model().Queue.SendEmail.Flat()) ; if err != nil {
		return
	}
	err = mqCh.QueueBind(Model().Queue.SendEmailBind.Flat()) ; if err != nil {
		return
	}
	return
}