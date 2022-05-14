package emailMessageQueue

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
)

func Framework() (m struct {
	FanoutExchange struct {
		SendEmail rab.ExchangeDeclare
	}
	Queue struct {
		SendEmail     rab.QueueDeclare
		SendEmailBind rab.QueueBind
	}
}) {
	m.FanoutExchange.SendEmail = rab.ExchangeDeclare{
		Name:    "x_send_email",
		Kind:    amqp.ExchangeFanout,
		Durable: true,
	}
	m.Queue.SendEmail = rab.QueueDeclare{
		Name:    "q_send_email",
		Durable: true,
	}
	m.Queue.SendEmailBind = rab.QueueBind{
		Queue:      m.Queue.SendEmail.Name,
		RoutingKey: "", // fanout 不需要 routing key
		Exchange:   m.FanoutExchange.SendEmail.Name,
	}
	return
}

func InitDeclareAndBind(mqCh *rab.ProxyChannel) (err error) {
	err = mqCh.ExchangeDeclare(Framework().FanoutExchange.SendEmail)
	if err != nil {
		return
	}
	_, err = mqCh.QueueDeclare(Framework().Queue.SendEmail)
	if err != nil {
		return
	}
	err = mqCh.QueueBind(Framework().Queue.SendEmailBind)
	if err != nil {
		return
	}
	return
}
