package emailMessageQueue

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
)

func Model() (m struct {
	FanoutExchange struct{
		SendEmail rab.ExchangeDeclare
	}
	Queue struct {
		SendEmail rab.QueueDeclare
		SendEmailBind rab.QueueBind
	}
}) {
	m.FanoutExchange.SendEmail = rab.ExchangeDeclare{
		Name: "x_send_email",
		Kind: amqp.ExchangeFanout,
		Durable: true,
	}
	m.Queue.SendEmail = rab.QueueDeclare{
		Name: "q_send_email",
		Durable: true,
	}
	m.Queue.SendEmailBind = rab.QueueBind{
		Queue: m.Queue.SendEmail.Name,
		RoutingKey: "", // fanout 不需要 routing key
		Exchange: m.FanoutExchange.SendEmail.Name,
	}
	return
}

func NewConnect() (conn *rab.ProxyConnection, err error) {
	return rab.Dial("amqp://guest:guest@localhost:5672/")
}

func InitDeclareAndBind(mqCh *rab.ProxyChannel) (err error) {
	err = mqCh.ExchangeDeclare(Model().FanoutExchange.SendEmail) ; if err != nil {
		return
	}
	_, err = mqCh.QueueDeclare(Model().Queue.SendEmail) ; if err != nil {
		return
	}
	err = mqCh.QueueBind(Model().Queue.SendEmailBind) ; if err != nil {
		return
	}
	return
}