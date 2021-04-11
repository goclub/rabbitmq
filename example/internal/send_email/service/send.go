package emailService

import (
	rabmq "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"github.com/streadway/amqp"
)


type Email struct {
	To string
	Subject string
}
func SendEmail(email Email, mqCh *amqp.Channel) (err error) {

	msg, err := emailMessageQueue.SendEmailMessage{
		From: "news@goclub.io",
		To: email.To,
		Subject: email.Subject,
	}.Publishing() ; if err != nil {
		return
	}
	return mqCh.Publish(rabmq.Publish{
		Exchange:   emailMessageQueue.Model().FanoutExchange.SendEmail.Name,
		RoutingKey: "", // fanout 不需要 key
		Mandatory:  true, // 要确保消息能到队列（配合 Channel{}.NotifyReturn ）
		Msg:        msg,
	}.Flat())
}

