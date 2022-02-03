package emailMessageQueue

import (
	xjson "github.com/goclub/json"
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
)

type SendEmailMessage struct {
	From string `json:"from"`
	To string `json:"to"`
	Subject string `json:"subject"`
}
func (v *SendEmailMessage) DecodeDelivery(d *amqp.Delivery) error {
	return xjson.Unmarshal(d.Body, v)
}
func (v SendEmailMessage) Publishing () (p amqp.Publishing, err error) {
	body, err := xjson.Marshal(v) ; if err != nil {
		return
	}
	return amqp.Publishing{
		MessageId: rab.MessageID(),
		ContentType: "application/json",
		Body:  body,
	}, nil
}