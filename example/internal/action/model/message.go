package m

import (
	xjson "github.com/goclub/json"
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
)

type UserSignupMessage struct {
	Email string `json:"email"`
}
func (v *UserSignupMessage) DecodeDelivery(d *amqp.Delivery) error {
	return xjson.Unmarshal(d.Body, v)
}
func (v UserSignupMessage) Publishing () (p amqp.Publishing, err error) {
	body, err := xjson.Marshal(v) ; if err != nil {
		return
	}
	return amqp.Publishing{
		MessageId: rab.MessageID(),
		ContentType: "application/json",
		Body:  body,
	}, nil
}