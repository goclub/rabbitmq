package emailMessageQueue

import (
	xjson "github.com/goclub/json"
	"github.com/streadway/amqp"
)

type SendEmailMessage struct {
	From string `json:"from"`
	To string `json:"to"`
	Subject string `json:"subject"`
}
func (v *SendEmailMessage) Decode(data []byte) error {
	return xjson.Unmarshal(data, v)
}
func (v SendEmailMessage) Publishing () (p amqp.Publishing, err error) {
	body, err := xjson.Marshal(v) ; if err != nil {
		return
	}
	return amqp.Publishing{
		ContentType: "application/json",
		Body:  body,
	}, nil
}