package main

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"log"
	"strconv"
	"time"
)

func main() {
	log.Printf("%+v", run())
}
func run() (err error) {
	if err != nil {
		log.Fatal(err)
	}
	conn, err := emailMessageQueue.NewConnect()
	if err != nil {
		return
	}
	mqCh, mqChClose, err := conn.Channel()
	if err != nil {
		return
	}
	defer mqChClose()
	log.Print("start mesasge done")
	for {
		err = SendEmail(Email{
			To:      "abc@domain.com",
			Subject: strconv.Itoa(time.Now().Second()),
		}, mqCh)
		if err != nil {
			log.Print("send Email error:", err)
		}
		time.Sleep(1 * time.Second)
		log.Print("send mesasge")
	}
}

type Email struct {
	To      string
	Subject string
}

func SendEmail(email Email, mqCh *rab.ProxyChannel) (err error) {
	msg, err := emailMessageQueue.SendEmailMessage{
		From:    "news@goclub.io",
		To:      email.To,
		Subject: email.Subject,
	}.Publishing()
	if err != nil {
		return
	}
	return mqCh.Publish(rab.Publish{
		Exchange:   emailMessageQueue.Framework().FanoutExchange.SendEmail.Name,
		RoutingKey: "",   // fanout 不需要 key
		Mandatory:  true, // 要确保消息能到队列（配合 rab.NotifyReturn() 使用 ）
		Msg:        msg,
	})
}
