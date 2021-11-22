package main

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"github.com/goclub/rabbitmq/example/internal/send_email/service"
	"log"
)

func main () {
	rab.Debug = true
	conn, err := emailMessageQueue.NewConnect() ; if err != nil {
		panic(err)
	}
	mqCh, err := conn.Channel() ; if err != nil {
		panic(err)
	}

	log.Print("start consume queue")
	err = emailService.ConsumeSendEmail(mqCh) ; if err != nil {
		panic(err)
	}
}