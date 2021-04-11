package main

import (
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"log"
)

func main (){
	log.Print("rabbitmq migrate start")
	conn, err := emailMessageQueue.NewConnect() ; if err != nil {
		panic(err)
	}
	mqCh, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	err = emailMessageQueue.InitDeclareAndBind(mqCh) ; if err != nil {
		panic(err)
	}
	log.Print("rabbitmq migrate done")
}
