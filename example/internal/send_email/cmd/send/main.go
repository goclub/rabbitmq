package main

import (
	"errors"
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"github.com/goclub/rabbitmq/example/internal/send_email/service"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

func main () {
	rab.Debug = true
	conn, err := emailMessageQueue.NewConnect() ; if err != nil {
		panic(err)
	}
	mqCh, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	log.Print("start mesasge done")
	for {
		err = emailService.SendEmail(emailService.Email{
			To: "abc@domain.com",
			Subject: strconv.Itoa(time.Now().Second()),
		}, mqCh) ; if err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				log.Print("!!!!! is close !!!!!!")
			} else {
				log.Print(err)
			}
		}
		// 延迟是为了让 data := <- notifyReturn 有足够的运行时间,正式项目中会通过 http/tcp listen  常驻进程，就不需要 sleep
		time.Sleep(1* time.Second)
		log.Print("send mesasge")
	}

}