package main

import (
	xerr "github.com/goclub/error"
	xjson "github.com/goclub/json"
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

func main () {
	log.Printf("%+v", run())
}
func run () (err error) {
	rab.OnReconnect(func(message string) {
		log.Print(message)
	})
	conn, err := emailMessageQueue.NewConnect() ; if err != nil {
		return
	}
	mqch, err := conn.Channel() ; if err != nil {
		return
	}
	defer func() {
		log.Print(222222,recover())
	}()
	rab.NotifyReturn(mqch, func(r *amqp.Return) {
		data, err := xjson.Marshal(r) ; if err != nil {
			// 正式项目将错误发生到 sentry:
			// sentry.CaptureException(err) 或者 dep.track.Error(err)
			xerr.PrintStack(err)
		    return
		}
		// 正式项目将信息发送到 sentry:
		// sentry.CaptureMessage(string(data)) 或 dep.track.Message(string(data))
		log.Print(string(data))
	}, func(panicRecover interface{}) {
		// 正式项目将 recover 发送到 sentry: sentry.Recover(panicRecover) 或者 dep.track.Recover(panicRecover)
		log.Print(panicRecover)
	})
	log.Print("start mesasge done")
	for {
		err = SendEmail(Email{
			To: "abc@domain.com",
			Subject: strconv.Itoa(time.Now().Second()),
		}, mqch) ; if err != nil {
			log.Print("send Email error:", err)
		}
		time.Sleep(1* time.Second)
		log.Print("send mesasge")
	}
}

type Email struct {
	To string
	Subject string
}
func SendEmail(email Email, mqCh *rab.ProxyChannel) (err error) {
	msg, err := emailMessageQueue.SendEmailMessage{
		From: "news@goclub.io",
		To: email.To,
		Subject: email.Subject,
	}.Publishing() ; if err != nil {
		return
	}
	return mqCh.Publish(rab.Publish{
		Exchange:   emailMessageQueue.Framework().FanoutExchange.SendEmail.Name,
		RoutingKey: "", // fanout 不需要 key
		Mandatory:  true, // 要确保消息能到队列（配合 rab.NotifyReturn() 使用 ）
		Msg:        msg,
	})
}

