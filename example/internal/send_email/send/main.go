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
	// goclub/rabbitmq 支持断线重连, OnReconnect 可以记录重连
	rab.OnReconnect(func(message string) {
		// 正式项目将信息发送到 sentry:
		// sentry.CaptureMessage(message) 或 dep.track.Message(message)
		log.Print(message)
	})
	// NotifyReturn 用于订阅发送时退回的消息 需在 Publish 时配合 Mandatory 使用
	err = NotifyReturn() ; if err != nil {
	    log.Fatal(err)
	}
	conn, err := emailMessageQueue.NewConnect() ; if err != nil {
		return
	}
	mqCh,mqChClose, err := conn.Channel() ; if err != nil {
		return
	}
	defer mqChClose()
	log.Print("start mesasge done")
	for {
		err = SendEmail(Email{
			To: "abc@domain.com",
			Subject: strconv.Itoa(time.Now().Second()),
		}, mqCh) ; if err != nil {
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



func NotifyReturn () (err error) {
	err = rab.NotifyReturn(rab.HandleNotifyReturn{
		Return: func(r *amqp.Return) {
			data, err := xjson.Marshal(r) ; if err != nil {
				// 正式项目建议错误发生到 sentry:
				// sentry.CaptureException(err) 或 dep.track.Error(err)
				xerr.PrintStack(err)
				return
			}
			// 正式项目将信息发送到 sentry:
			// sentry.CaptureMessage(string(data)) 或 dep.track.Message(string(data))
			log.Print(string(data))
		},
		Panic: func(panicRecover interface{}) {
			// Panic处理函数 不能在发生 Panic 否则会因为子routine panic 导致程序直接退出
			defer func() { r := recover() ; if r != nil { log.Print(r) } }()
			// 正式项目将信息发送到 sentry:
			// sentry.Recover(panicRecover) 或 dep.track.Recover(panicRecover)
			log.Print(panicRecover)
		},
	}) ; if err != nil {
		return
	}
	return
}