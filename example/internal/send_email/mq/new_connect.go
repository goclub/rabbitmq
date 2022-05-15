package emailMessageQueue

import (
	xerr "github.com/goclub/error"
	xjson "github.com/goclub/json"
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
	"log"
	"runtime/debug"
	"time"
)

func NewConnect() (conn *rab.ProxyConnection, err error) {
	return rab.Dial("amqp://guest:guest@localhost:5672/", rab.Option{
		// goclub/rabbitmq 有重连机制,当发生重连时会触发 OnReconnect
		OnReconnect: func(message string) {
			log.Print(message, string(debug.Stack()))
		},
		// NotifyReturn 用于订阅发送时退回的消息 需在 Publish 时配合 Mandatory 使用
		HandleNotifyReturn: rab.HandleNotifyReturn{
			Return: func(r *amqp.Return) {
				data, err := xjson.Marshal(r)
				if err != nil {
					// 正式项目建议错误发生到 sentry:
					// sentry.CaptureException(err) 或 dep.track.Error(err)
					xerr.PrintStack(err)
					return
				}
				// 正式项目将信息发送到 sentry:
				// sentry.CaptureMessage(string(data)) 或 dep.track.Message(string(data))
				log.Print("NotifyReturn:", string(data))
			},
			Panic: func(panicRecover interface{}) {
				// Panic处理函数 不能在发生 Panic 否则会因为子routine panic 导致程序直接退出
				defer func() {
					r := recover()
					if r != nil {
						log.Print(r)
					}
				}()
				// 正式项目将信息发送到 sentry:
				// sentry.Recover(panicRecover) 或 dep.track.Recover(panicRecover)
				log.Print(panicRecover)
			},
		},
		// outbox 消息队列事务发件箱
		Outbox: rab.OutboxOption{
			MaxPublishTimes:     10,
			NextPublishTime: func(n uint16) time.Duration {
				return time.Duration(n) * time.Second * 3
			},
			ConsumeLoopInterval: time.Second,
			TimeZone:            time.FixedZone("CST", 8*3600),
			Logger:              log.Default(),
		},
	})
}