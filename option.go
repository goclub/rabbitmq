package rab

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type Option struct {
	// OnReconnect
	// goclub/rabbitmq 封装后的 Connection 和 Channel 都有重连机制,当发生重连时会触发 OnReconnect
	// OnReconnect 为 nil 时默认执行 log.Print(message, string(debug.Stack()))
	OnReconnect func(reconnectID string, message string, err error)
	// NotifyReturn 用于订阅发送时退回的消息 需在 Publish 时配合 Mandatory 使用
	HandleNotifyReturn HandleNotifyReturn
	Outbox OutboxOption
}
type OutboxOption struct {
	MaxPublishTimes     uint16                       `default:"10"`
	NextPublishTime     func(n uint16) time.Duration `default:"3s"`
	ConsumeLoopInterval time.Duration  `default:"1s"`
	Timezone            *time.Location `default:"time.FixedZone("CST", 8*3600) china"`
	Logger              *log.Logger    `default:"log.Default()"`
}

type HandleNotifyReturn struct {
	// 发生 NotifyReturn 时触发
	Return func(r *amqp.Return)
	// 当 Return panic时触发 Panic
	Panic func(panicRecover interface{})
}

func (o *Option) init() (err error) {
	if o.OnReconnect == nil {
		o.OnReconnect = func(reconnectID string, message string, err error) {
			log.Print(reconnectID, ": ", message, " ", err)
		}
	}
	if o.HandleNotifyReturn.Return == nil {
		o.HandleNotifyReturn.Return = func(r *amqp.Return) {
			log.Print("RabbitMQNotifyReturn", r.MessageId, string(r.Body))
		}
	}
	if o.HandleNotifyReturn.Panic == nil {
		o.HandleNotifyReturn.Panic = func(panicRecover interface{}) {
			panic(panicRecover)
		}
	}
	if o.Outbox.MaxPublishTimes == 0 {
		o.Outbox.MaxPublishTimes = 10
	}
	if o.Outbox.NextPublishTime == nil {
		o.Outbox.NextPublishTime = func(n uint16) time.Duration {
			return time.Duration(n) * time.Second * 3
		}
	}
	if o.Outbox.ConsumeLoopInterval == 0 {
		o.Outbox.ConsumeLoopInterval = time.Second * 1
	}
	if o.Outbox.Timezone == nil {
		o.Outbox.Timezone = time.FixedZone("CST", 8*3600)
	}
	if o.Outbox.Logger == nil {
		o.Outbox.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	return
}
