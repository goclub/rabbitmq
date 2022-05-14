package rab

import (
	"github.com/streadway/amqp"
	"log"
	"runtime/debug"
	"time"
)

type Option struct {
	// OnReconnect
	// goclub/rabbitmq 封装后的 Connection 和 Channel 都有重连机制,当发生重连时会触发 OnReconnect
	// OnReconnect 为 nil 时默认执行 log.Print(message, string(debug.Stack()))
	OnReconnect func(message string)
	// NotifyReturn 用于订阅发送时退回的消息 需在 Publish 时配合 Mandatory 使用
	HandleNotifyReturn HandleNotifyReturn
	LocalMessage LocalMessageOption
}
type LocalMessageOption struct {
	MaxPublishTimes             uint16        `default:"3"`
	MessageRetryInterval        time.Duration `default:"3s"`
	ConsumeLoopInterval			time.Duration `default:"1s"`
	TimeZone					*time.Location `default:"time.FixedZone("CST", 8*3600) china"`
	Logger 						*log.Logger	   `default:"log.Default()"`
}

type HandleNotifyReturn struct {
	// 发生 NotifyReturn 时触发
	Return func(r *amqp.Return)
	// 当 Return panic时触发 Panic
	Panic func(panicRecover interface{})
}

func (o *Option) init() (err error) {
	if o.OnReconnect == nil {
		o.OnReconnect = func(message string) {
			log.Print(message, string(debug.Stack()))
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
	if o.LocalMessage.MaxPublishTimes == 0 {
		o.LocalMessage.MaxPublishTimes = 3
	}
	if o.LocalMessage.MessageRetryInterval == 0 {
		o.LocalMessage.MessageRetryInterval = time.Second * 3
	}
	if o.LocalMessage.ConsumeLoopInterval == 0 {
		o.LocalMessage.ConsumeLoopInterval = time.Second * 1
	}
	if o.LocalMessage.TimeZone == nil {
		o.LocalMessage.TimeZone = time.FixedZone("CST", 8*3600)
	}
	if o.LocalMessage.Logger == nil {
		o.LocalMessage.Logger = log.Default()
	}


	return
}
