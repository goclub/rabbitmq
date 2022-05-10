package rab

import (
	xerr "github.com/goclub/error"
	"github.com/streadway/amqp"
	"log"
	"runtime/debug"
)

type Option struct {
	// OnReconnect
	// goclub/rabbitmq 封装后的 Connection 和 Channel 都有重连机制,当发生重连时会触发 OnReconnect
	// OnReconnect 为 nil 时默认执行 log.Print(message, string(debug.Stack()))
	OnReconnect func(message string)
	// NotifyReturn 用于订阅发送时退回的消息 需在 Publish 时配合 Mandatory 使用
	HandleNotifyReturn HandleNotifyReturn
}

type HandleNotifyReturn struct {
	// 发生 NotifyReturn 时触发
	Return func(r *amqp.Return)
	// 当 Return panic时触发 Panic
	Panic func(panicRecover interface{})
}

func (o Option) init() (err error) {
	if o.OnReconnect == nil {
		o.OnReconnect = func(message string) {
			log.Print(message, string(debug.Stack()))
		}
	}
	if o.HandleNotifyReturn.Return == nil {
		return xerr.New("rab.Dial(url , opt) opt.HandleNotifyReturn.Return can not be nil")
	}
	if o.HandleNotifyReturn.Panic == nil {
		return xerr.New("rab.Dial(url , opt) opt.HandleNotifyReturn.Panic can not be nil")
	}
	return
}
func (o Option) notifyReturn() (err error) {

	return
}
