package rab

import (
	xerr "github.com/goclub/error"
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"
	"time"
)

// 参考 https://github.com/isayme/go-amqp-reconnect/blob/master/rabbitmq/rabbitmq.go
type ProxyConnection struct {
	*amqp.Connection
}
func Dial(url string) (conn *ProxyConnection, err error) {
	return DialConfig(url, amqp.Config{
		Heartbeat: 2 * time.Second,
		Locale: "en_US",
	})
}
func DialConfig(url string, config amqp.Config) (conn *ProxyConnection, err error) {
	conn = &ProxyConnection{}
	conn.Connection, err = amqp.DialConfig(url, config) ; if err != nil {
	    return
	}
	go func() {
		for {
			notifyClose, ok := <-conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				notifyReconnect("connection closed")
				break
			}
			notifyReconnectf("connection closed, reason: %v", notifyClose)
			for {
				// wait 1s for reconnect
				time.Sleep(3 * time.Second)
				conn.Connection, err = amqp.DialConfig(url, config)
				// 成功重连则break
				if err == nil {
					notifyReconnect("reconnect success")
					break
				}
				notifyReconnectf("reconnect failed, err: %v", err)
			}
		}
	}()
	return
}

type ProxyChannel struct {
	*amqp.Channel
	closed int32
}
func (ch *ProxyChannel) IsClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}
// 利用继承代理 Close获取用户主动调用的close
func (ch *ProxyChannel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}
	atomic.StoreInt32(&ch.closed, 1)
	return ch.Channel.Close()
}
func (conn *ProxyConnection) Channel() (channel *ProxyChannel, err error){
	channel = &ProxyChannel{}
	amqpCh, err := conn.Connection.Channel() ; if err != nil {
	    return
	}
	channel.Channel = amqpCh
	mqNotifyReturnCh := make(chan amqp.Return, 1)
	channel.NotifyReturn(mqNotifyReturnCh)
	go func() {
		defer func() {
			r := recover()
			if r != nil {
				for _, handle := range notifyReturnQeueue {
					handle.Panic(r)
				}
			}
		}()
		for r := range mqNotifyReturnCh {
			for _, handle := range notifyReturnQeueue {
				handle.Return(&r)
			}
		}
	}()
	go func() {
		for {
			notifyClose, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			if !ok || channel.IsClosed() {
				notifyReconnect("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			notifyReconnectf("channel closed, reason: %v", notifyClose)
			for {
				time.Sleep(3 * time.Second)
				if conn.Connection != nil {
					ch, err := conn.Connection.Channel()
					if err == nil {
						notifyReconnect("channel recreate success")
						channel.Channel = ch
						break
					}
				}
				notifyReconnectf("channel recreate failed, err: %v", err)
			}
		}
	}()
	return
}

func (channel *ProxyChannel) Consume(consume Consume) (<-chan amqp.Delivery,error) {
	deliveries := make(chan amqp.Delivery)
	queue, consumer, autoAck, exclusive, noLocal, noWait , args := consume.Flat()
	firstTimeErrHandleOnce := sync.Once{}
	firstTimeErrCh := make(chan error)
	go func() {
		for {
			d, err := channel.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait , args)
			shouldBreak := false
			firstTimeErrHandleOnce.Do(func() {
				firstTimeErrCh<-err
				if err != nil {
					// 第一次 consume 就失败,则通过 shouldBreak 退出
					shouldBreak = true
				}
			})

			if shouldBreak {
				// 退出 for 从而释放 go func
				break
			}
			if err != nil {
				notifyReconnectf("consume failed, err: %v", err)
				time.Sleep(3 * time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}
			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(3 * time.Second)
			if channel.IsClosed() {
				break
			}
		}
	}()
	firstTimeErr :=  <-firstTimeErrCh
	if firstTimeErr != nil {
		return deliveries, firstTimeErr
	}
	return deliveries, nil
}

func (channel *ProxyChannel) Publish(publish Publish) (err error) {
	exchange, key, mandatory,immediate, msg := publish.Flat()
	if publish.Msg.MessageId == "" {
		publish.Msg.MessageId = MessageID()
	}
	return channel.Channel.Publish(exchange, key, mandatory,immediate, msg)
}

func (channel *ProxyChannel)  ExchangeDeclare(declare ExchangeDeclare) (err error) {
	name, kind , durable, autoDelete, internal, noWait, args := declare.Flat()
	return channel.Channel.ExchangeDeclare(name, kind , durable, autoDelete, internal, noWait, args)
}
func (channel *ProxyChannel)  QueueDeclare(queueDeclare QueueDeclare) (queue amqp.Queue, err error) {
	name, durable, autoDelete, exclusive, noWait , args := queueDeclare.Flat()
	return channel.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait , args)
}
func (channel *ProxyChannel)  QueueBind(queueBind QueueBind) ( err error) {
	name, key, exchange, noWait, args := queueBind.Flat()
	return channel.Channel.QueueBind(name, key, exchange, noWait, args)
}

// NotifyReturn example:
// rab.NotifyReturn(mqch, func(r *amqp.Return) {
// 	data, err := xjson.Marshal(r) ; if err != nil {
// 		log.Printf("%+v", err)
// 		return
// 	}
// 	log.Print(string(data))
// }, nil)
var notifyReturnQeueue []HandleNotifyReturn
type HandleNotifyReturn struct {
	// 发生 NotifyReturn 时触发
	Return func(r *amqp.Return)
	// 当 Return panic时触发 Panic
	Panic func(panicRecover interface{})
}
func NotifyReturn(handle HandleNotifyReturn) (err error) {
	if handle.Return == nil {
		return xerr.New("rab.NotifyReturn(handle) handle.Return can not be nil")
	}
	if handle.Panic == nil {
		return xerr.New("rab.NotifyReturn(handle) handle.Panic can not be nil")
	}
	notifyReturnQeueue = append(notifyReturnQeueue, handle)
	return
}