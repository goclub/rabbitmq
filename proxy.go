package rab

import (
	"github.com/streadway/amqp"
	"log"
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
func NotifyReturn(channel *ProxyChannel, returnHandle func(r *amqp.Return), panicHandle func(panicRecover interface{})) {
	mqNotifyReturnCh := make(chan amqp.Return, 1)
	channel.NotifyReturn(mqNotifyReturnCh)
	if panicHandle == nil {
		// default panicHandle
		panicHandle = func(panicRecover interface{}) {
			log.Print(panicRecover)
		}
	}
	go func() {
		defer func() {
			r := recover()
			if r != nil {
				panicHandle(r)
			}
		}()
		for r := range mqNotifyReturnCh {
			returnHandle(&r)
		}
	}()
}