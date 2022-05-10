package rab

import (
	"fmt"
	xerr "github.com/goclub/error"
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"
	"time"
)

// 参考 https://github.com/isayme/go-amqp-reconnect/blob/master/rabbitmq/rabbitmq.go
type ProxyConnection struct {
	*amqp.Connection
	opt Option
}

func Dial(url string, opt Option) (conn *ProxyConnection, err error) {
	return DialConfig(url, amqp.Config{
		Heartbeat: 2 * time.Second,
		Locale:    "en_US",
	}, opt)
}
func DialConfig(url string, config amqp.Config, opt Option) (conn *ProxyConnection, err error) {
	err = opt.init()
	if err != nil {
		return
	}
	conn = &ProxyConnection{
		opt: opt,
	}
	conn.Connection, err = amqp.DialConfig(url, config)
	if err != nil {
		return
	}
	go func() {
		for {
			notifyClose, ok := <-conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				conn.opt.OnReconnect("connection closed")
				break
			}
			conn.opt.OnReconnect(fmt.Sprintf("connection closed, reason: %v", notifyClose))
			for {
				time.Sleep(time.Second)
				conn.Connection, err = amqp.DialConfig(url, config)
				// 成功重连则break
				if err == nil {
					conn.opt.OnReconnect("reconnect success")
					break
				}
				conn.opt.OnReconnect(fmt.Sprintf("reconnect failed, err: %v", err))
			}
		}
	}()
	return
}

type ProxyChannel struct {
	*amqp.Channel
	closed int32
	opt    Option
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
func (conn *ProxyConnection) Channel() (channel *ProxyChannel, channelClose func() error, err error) {
	// 防止调用 nil
	channelClose = func() error {
		return nil
	}

	channel = &ProxyChannel{
		opt: conn.opt,
	}
	amqpCh, err := conn.Connection.Channel()
	if err != nil {
		err = xerr.WithStack(err)
		return
	}
	channel.Channel = amqpCh
	channelClose = channel.Close
	mqNotifyReturnCh := make(chan amqp.Return, 1)
	channel.NotifyReturn(mqNotifyReturnCh)
	go func() {
		defer func() {
			r := recover()
			if r != nil {
				conn.opt.HandleNotifyReturn.Panic(r)
			}
		}()
		for r := range mqNotifyReturnCh {
			conn.opt.HandleNotifyReturn.Return(&r)
		}
	}()
	go func() {
		for {
			notifyClose, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			if !ok || channel.IsClosed() {
				conn.opt.OnReconnect("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			conn.opt.OnReconnect(fmt.Sprintf("channel closed, reason: %v", notifyClose))
			for {
				time.Sleep(time.Second)
				if conn.Connection != nil {
					ch, err := conn.Connection.Channel()
					if err == nil {
						conn.opt.OnReconnect("channel recreate success")
						channel.Channel = ch
						break
					}
				}
				conn.opt.OnReconnect(fmt.Sprintf("channel recreate failed, err: %v", err))
			}
		}
	}()
	return
}

func (channel *ProxyChannel) Consume(consume Consume) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)
	queue, consumer, autoAck, exclusive, noLocal, noWait, args := consume.Flat()
	firstTimeErrHandleOnce := sync.Once{}
	firstTimeErrCh := make(chan error)
	go func() {
		for {
			d, err := channel.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			shouldBreak := false
			firstTimeErrHandleOnce.Do(func() {
				firstTimeErrCh <- xerr.WithStack(err)
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
				channel.opt.OnReconnect(fmt.Sprintf("consume failed, err: %v", err))
				time.Sleep(time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}
			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(time.Second)
			if channel.IsClosed() {
				break
			}
		}
	}()
	firstTimeErr := <-firstTimeErrCh
	if firstTimeErr != nil {
		return deliveries, firstTimeErr
	}
	return deliveries, nil
}

// Publish 自动添加 MessageId 和 Timestamp
func (channel *ProxyChannel) Publish(publish Publish) (err error) {
	if publish.Msg.MessageId == "" {
		publish.Msg.MessageId = MessageID()
	}
	if publish.Msg.Timestamp.IsZero() {
		publish.Msg.Timestamp = time.Now()
	}
	exchange, key, mandatory, immediate, msg := publish.Flat()
	return channel.Channel.Publish(exchange, key, mandatory, immediate, msg)
}

func (channel *ProxyChannel) ExchangeDeclare(declare ExchangeDeclare) (err error) {
	name, kind, durable, autoDelete, internal, noWait, args := declare.Flat()
	return channel.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}
func (channel *ProxyChannel) QueueDeclare(queueDeclare QueueDeclare) (queue amqp.Queue, err error) {
	name, durable, autoDelete, exclusive, noWait, args := queueDeclare.Flat()
	return channel.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}
func (channel *ProxyChannel) QueueBind(queueBind QueueBind) (err error) {
	name, key, exchange, noWait, args := queueBind.Flat()
	return channel.Channel.QueueBind(name, key, exchange, noWait, args)
}
