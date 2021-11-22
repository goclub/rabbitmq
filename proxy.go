package rab

import (
	"github.com/streadway/amqp"
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
				debug("connection closed")
				break
			}
			debugf("connection closed, reason: %v", notifyClose)
			for {
				// wait 1s for reconnect
				time.Sleep(3 * time.Second)
				conn.Connection, err = amqp.DialConfig(url, config)
				// 成功重连则break
				if err == nil {
					debug("reconnect success")
					break
				}
				debugf("reconnect failed, err: %v", err)
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
				debug("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			debug("channel closed, reason: %v", notifyClose)
			for {
				time.Sleep(3 * time.Second)
				if conn.Connection != nil {
					ch, err := conn.Connection.Channel()
					if err == nil {
						debug("channel recreate success")
						channel.Channel = ch
						break
					}
				}
				debugf("channel recreate failed, err: %v", err)
			}
		}
	}()
	return
}

func (channel *ProxyChannel) Consume(consume Consume) (<-chan amqp.Delivery,error) {
	deliveries := make(chan amqp.Delivery)
	go func() {
		for {
			queue, consumer, autoAck, exclusive, noLocal, noWait , args := consume.Flat()
			d, err := channel.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait , args) ; if err != nil {
				debugf("consume failed, err: %v", err)
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