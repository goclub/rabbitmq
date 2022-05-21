package rab

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	xerr "github.com/goclub/error"
	xjson "github.com/goclub/json"
	"github.com/streadway/amqp"
	"log"
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
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
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
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
	// 防止调用 nil
	channelClose = func() error {
		return nil
	}

	channel = &ProxyChannel{
		opt: conn.opt,
	}
	amqpCh, err := conn.Connection.Channel()
	if err != nil {
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
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
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
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
	name, kind, durable, autoDelete, internal, noWait, args := declare.Flat()
	return channel.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}
func (channel *ProxyChannel) QueueDeclare(queueDeclare QueueDeclare) (queue amqp.Queue, err error) {;defer func() {
	if err != nil {
		err = xerr.WithStack(err)
	}
}()
	name, durable, autoDelete, exclusive, noWait, args := queueDeclare.Flat()
	return channel.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}
func (channel *ProxyChannel) QueueBind(queueBind QueueBind) (err error) {
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
	name, key, exchange, noWait, args := queueBind.Flat()
	return channel.Channel.QueueBind(name, key, exchange, noWait, args)
}

type DeadLetterSaveToSQLOption struct {
	DB *sql.DB
	QueueName QueueName
	OnConsumerError func(err error, d *amqp.Delivery)
	RequeueMiddleware func(d *amqp.Delivery) (requeue bool)
	Timezone        *time.Location `default:"time.FixedZone("CST", 8*3600) china"`

}
func (opt DeadLetterSaveToSQLOption) initAndCheck (ctx context.Context)(err error) {
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
	if opt.DB == nil {
		err = xerr.New("goclub/rabbitmq: ProxyChannel{}.DeadLetterSaveToSQL(ctx, opt) opt.db can not be nil")
		return
	}
	err = opt.DB.PingContext(ctx) ; if err != nil {
	    return
	}
	if opt.QueueName == "" {
		err = xerr.New("goclub/rabbitmq: ProxyChannel{}.DeadLetterSaveToSQL(ctx, opt) opt.QueueName can not be empty string")
		return
	}
	if opt.OnConsumerError == nil {
		opt.OnConsumerError = func(err error, d *amqp.Delivery) {
			log.Printf("goclub/rabbitmq: DeadLetterSaveToSQL %+v", err)
			jsond, err := json.Marshal(d) ; if err != nil {
				log.Printf("%+v", err)
			}
			log.Print("delivery", string(jsond))
		}
	}
	if opt.RequeueMiddleware == nil {
		err = xerr.New("goclub/rabbitmq: ProxyChannel{}.DeadLetterSaveToSQL(ctx, opt) opt.RequeueMiddleware can not be nil")
		return
	}
	if opt.Timezone == nil {
		opt.Timezone = time.FixedZone("CST", 8*3600)
	}
	return
}
// DeadLetterSaveToSQL
// https://www.rabbitmq.com/dlx.html
func(channel *ProxyChannel) DeadLetterSaveToSQL(ctx context.Context, opt DeadLetterSaveToSQLOption) (err error) {
	err = opt.initAndCheck(ctx) ; if err != nil {
	    return
	}
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
	db := opt.DB
	msgs, err := channel.Consume(Consume{
		Queue:       opt.QueueName,
		ConsumerTag: "goclub/rabbitmq:DeadLetterSaveToSQL",
	}) ; if err != nil {
	    return
	}
	for delivery := range msgs {
		err = ConsumeDelivery{
			Delivery: delivery,
			// RequeueMiddleware: opt.RequeueMiddleware,
			RequeueMiddleware: func(d *amqp.Delivery) (requeue bool) {
				return true
			},
			Handle: func(ctx context.Context, d *amqp.Delivery) DeliveryResult {
				type Death struct {
					Exchange    string    `json:"exchange"`
					Queue       string    `json:"queue"`
					Reason      string    `json:"reason"`
					RoutingKeys []string  `json:"routing-keys"`
					Time        string `json:"time"`
				}
				headers := struct {
					XDeath []Death `json:"x-death"`
					XFirstDeathExchange string `json:"x-first-death-exchange"`
					XFirstDeathQueue    string `json:"x-first-death-queue"`
					XFirstDeathReason   string `json:"x-first-death-reason"`
				}{}

				var firstDeath Death
				headersJson, err := xjson.Marshal(d.Headers) ; if err != nil {
					// 理论上不会出错所以调用 OnConsumerError 并忽略错误
					opt.OnConsumerError(xerr.WithStack(err), d)
					err = nil
					headersJson = []byte(`{}`)
				}
				err = xjson.Unmarshal(headersJson, &headers) ; if err != nil {
					// 理论上不会出错所以调用 OnConsumerError 并忽略错误
					opt.OnConsumerError(xerr.WithStack(err), d)
					err = nil
				}
				if len(headers.XDeath) != 0 {
					firstDeath = headers.XDeath[0]
				}
				var firstDeathTime time.Time
				firstDeathTime, err = time.Parse(time.RFC3339, firstDeath.Time) ; if err != nil {
					// 理论上不会出错所以调用 OnConsumerError 并忽略错误
					opt.OnConsumerError(xerr.WithStack(err), d)
					err = nil
				}
				firstDeathTime = firstDeathTime.In(opt.Timezone)
					query := `
INSERT INTO rabbitmq_dead_letter 
    (
     message_id, message_time, exchange, routing_key, 
     first_death_exchange, first_death_queue, first_death_reason,
     first_death_routing_keys_json, first_death_time,
     message_json, create_time
     )
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`
				message_json, err := xjson.Marshal(d) ; if err != nil {
					// 理论上不会出错所以调用 OnConsumerError 并忽略错误
					opt.OnConsumerError(err, d)
					err = nil
				}
				firstDeathRoutingKeysJSON, err := xjson.Marshal(firstDeath.RoutingKeys) ; if err != nil {
					// 理论上不会出错所以调用 OnConsumerError 并忽略错误
					opt.OnConsumerError(err, d)
					err = nil
				}
				values := []interface{}{
					d.MessageId, d.Timestamp.In(opt.Timezone), d.Exchange, d.RoutingKey,
					firstDeath.Exchange, firstDeath.Queue, firstDeath.Reason,
					firstDeathRoutingKeysJSON,
					firstDeathTime,
					message_json, time.Now().In(opt.Timezone),
				}
				_, err = db.ExecContext(ctx, query , values...) ; if err != nil {
					// 插入sql可能因为网络或sql不稳定导致失败,此时应该 requeue
					opt.OnConsumerError(err, d)
					return Reject(err, true)
				}
				return Ack()
			},
		}.Do(ctx) ; if err != nil {
			opt.OnConsumerError(err, nil)
		}
	}
	return
}