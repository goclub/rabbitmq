package main

import (
	"context"
	xerr "github.com/goclub/error"
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/action/model"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func main() {
	conn, err := m.NewConnect()
	if err != nil {
		panic(err)
	}
	mqCh, mqChClose, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	defer mqChClose()
	log.Print("start consume queue")
	err = ConsumeSendEmail(mqCh)
	if err != nil {
		panic(err)
	}
}

func ConsumeSendEmail(mqCh *rab.ProxyChannel) (err error) {
	ctx := context.Background()
	f := m.Framework()
	msgs, err := mqCh.Consume(rab.Consume{
		Queue: f.UserSignUp.WelcomeEmail.Queue.Name,
	})
	if err != nil {
		return
	}
	for d := range msgs {
		// 设置 timeout 防止意外"堵塞"导致消费者一直在消费某个消息,注意即使当发送超时并返回了超时错误. Handle 可能还是会执行
		handleCtx, cancel := context.WithTimeout(ctx, time.Second*2)
		defer cancel()
		// 使用 rab.HandleDelivery 可以简化消费.避免"堵塞"
		err := rab.ConsumeDelivery{
			Delivery: d,
			// 通过 重新入队中间件控制同一个消费只重复入队3次,避免一些无法被消费的消息反复消费
			RequeueMiddleware: func(d *amqp.Delivery) (requeue bool) {
				// 同一消息最多能重新入队3次
				// 正式项目中请使用 https://github.com/goclub/redis#IncrLimiter 实现
				return m.RequeueIncrLimiter(d.MessageId, 3)
			},
			Handle: func(ctx context.Context, d *amqp.Delivery) rab.DeliveryResult {
				var err error
				// 通过 return rab.Reject 拒绝消息, err 会通过 Do(handleCtx)(err error) 传递
				// requeue := true
				// return rab.Reject(err, requeue)

				var msg m.UserSignupMessage
				log.Print("received message")
				err = msg.DecodeDelivery(d)
				if err != nil {
					// 不重新入队,因为 json decode 失败即使重新入队再次消费还是会错误
					return rab.Reject(err, false)
				}
				log.Print( "欢迎 " + msg.Email)
				// 消费完成后应答消息处理完成
				return rab.Ack()
			},
		}.Do(handleCtx)
		if err != nil {
			// 消息队列的消费者不同于 http/rpc 等接口,当出现错误时不能直接退出,退出会导致无消费者消费消息
			// 应当将错误记录到类似 sentry 的错误追踪平台
			xerr.PrintStack(err)
		}
	}
	return nil
}
