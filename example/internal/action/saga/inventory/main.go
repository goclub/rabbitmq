package main

import (
	"context"
	"database/sql"
	xerr "github.com/goclub/error"
	rab "github.com/goclub/rabbitmq"
	m "github.com/goclub/rabbitmq/example/internal/action/model"
	"github.com/streadway/amqp"
	"log"
)

func main () {
    log.Printf("%+v", run())
}

func run () (err error) {
	ctx := context.Background()
	db, err := sql.Open("mysql", "root:somepass@(localhost:3306)/goclub_boot?charset=utf8&loc=Local&parseTime=True") ; if err != nil {
		return
	}
	conn, err := m.NewConnect() ; if err != nil {
		panic(err)
	}
	mqCh, mqChClose, err := conn.Channel() ; if err != nil {
		return
	}
	defer mqChClose()
	msgs, err := mqCh.Consume(rab.Consume{
		Queue: "q_create_order_inventory_deduction_request",
	}) ; if err != nil {
		return
	}
	for d := range msgs {
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
				msg := m.SagaCreateOrderMessage{}
				err = msg.DecodeDelivery(d) ; if err != nil {
				    return rab.Reject(err, true)
				}
				tx, err := db.Begin() ; if err != nil {
				    return rab.Reject(err, true)
				}
				query := `
UPDATE sku_inventory 
SET 
	inventory = inventory - 1 AND cost = cost + 1 
WHERE 
	sku_id = ? AND inventory > 1 
LIMIT 1`
				values := []interface{}{
					msg.SkuID,
				}
				result, err := tx.ExecContext(ctx, query, values...) ; if err != nil {
					return rab.Reject(err, true)
				}
				changeCount, err := result.RowsAffected() ; if err != nil {
					return rab.Reject(err, true)
				}
				if changeCount == 0 {
					msg.InventoryDeductionReplySuccess = false
				} else {
					msg.InventoryDeductionReplySuccess = true
				}
				msgPublish, err := msg.Publishing() ; if err != nil {
					return rab.Reject(err, true)
				}
				publish := rab.Publish{
					Exchange:   "x_create_order_inventory_deduction_reply",
					RoutingKey: "",
					Mandatory:  true,
					Msg:        msgPublish,
				}
				outbox, err := mqCh.SQLOutboxInsert(ctx, db, tx, rab.OutboxInsertOption{
					Publish:  rab.Publish{},
				}) ; if err != nil {
					return rab.Reject(err, true)
				}
				err = tx.Commit() ; if err != nil {
					return rab.Reject(err, true)
				}
				err = mqCh.Publish(publish) ; if err != nil {
					return rab.Reject(err, true)
				}
				err = outbox.Delete(ctx) ; if err != nil {
					return rab.Reject(err, true)
				}
				return rab.Ack()
			},
		}.Do(ctx)
		if err != nil {
			// 消息队列的消费者不同于 http/rpc 等接口,当出现错误时不能直接退出,退出会导致无消费者消费消息
			// 应当将错误记录到类似 sentry 的错误追踪平台
			xerr.PrintStack(err)
		}
	}
    return
}