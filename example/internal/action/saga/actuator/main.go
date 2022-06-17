package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	xerr "github.com/goclub/error"
	rab "github.com/goclub/rabbitmq"
	m "github.com/goclub/rabbitmq/example/internal/action/model"
	"github.com/streadway/amqp"
	"log"
	"net/http"
)

func main() {
	log.Printf("%+v", run())
}
func run () (err error) {
	ctx := context.Background()
	// 连接 rabbitmq
	conn, err := m.NewConnect() ; if err != nil {
		return
	}
	// 连接 channel
	mqCh, mqChClose, err := conn.Channel() ; if err != nil {
		return
	}
	// 函数结束时候关闭 channel ,mqChClose 调用时还有消息没送达rabbitmq之前会堵塞
	defer mqChClose()
	// 连接 mysql
	db, err := sql.Open("mysql", "root:somepass@(localhost:3306)/goclub_boot?charset=utf8&loc=Local&parseTime=True") ; if err != nil {
		return
	}
	_=db
	_=ctx
	_=mqCh
	go func() {
		defer func() {
			r := recover()
			if r != nil {
				log.Printf("%+v", r)
			}
		}()
		msgs, err := mqCh.Consume(rab.Consume{
			Queue:       "x_create_order_inventory_deduction_reply",
		}) ; if err != nil {
		    panic(err)
		}
		for d := range msgs {
			err = rab.ConsumeDelivery{
				Delivery:          d,
				RequeueMiddleware: func(d *amqp.Delivery) (requeue bool) {
					return m.RequeueIncrLimiter(d.MessageId,3)
				},
				Handle: func(ctx context.Context, d *amqp.Delivery) rab.DeliveryResult {
					msg := m.SagaCreateOrderMessage{}
					err = msg.DecodeDelivery(d) ; if err != nil {
					    return rab.Reject(err, true)
					}
					if msg.InventoryDeductionReplySuccess == false {
						// update saga set status = "rollback", reason = "InventoryDeductionReplyFail" where id = ?
						return
					}
					mqCh.Publish(rab.Publish{
						Exchange: "x_create_order_order_insert",
					})
				},
			}.Do(ctx) ; if err != nil {
			    log.Printf("%+v", err)
			}
		}
	}()
	http.HandleFunc("/start", func(writer http.ResponseWriter, request *http.Request) {
		err = func () (err error) {
			ctx := request.Context()
			_, err = writer.Write([]byte("start")) ; if err != nil { return }
			query := `
INSERT INTO saga (business)
VALUES
	(?);
`
			values := []interface{}{
				100,
			}
			tx, err := db.Begin(); if err != nil { return }
			result, err := tx.ExecContext(ctx, query, values...); if err != nil { return }
			sagaID, err := result.LastInsertId() ; if err != nil {
			    return
			}
			msg, err := m.SagaCreateOrderMessage{
				SagaID: uint64(sagaID),
				AccountID: 100,
				SkuID: 200,
			}.Publishing(); if err != nil { return }
			publish := rab.Publish{
				Exchange:   "x_create_order_inventory_deduction_request",
				RoutingKey: "",
				Mandatory:  true,
				Msg:        msg,
			}
			outbox, err := mqCh.SQLOutboxInsert(ctx, db, tx, rab.OutboxInsertOption{
				Publish:  publish,
			}); if err != nil { return }
			err = tx.Commit(); if err != nil { return }
			err = mqCh.Publish(publish); if err != nil { return }
			err = outbox.Delete(ctx); if err != nil { return }
			return
		}() ; if err != nil {
			err = xerr.WithStack(err)
			log.Printf("%+v", err)
			writer.WriteHeader(500)
			return
		}
		
		return
	})
	addr := ":1111"
	log.Print("http://127.0.0.1" + addr)
	return http.ListenAndServe(addr, nil)
}

