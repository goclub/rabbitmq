package main

import (
	"context"
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/action/model"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func main () {
	log.Printf("%+v", run())
}

func run() (err error) {
	log.Print("start consume save to sql")
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
	err = mqCh.DeadLetterSaveToSQL(ctx, rab.DeadLetterSaveToSQLOption{
		DB:        db,
		QueueName: m.Framework().DeadLetterHumanIntervention.SaveToSQL.Queue.Name,
		OnConsumerError: func(err error, d *amqp.Delivery) {
			log.Printf("goclub/rabbitmq: DeadLetterSaveToSQL %+v", err)
			jsond, err := json.Marshal(d) ; if err != nil {
				log.Printf("%+v", err)
			}
			log.Print("delivery", string(jsond))
		},
		RequeueMiddleware: func(d *amqp.Delivery) (requeue bool) {
			return m.RequeueIncrLimiter(d.MessageId, 3)
		},
		Timezone:            time.FixedZone("CST", 8*3600),
	}) ; if err != nil {
	    return
	}
	return
}
