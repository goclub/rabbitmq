package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	rab "github.com/goclub/rabbitmq"
	emailMessageQueue "github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"log"
)

func main() {
	log.Printf("%+v", run())
}
func run ()(err error) {
	ctx := context.Background()
	// 连接 rabbitmq
	conn, err := emailMessageQueue.NewConnect() ; if err != nil {
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
	list, total , err := mqCh.SQLOutboxQuery(ctx, db, rab.ViewOutboxRequest{
		Exchange:    "",
		RoutingKey:  "",
		MessageID: "",
		Business:    0,
		Status:      0,
		OrderByDesc: false,
		Page:        1,
		PerPage:     10,
	}) ; if err != nil {
	    return
	}
	log.Print("total: ", total)
	for _, outbox := range list {
		log.Print(outbox.ID)
	}
	if len(list) != 0 {
		id := list[0].ID
		log.Printf("SQLOutboxSend(ctx, db, %d)", id)
		err = mqCh.SQLOutboxSend(ctx, db, []uint64{id}) ; if err != nil {
		    return
		}
	}
	return
}