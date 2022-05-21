package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/goclub/rabbitmq/example/internal/action/model"
	"log"
)

func main() {
	log.Printf("%+v", run())
}
func run ()(err error) {
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
	err = mqCh.SQLOutboxStartWork(ctx, db, func(err error) {
		// 正式项目发送到类似 sentry 的平台进行记录
		log.Printf("%+v", err)
	}) ; if err != nil {
	    return
	}
	return
}