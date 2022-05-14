package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
	"log"
)

func main() {
	log.Printf("%+v", run())
}

var SendEmail uint16 = 1

func run() (err error) {
	ctx := context.Background()
	conn, err := rab.Dial("amqp://guest:guest@localhost:5672/", rab.Option{})
	if err != nil {
		panic(err)
	}
	mqCh, mqChClose, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer mqChClose()
	db, err := sql.Open("mysql", "root:somepass@(localhost:3306)/goclub_boot?charset=utf8&loc=Local&parseTime=True")
	if err != nil {
		return
	}
	tx, err := db.Begin()
	if err != nil {
		return
	}
	// 执行插入用户信息操作
	// tx.Exec("INSERT INTO `user` VALUES (?)", "nimo")

	publish := rab.Publish{
		Exchange: "x_send_email",
		Mandatory: true,
		Msg: amqp.Publishing{
			Body: []byte("abc"),
		},
	}
	localMessage, err := mqCh.SQLInsertLocalMessage(ctx, db ,tx, rab.LocalMessageInsertOption{
		Business: SendEmail,
		Publish:  publish,
	}) ; if err != nil {
	    return
	}

	err = tx.Commit();
	if err != nil {
		return
	}
	panic("模拟各种意外中断进程导致后续 publish 操作无法成功")
	err = mqCh.Publish(publish) ; if err != nil {
		return
	}
	err = localMessage.Delete(ctx) ; if err != nil {
	    return
	}
	return
}
