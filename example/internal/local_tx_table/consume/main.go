package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	rab "github.com/goclub/rabbitmq"
	"log"
)

func main() {
	log.Printf("%+v", run())
}

var SendEmail uint32 = 1

func run() (err error) {
	/*
	update mq_local_message
	set
	 upload_lock_key = "1"
	,utc_next_publish_time = "2022-05-14 00:00:00"
	,publish_count =  publish_count + 1
	where
		publish_count < max_publish_times
	and utc_next_publish_time < "2022-05-16 00:00:00"
	*/
	ctx := context.Background()
	conn, err := rab.Dial("amqp://guest:guest@localhost:5672/", rab.Option{})
	if err != nil {
		return
	}
	mqCh, mqChClose, err := conn.Channel()
	if err != nil {
		return
	}
	defer mqChClose()
	db, err := sql.Open("mysql", "root:somepass@(localhost:3306)/goclub_boot?charset=utf8&loc=Local&parseTime=True")
	if err != nil {
		return
	}
	err = mqCh.SQLConsumeLocalMessage(ctx, db, func(err error) {
		// 正式项目发送到 sentry
		log.Printf("%+v", err)
	}) ; if err != nil {
	    return
	}
	return
}
