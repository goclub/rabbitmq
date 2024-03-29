package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	xerr "github.com/goclub/error"
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/action/model"
	"log"
	"math/rand"
	"time"
)

func main() {
	log.Printf("%+v", run())
}
func run() (err error) {
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
	f := m.Framework()
	for {
		// 每1秒发送一条消息
		time.Sleep(1 * time.Second)
		log.Print("send mesasge")
		err := func() (err error) {
			// 定义消息内容
			msg, err := m.UserSignupMessage{
				Email:    "some@goclub.run",
			}.Publishing() ; if err != nil {
				return
			}
			// 定义 publish
			publish := rab.Publish{
				Exchange:   f.UserSignUp.Exchange.Name,
				RoutingKey: "",   // fanout 不需要 key
				Mandatory:  true, // 要确保消息能到队列（配合 rab.HandleNotifyReturn 使用 ）
				Msg:        msg,
			}
			// 开启sql事务
			tx, err := db.Begin() ; if err != nil {
				return
			}

			// 插入新用户
			// tx.ExecContext(ctx, "INSERT INTO user VALUES (?)",)
			// 插入事务发件箱 outbox
			outbox, err := mqCh.SQLOutboxInsert(ctx, db, tx, rab.OutboxInsertOption{
				Publish:  publish,
			}) ; if err != nil {
				tx.Rollback()
				return
			}
			// sql事务提交
			err = tx.Commit() ; if err != nil {
				return
			}
			// 模拟可能出现的进程或者网络中断导致没有 publish
			if rand.Int31()%5 == 0 {
				return xerr.New("mock no publish")
			}
			// 发布消息
			err = mqCh.Publish(publish) ; if err != nil {
				return
			}
			// 在发件箱中删除已发消息
			err = outbox.Delete(ctx) ; if err != nil {
				return
			}
			return
		}() ; if err != nil {
			// 正式环境将错误发送到类似sentry的平台
		    log.Printf("%+v", err)
		}
	}
}