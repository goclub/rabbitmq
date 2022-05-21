package main

import (
	"github.com/goclub/rabbitmq/example/internal/action/model"
	"log"
)

// 集中管理交换机和队列
func main (){
	log.Print("rabbitmq migrate start")
	conn, err := m.NewConnect() ; if err != nil {
		panic(err)
	}
	mqCh, mqChClose, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	defer mqChClose()
	err = m.InitDeclareAndBind(mqCh) ; if err != nil {
		panic(err)
	}
	log.Print("rabbitmq migrate done")
}
