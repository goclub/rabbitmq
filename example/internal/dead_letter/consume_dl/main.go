package main

import (
	rabmq "github.com/goclub/rabbitmq"
	xsync "github.com/goclub/sync"
	"log"
)

func main() {
	log.Print("consume dl")
	conn, err := rabmq.Dial("amqp://guest:guest@localhost:5672/") ; if err != nil {
		panic(err)
	}
	mqCh, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	// 消费死信队列消息
	msgs, err := mqCh.Consume(rabmq.Consume{
		Queue: rabmq.QueueName("dlq_example_time"),
	}) ; if err != nil {
		panic(err)
	}
	routine := xsync.Routine{}
	routine.Go(func() error {
		for d := range msgs {
			// 记录死信队列到数据库，通过人工干预解决死信问题
			log.Print("record dlx: " + string(d.Body))
			err  = d.Ack(false) ; if err != nil {
				log.Print(err) // 记录错误到日志或监控系统而不是退出，单个消息消费失败并不一定要让消费端停止工作
			}
		}
		return nil
	})
	err, recoverValue := routine.Wait() ; if err != nil {
		panic(err)
	} ; if recoverValue != nil {
		panic(recoverValue)
	}
}

