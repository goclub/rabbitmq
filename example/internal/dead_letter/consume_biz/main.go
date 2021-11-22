package main

import (
	rab "github.com/goclub/rabbitmq"
	xsync "github.com/goclub/sync"
	"log"
)

func main() {
	log.Print("consume biz")
	conn, err := rab.Dial("amqp://guest:guest@localhost:5672/") ; if err != nil {
		panic(err)
	}
	mqCh, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	// 消费业务消息
	msgs, err := mqCh.Consume(rab.Consume{
		Queue: rab.QueueName("q_example_time"),
	}) ; if err != nil {
		panic(err)
	}
	routine := xsync.Routine{}
	routine.Go(func() error {
		for d := range msgs {
			log.Print("receive: " + string(d.Body))
			// 代码中硬编码 reject 以便于测试死信
			err = d.Reject(false) // 参数 false 表示拒绝后不重新加入队列
			if err != nil {
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
