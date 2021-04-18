package main

import (
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	"github.com/goclub/rabbitmq/example/internal/send_email/service"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

func main () {
	conn, err := emailMessageQueue.NewConnect() ; if err != nil {
		panic(err)
	}
	mqCh, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	notifyReturn := mqCh.NotifyReturn(make(chan amqp.Return))
	go func() {
		// 使用 for 持续的获取结果，如果没有 for 直接  data := <- notifyREturn 会导致只能获取第一次的通知
		for data := range notifyReturn {
			log.Print("消息没有被发送到队列(业务中可以存入db和监控系统进一步处理)" + string(data.Body))
		}
	}()
	log.Print("start mesasge done")
	err = emailService.SendEmail(emailService.Email{
		To: "abc@domain.com",
		Subject: strconv.Itoa(time.Now().Second()),
	}, mqCh) ; if err != nil {
		panic(err)
	}
	// 延迟是为了让 data := <- notifyReturn 有足够的运行时间,正式项目中会通过 http/tcp listen  常驻进程，就不需要 sleep
	time.Sleep(1* time.Second)
	log.Print("send mesasge done")

}