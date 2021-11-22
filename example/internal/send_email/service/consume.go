package emailService

import (
	"fmt"
	rab "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	xsync "github.com/goclub/sync"
	"log"
)

func ConsumeSendEmail(mqCh *rab.ProxyChannel) (err error) {
	msgs, err := mqCh.Consume(rab.Consume{
		Queue: emailMessageQueue.Model().Queue.SendEmail.Name,
	})
	routine := xsync.Routine{}
	routine.Go(func() error {
		for d := range msgs {
			var msg emailMessageQueue.SendEmailMessage
			err := msg.Decode(d.Body) ; if err != nil {
				return err
			}
			log.Print("consume: from " + msg.From + ", to " + msg.To + "(" + msg.Subject + ")")
			// 消费完成后应答消息处理完成
			err = d.Ack(false) ; if err != nil {
				log.Print(err) // 记录错误到日志或监控系统而不是退出，单个消息消费失败并不一定要让消费端停止工作
			}
		}
		return nil
	})
	err, recoverValue := routine.Wait() ; if err != nil {
		return
	} ; if recoverValue != nil {
		return fmt.Errorf("%v", recoverValue)
	}
	return nil
}

