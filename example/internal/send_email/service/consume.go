package emailService

import (
	"fmt"
	rabmq "github.com/goclub/rabbitmq"
	"github.com/goclub/rabbitmq/example/internal/send_email/mq"
	xsync "github.com/goclub/sync"
	"github.com/streadway/amqp"
	"log"
)

func ConsumeSendEmail(mqCh *amqp.Channel) (err error) {
	msgs, err := mqCh.Consume(rabmq.Consume{
		Queue: emailMessageQueue.Model().Queue.SendEmail.Name,
	}.Flat())
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
				return err
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

