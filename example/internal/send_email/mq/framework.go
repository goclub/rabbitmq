package emailMessageQueue

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
)

func Framework() (m struct {
	Exchange struct {
		SendEmail rab.ExchangeDeclare
	}
	DeadLetterExchange struct{
		HumanIntervention rab.ExchangeDeclare
	}
	Queue struct {
		SendEmail     rab.QueueDeclare
		SendEmailBind rab.QueueBind
	}
	DeadLetterQueue struct{
		SaveToSQL rab.QueueDeclare
		SaveToSQLBind rab.QueueBind
	}
}) {
	m.DeadLetterExchange.HumanIntervention = rab.ExchangeDeclare{
		Name:       "dlx_human_intervention",
		Kind:       amqp.ExchangeFanout,
		Durable:    true,
	}
	m.DeadLetterQueue.SaveToSQL = rab.QueueDeclare{
		Name:       "dlq_save_to_sql",
		Durable:    true,
	}
	m.DeadLetterQueue.SaveToSQLBind = rab.QueueBind{
		Queue:      m.DeadLetterQueue.SaveToSQLBind.Queue,
		Exchange:   m.DeadLetterExchange.HumanIntervention.Name,
	}
	m.Exchange.SendEmail = rab.ExchangeDeclare{
		Name:    "x_send_email",
		Kind:    amqp.ExchangeFanout,
		Durable: true,
	}
	m.Queue.SendEmail = rab.QueueDeclare{
		Name:    "q_send_email",
		Durable: true,
		// Args: map[string]interface{}{
		// 	"x-dead-letter-exchange": m.DeadLetterExchange.HumanIntervention.Name.String(),
		// },
	}
	m.Queue.SendEmailBind = rab.QueueBind{
		Queue:      m.Queue.SendEmail.Name,
		RoutingKey: "", // fanout 不需要 routing key
		Exchange:   m.Exchange.SendEmail.Name,
	}
	return
}

func InitDeclareAndBind(mqCh *rab.ProxyChannel) (err error) {
	f := Framework()
	// dead letter HumanIntervention
	// err = mqCh.ExchangeDeclare(f.DeadLetterExchange.HumanIntervention) ; if err != nil {
	// 	return
	// }
	// _, err = mqCh.QueueDeclare(f.DeadLetterQueue.SaveToSQL) ; if err != nil {
	// 	return
	// }
	// err = mqCh.QueueBind(f.DeadLetterQueue.SaveToSQLBind) ; if err != nil {
	// 	return
	// }

	// sendEmail
	err = mqCh.ExchangeDeclare(f.Exchange.SendEmail) ; if err != nil {
		return
	}
	_, err = mqCh.QueueDeclare(f.Queue.SendEmail) ; if err != nil {
		return
	}
	err = mqCh.QueueBind(f.Queue.SendEmailBind) ; if err != nil {
		return
	}

	return
}
