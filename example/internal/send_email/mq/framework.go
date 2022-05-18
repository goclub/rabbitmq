package emailMessageQueue

import (
	rab "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
)

type QueueAndBind struct {
	Queue rab.QueueDeclare
	Bind rab.QueueBind
}
func Framework() (m struct {
	UserSignUp struct{
		Exchange     rab.ExchangeDeclare
		WelcomeEmail QueueAndBind
	}
	DeadLetterHumanIntervention struct{
		Exchange rab.ExchangeDeclare
		SaveToSQL QueueAndBind
	}
}) {
	m.DeadLetterHumanIntervention.Exchange = rab.ExchangeDeclare{
		Name:       "dlx_human_intervention",
		Kind:       amqp.ExchangeFanout,
		Durable:    true,
	}
	m.DeadLetterHumanIntervention.SaveToSQL.Queue = rab.QueueDeclare{
		Name:       "dlq_save_to_sql",
		Durable:    true,
	}
	m.DeadLetterHumanIntervention.SaveToSQL.Bind = rab.QueueBind{
		Queue:      m.DeadLetterHumanIntervention.SaveToSQL.Queue.Name,
		Exchange:   m.DeadLetterHumanIntervention.Exchange.Name,
	}

	m.UserSignUp.Exchange = rab.ExchangeDeclare{
		Name:    "x_user_signup",
		Kind:    amqp.ExchangeFanout,
		Durable: true,
	}
	m.UserSignUp.WelcomeEmail.Queue = rab.QueueDeclare{
		Name:    "q_welcome_email",
		Durable: true,
		Args: map[string]interface{}{
			"x-dead-letter-exchange": m.DeadLetterHumanIntervention.Exchange.Name.String(),
		},
	}
	m.UserSignUp.WelcomeEmail.Bind = rab.QueueBind{
		Queue:      m.UserSignUp.WelcomeEmail.Queue.Name,
		RoutingKey: "", // fanout 不需要 routing key
		Exchange:   m.UserSignUp.Exchange.Name,
	}
	return
}

func InitDeclareAndBind(mqCh *rab.ProxyChannel) (err error) {
	f := Framework()
	err = mqCh.ExchangeDeclare(f.DeadLetterHumanIntervention.Exchange) ; if err != nil {
	    return
	}
	_, err = mqCh.QueueDeclare(f.DeadLetterHumanIntervention.SaveToSQL.Queue) ; if err != nil {
	    return
	}
	err = mqCh.QueueBind(f.DeadLetterHumanIntervention.SaveToSQL.Bind) ; if err != nil {
	    return
	}
	err = mqCh.ExchangeDeclare(f.UserSignUp.Exchange) ; if err != nil {
	    return
	}
	_, err = mqCh.QueueDeclare(f.UserSignUp.WelcomeEmail.Queue) ; if err != nil {
	    return
	}
	err = mqCh.QueueBind(f.UserSignUp.WelcomeEmail.Bind) ; if err != nil {
	    return
	}
	return
}
