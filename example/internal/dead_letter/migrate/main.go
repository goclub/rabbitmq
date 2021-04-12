package main

import (
	rabmq "github.com/goclub/rabbitmq"
	"github.com/streadway/amqp"
	"log"
)

func main() {
	log.Print("start migrate")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/") ; if err != nil {
		panic(err)
	}
	mqCh, err := conn.Channel() ; if err != nil {
		panic(err)
	}
	// 声明死信交换机
	err = mqCh.ExchangeDeclare(rabmq.ExchangeDeclare{
		Name: "dlx_example_time",
		Kind: amqp.ExchangeFanout,
		Durable: true,
	}.Flat()) ; if err != nil {
		panic(err)
	}
	// 声明死信队列
	_,  err = mqCh.QueueDeclare(rabmq.QueueDeclare{
		Name: "dlq_example_time",
		Durable: true,
	}.Flat()) ; if err != nil {
		panic(err)
	}
	// 绑定死信队列到死信交换机
	err = mqCh.QueueBind(rabmq.QueueBind{
		Queue: rabmq.QueueName("dlq_example_time"),
		Exchange: rabmq.ExchangeName("dlx_example_time"),
	}.Flat()) ; if err != nil {
		panic(err)
	}

	// 声明业务用交换机
	err = mqCh.ExchangeDeclare(rabmq.ExchangeDeclare{
		Name: "x_example_time",
		Kind: amqp.ExchangeFanout,
		Durable: true,
	}.Flat()) ; if err != nil {
		panic(err)
	}
	// 如果出现错误 panic: Exception (406) Reason: "PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange' for queue 'q_example_time' in vhost '/': received the value 'dlx_example_time' of type 'longstr' but current is none"
	// 则是因为队列已存在，需要删除队列再声明.(取消下面一段代码注释)
	/*
	_, err = mqCh.QueueDelete(rabmq.QueueDelete{
		Name:"q_example_time",
		IfEmpty: true, // 确保队列是空的再删除，这样能防止意外删除数据
	}.Flat()) ; if err != nil {
		panic(err)
	}
	*/
	 */
	// 声明业务用队列
	_,  err = mqCh.QueueDeclare(rabmq.QueueDeclare{
		Name: "q_example_time",
		Durable: true,
		// 需要配置 x-dead-letter-exchange 表明死信发送到  dlx_example_time 交换机
		Args: map[string]interface{}{
			"x-dead-letter-exchange": "dlx_example_time",
		},
	}.Flat()) ; if err != nil {
		// 如果出现错误 panic: Exception (406), 需运行 QueueDelete
		panic(err)
	}
	// 绑定业务用队列到业务用交换机,并配置死信队列
	err = mqCh.QueueBind(rabmq.QueueBind{
		Queue: rabmq.QueueName("q_example_time"),
		Exchange: rabmq.ExchangeName("x_example_time"),
	}.Flat()) ; if err != nil {
		panic(err)
	}
	log.Print("migrate done")
}
