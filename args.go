package rabmq

import "github.com/streadway/amqp"

type ExchangeName string
func (name ExchangeName) String() string {
	return string(name)
}
type ExchangeDeclare struct {
	Name ExchangeName // 交换器的名称
	Kind string // 交换器类型 amqp.ExchangeFanout amqp.ExchangeFanout amqp.ExchangeTopic amqp.ExchangeHeaders
	Durable bool // 设置是否持久化。durable设置为true表示持久化，反之是非持久化。持久化可以将交换器存盘，在服务器重启的时候不会丢失相关信息。
	AutoDelete bool // 设置是否自动删除。autoDelete设置为true则表示自动删除。自动删除的前提是至少有一个队列或者交换器与这个交换器绑定，之后所有与这个交换器绑定的队列或者交换器都与此解绑。注意不能错误地把这个参数理解为：“当与此交换器连接的客户端都断开时，RabbitMQ会自动删除本交换器
	Internal bool // 设置是否是内置的。如果设置为true，则表示是内置的交换器，客户端程序无法直接发送消息到这个交换器中，只能通过交换器路由到交换器这种方式。
	NoWait bool // 当noWait为true时，无需等待服务器的确认即可进行声明。如果没有特殊的缘由和应用场景，并不建议使用这个方法。
	Args map[string]interface{} // 其他一些结构化参数，比如alternate-ex-change
}
func (v ExchangeDeclare) Flat() (name, kind string, durable, autoDelete, internal, noWait bool, args map[string]interface{})  {
	return v.Name.String(),v.Kind,v.Durable,v.AutoDelete,v.Internal,v.NoWait,v.Args
}

type ExchangeDeclarePassive struct {
	ExchangeDeclare
}
type ExchangeDelete struct {
	Name string // 交换器名称
	IfUnused bool // ifUnused用来设置是否在交换器没有被使用的情况下删除。如果isUnused设置为true，则只有在此交换器没有被使用的情况下才会被删除；如果设置false，则无论如何这个交换器都要被删除。
	NoWait bool// 当noWait为true时，不要等待服务器确认exchange删除已完成。删除通道失败可能会关闭通道。
}
func (v ExchangeDelete) Flat() (name string, ifUnused, noWait bool)  {
	return v.Name,v.IfUnused,v.NoWait
}

type ExchangeBind struct {
	DestinationExchange string
	SourceExchange string
	RoutingKey string
	NoWait bool
	Args map[string]interface{}
}
func (v ExchangeBind) Flat() (destination, key, source string, noWait bool, args map[string]interface{}) {
	return v.DestinationExchange, v.RoutingKey, v.SourceExchange, v.NoWait, v.Args
}
type ExchangeUnbind struct {
	ExchangeBind
}


type QueueName string
func (name QueueName) String() string {
	return string(name)
}
type QueueDeclare struct {
	Name QueueName //  队列名称
	Durable bool // 设置是否持久化。为true则设置队列为持久化。持久化的队列会存盘，在服务器重启的时候可以保证不丢失相关信息。
	AutoDelete bool // 设置是否自动删除。为true则设置队列为自动删除。自动删除的前提是：至少有一个消费者连接到这个队列，之后所有与这个队列连接的消费者都断开时，才会自动删除。不能把这个参数错误地理解为：“当连接到此队列的所有客户端断开时，这个队列自动删除”，因为生产者客户端创建这个队列，或者没有消费者客户端与这个队列连接时，都不会自动删除这个队列。
	Exclusive bool // 是否排它，排它队适用一个客户端同时发送和读取消息的应用场景
	NoWait bool //当noWait为true时，无需等待服务器的确认即可进行声明。如果没有特殊的缘由和应用场景，并不建议使用这个方法。
	Args map[string]interface{} // 设置队列的其他一些参数，如x-me s s age-ttl、x-expire s、x-max-length、x-max-length-bytes、x-dead-letter-exchange、x-deadletter-routing-key、x-max-priority等。

}
func (v QueueDeclare) Flat()  (name string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) {
	return v.Name.String(),v.Durable, v.AutoDelete, v.Exclusive, v.NoWait, v.Args
}
func exampleQueueDeclare(ch amqp.Channel) {
	_, _ = ch.QueueDeclare(QueueDeclare{}.Flat())
}
type QueueDelete struct {
	Name string // 队列名称
	IfUnused bool // ifUnused用来设置是否在交换器没有被使用的情况下删除。如果isUnused设置为true，则只有在此队列没有被使用的情况下才会被删除；如果设置false，则无论如何这个队列都要被删除。
	IfEmpty bool // 队列为空的情况下才删除
	NoWait bool// 当noWait为true时，不要等待服务器确认queue删除完成如果队列 无法删除，将引发通道异常，通道将被关闭。
}
func (v QueueDelete) Flat() (name string, ifUnused, ifEmpty, noWait bool)  {
	return v.Name,v.IfUnused,v.IfEmpty, v.NoWait
}
func exampleQueueDelete(ch amqp.Channel) {
	_, _ = ch.QueueDelete(QueueDelete{}.Flat())
}
type RoutingKey string
func (key RoutingKey) String() string {
	return string(key)
}
type QueueBind struct {
	Queue QueueName // 队列名称
	RoutingKey RoutingKey // 用来绑定队列和交换器的路由键
	Exchange ExchangeName // 交换器名称
	NoWait bool  // 当noWait为true时，不要等待服务器确认queue绑定完成
	Args map[string]interface{}
}
func (v QueueBind) Flat() (name, key, exchange string, noWait bool, args map[string]interface{}) {
	return v.Queue.String(),v.RoutingKey.String(),v.Exchange.String(),v.NoWait,v.Args
}
func exampleQueueBind(ch amqp.Channel) {
	_= ch.QueueBind(QueueBind{}.Flat())
}

type QueueUnbind struct {
	Queue QueueName // 队列名称
	RoutingKey RoutingKey // 用来解绑队列和交换器的路由键
	Exchange ExchangeName // 交换器名称
	Args map[string]interface{}
}
func (v QueueUnbind) Flat() (name, key, exchange string, args map[string]interface{}) {
	return v.Queue.String(),v.RoutingKey.String(),v.Exchange.String(),v.Args
}
func exampleQueueUnBind(ch amqp.Channel) {
	_= ch.QueueUnbind(QueueUnbind{}.Flat())
}



type Publish struct {
	Exchange ExchangeName // 交换机名
	RoutingKey RoutingKey
	Mandatory bool // mandatory参数告诉服务器至少将该消息路由到一个队列中，否则将消息返回给生产者。
	Immediate bool // RabbitMQ 3.0版本开始去掉了对immediate参数的支持， immediate参数告诉服务器，如果该消息关联的队列上有消费者，则立刻投递；如果所有匹配的队列上都没有消费者，则直接将消息返还给生产者，不用将消息存入队列而等待消费者了。
	Msg amqp.Publishing
}
func (v Publish) Flat() (exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
	return v.Exchange.String(), v.RoutingKey.String(), v.Mandatory, v.Immediate, v.Msg
}
func exampleBasicPublish(ch amqp.Channel) {
	_ = ch.Publish(Publish{}.Flat())
}


type Consume struct {
	Queue       QueueName // 队列名称
	ConsumerTag string // 消费者标签，区分多个消费者
	NoLocal     bool // 设置为true则表示不能将同一个Connection中生产者发送的消息传送给这个Connection中的消费者；
	NoAck       bool // 设置是否自动确认。建议设成false，即不自动确认
	Exclusive   bool // 排它
	NoWait      bool
	Args   map[string]interface{}
}
func (v Consume) Flat() (queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args map[string]interface{}) {
	return v.Queue.String(),v.ConsumerTag, v.NoAck,v.Exclusive,v.NoLocal,v.NoWait,v.Args
}