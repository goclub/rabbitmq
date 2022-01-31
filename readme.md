# rabbitmq

## 特色
1. 对 `github.com/streadway/amqp` 进行封装,让接口更友好 
2. 自动支持重连机制,避免连接断开导致业务不可用
3. 对 ack/reject 进行封装,防止逻辑错误导致消费者永远在消费同一个消息

## 示例

1. [发送邮件](./example/internal/send_email/readme.md)
1. [死信队列](./example/internal/dead_letter/readme.md)