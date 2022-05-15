
## 特色
1. 对 `github.com/streadway/amqp` 进行封装,让接口更友好
2. 自动支持重连机制,避免连接断开导致业务不可用
3. 对 ack/reject 进行封装,防止逻辑错误导致消费者永远在消费同一个消息
4. 只需简单配置即可实现支持多进程并发的事务发件箱(本地消息表)

## 示例

1. [发送邮件](./example/internal/send_email/readme.md)
2. [死信队列](./example/internal/dead_letter/readme.md)

## 待实现功能

1. 提供 outbox sql 查询接口
2. 提供消费死信队列并记录到sql的便捷方法