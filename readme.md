# goclub/rabbitmq

> 保姆式 golang rabbitmq 

## 特色 

1. **接口友好**: 基于 `github.com/streadway/amqp` 进行封装,提供 `rab.ConsumeDelivery`等工具 防止逻辑错误导致消费者永远在消费同一个消息. **自动重连**:避免连接断开导致业务不可用
3. **事务发件箱(本地消息表)**:只需简单配置即可实现支持多进程并发的事务发件箱
4. **死信队列人工干预**:几行代码即可实现将死信保存到队列
5. **实战示例**: 干活满满的实战示例,绝不是只有 publish  和 consume 的 demo 代码片段. 

## 示例

1. [实战](example/internal/action/readme.md)


## 常见问题

### 为什么不直接使用 [streadway/amqp](https://github.com/streadway/amqp)?

因为 `streadway/amqp` 没有[特色](#特色)中的部分功能

### 为什么底层库是 [streadway/amqp](https://github.com/streadway/amqp) 而不是 [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go)?

`rabbitmq/amqp091-go` 是基于 `github.com/streadway/amqp` 的基础上迭代的, 而前者不保障API向前兼容.

**grabbitmq/amqp091-go 原文**
> This client retains key API elements as practically possible. It is, however, open to reasonable breaking public API changes suggested by the community. We don't have the "no breaking public API changes ever" rule and fully recognize that a good client API evolves over time.

等到出现某些常用功能基于 `streadway/amqp` 无法满足时,可能会新开发一个 `github.com/goclub/amqp` 库.

目前 `streadway/amqp` 的功能完全能覆盖日常开发需求