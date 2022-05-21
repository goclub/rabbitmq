# goclub/rabbitmq

> 保姆式 golang rabbitmq 

## 特色
1. **接口友好**: 基于 `github.com/streadway/amqp` 进行封装,提供 `rab.ConsumeDelivery`等工具 防止逻辑错误导致消费者永远在消费同一个消息
2. **自动重连**:避免连接断开导致业务不可用
3. **事务发件箱(本地消息表)**:只需简单配置即可实现支持多进程并发的事务发件箱
4. **死信队列人工干预**:几行代码即可实现将死信保存到队列
5. **实战示例**: 干活满满的实战示例,绝不是只有 publish  和 consume 的 demo 代码片段. 

## 示例

1. [实战](example/internal/action/readme.md)
