# rabbitmq 示例

涉及的知识点:
1. 声明交换机 `exchange.declare`
2. 声明队列 `queue.declare`
3. 绑定队列 `queue.bind`
4. 解绑队列 `queue.unbind`
5. Mandatory & NotifyReturn
6. 自动重连
7. RequeueMiddleware 重新入队拦截器
8. 事务发件箱
9. 死信队列

[AMQP 0-9-1 快速参考指南](http://rabbitmq.mr-ping.com/AMQP/amqp-0-9-1-quickref.html)
 

## 需求

通过消息队列发送邮件，接收到邮件发送请求后将发送任务发布到消息队列，然后立即响应"发送中"。

再启动一个进程订阅消息队列中的邮件任务。

## 建模

声明交换机和队列，并绑定交换机和队列。因为发送邮件的场景简单，所以只需要一个交换机。

### 交换机

1. 名称: x_user_signup (约定 x 前缀表示是交换机)
2. 类型: fanout （当前业务场景简单，所以直接用扇形/广播模式）
3. 持久化：true

### 队列

1. 名称: q_welcome_email (约定 q 前缀表示是队列)
3. 持久化：true

### 队列绑定交换机

1. 队列名：q_welcome_email
2. routing key: fanout 不需要 routing key
3. 交换机名：x_user_signup


### 代码

确保在 publish_email 目录下后按顺序运行

```shell
# 定义队列和交换机（采取集中管理交换机和队列的方式,你也可以提前在web界面中管理）
go run migrate/main.go

# 启动队列消费端
go run consume/main.go

# 启动发件箱消费端
go run outbox/main.go

# 发布消息到交换机（在新的终端窗口运行）
go run publish/main.go

# 查看和管理事务发件箱 
go run outbox_ui/main.go

# 死信消息保存到sql后续人工干预 
go run save_to_sql/main.go

# 查看需要人工干预的消息 
go run save_to_sql_ui/main.go
```

可多次运行 `go run publish/main.go` 观察消费情况。

反复重启关闭消费端，观察消费端离线时消息发送情况，和消费端重新上线后消息消费情况。

你可以运行 `go run unbind/main.go` 解绑队列后在运行 `go run publish/main.go` 观察 `HandleNotifyReturn` 运行结果。

> 想恢复绑定则再次运行 `go run migrate/main.go` 即可

