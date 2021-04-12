# 死信队列


## 需求

了解rabbitmq死信机制:

> 队列中的消息可能会变成死信消息(dead-lettered)，进而当以下几个事件任意一个发生时，消息将会被重新发送到一个交换机：

> 消息被消费者使用basic.reject或basic.nack方法并且requeue参数值设置为false的方式进行消息确认(negatively acknowledged)
消息由于消息有效期(per-message TTL)过期
消息由于队列超过其长度限制而被丢弃
注意，队列的有效期并不会导致其中的消息过期

> 死信交换机(DLXs)就是普通的交换机，可以是任何一种类型，也可以用普通常用的方式进行声明。

参考：https://www.rabbitmq.com/dlx.html


## 建模

定义死信交换机和死信队列

```shell
Exchange                         Queue
-----------------------------------------------
dlx_example_time    -->   dlq_example_time
```


定义业务用交换机和业务用队列，并设置队列的死信

```shell
Delivery       Exchange                   Queue
----------------------------------------------------
message  -->  x_example_time   -->  q_example_time
```

```go
// queue declare dq_example_time 参数
Args: map[string]interface{}{
    "x-dead-letter-exchange": "dlx_example_time",
}
```

业务队列 `q_example_time` 出现死信时，会传递消息给死信交换机，死信交换机传递给绑定的死信队列

```shell
Queue                   Exchange                  Queue
----------------------------------------------------------------
q_example_time   -->  dlx_example_time    -->   dlq_example_time
```


## 代码

打开终端，确保在 `example/internal/dead-letter` 目录下
```shell
# 提前声明和绑定交换机队列（如果出现错误 Exception (406)  修改 migrate/main.go 中的 queueDelete 代码 ）
go run migrate/main.go
# 运行业务消费端（打开新的终端窗口）
go run consume_biz/main.go
# 运行死信消费端（打开新的终端窗口）
go run consume_dl/main.go
# 发布业务消息
go run publish/main.go
```