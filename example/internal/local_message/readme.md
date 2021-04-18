# 本地消息


> 解决因分布式数据库的场景下 sql 与发布消息不是原子性而导致的数据不一致。·

## 问题

用户下单成功，保存下单信息。并通过 rabbitmq 发送消息。

````
// 插入订单数据
sql("INSERT INTO order VALUES(...)")
// 通知队列产生了新订单
mq.publish({...})
````

物流系统消费订单消息完成后续逻辑
```
mq.consume()
```

在这个过程中 mq.publish() 有可能因为网络或各种原因失败。


## 错误的方案：事务包裹发送消息

使用事务包裹 `sql()` 和 `mq.publish()` 解决不了这个问题。

```
begin()
sql("INSERT INTO order VALUES(...)")
mq.publish({...})
commit()
```

如果发布消息成功了，但是 commit 时因为网络等其他原因失败了。会导致物流系统接收到了消息，但订单系统没有报错这条记录。


## 错误的方案：反查

在物流系统接收到消息时使用消息中的 order id 去像订单系统反查订单是否存在，如果不存在则将消息标记为已消费。并记录有错误消息。

```
mq.publish({...})
sql("INSERT INTO order VALUES(...)")
```

```
func consume(msg) {
    has = orderServer.OrderByID(msg.orderID)
    if has == false {
        msg.ack()
        log("")
    }
}
```

如果 `mq.publish()` 成功了， `consume()` 在 `sql()` 之前读取到了消息，反查时订单不存在，但 `sql()` 在查询之后成功了。会导致消息没有被正确消费。


## 本地消息表

创建订单

```go
func createOrder() {
    begin()

    sql("INSERT INTO order VALUES(...)")

    localMessageID = sql("INSERT INTO order_local_message VALUES(...)")

    commit()

    mq.publish({...})

    sql("UPDATE order_local_message SET status= 'sent' WHERE id = $localMessageID")
}
```

定时任务补偿机制：

```go
func cronCompensationLocalMessage() {
    // 查询创建时间为一分钟前并且 status  != send 的本地消息 (如果存在并发查询的情况需使用 UPDATE + SELECT 标记查询法)
    // 查询到数据后重试发送消息
    // 发送成功标记本地消息 status = sent
}
```

## go代码实现

TODO
