package rab

import (
	"context"
	"database/sql"
	xerr "github.com/goclub/error"
	xjson "github.com/goclub/json"
	"log"
	"time"
)
const localMessageStatusWaitPublish uint8 = 1
const localMessageStatusProcessing uint8 = 2
const localMessageStatusDeadLetter uint8 = 3


type OutboxInsertOption struct {
	Business uint16
	Publish Publish
}
func (c *ProxyChannel) SQLInsertOutbox(ctx context.Context, db *sql.DB, tx *sql.Tx, opt OutboxInsertOption) (outbox SQLOutbox, err error) {
	publishJson, err  := xjson.Marshal(opt.Publish) ; if err != nil {
	    return
	}
	publishCount := 0
	maxPublishTimes := c.opt.Outbox.MaxPublishTimes
	utcNextPublishTime := time.Now().Add(c.opt.Outbox.NextPublishTime(1)).In(c.opt.Outbox.TimeZone)
	createTime := time.Now().In(c.opt.Outbox.TimeZone)

	updateID := MessageID()
	values := []interface{}{
		opt.Publish.Exchange,
		opt.Publish.RoutingKey,
		updateID,

		opt.Business,
		publishJson,
		localMessageStatusWaitPublish,

		publishCount,
		maxPublishTimes,
		utcNextPublishTime,
		createTime,
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO rabbitmq_outbox 
		(
		exchange, routing_key, update_id,
		business, publish_json, status, 
		publish_count, max_publish_times, next_publish_time, create_time 
		)
		VALUES
			(?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?)`, values...) ; if err != nil {
		err = xerr.WithStack(err)
		return
	}
	localMessageID, err := result.LastInsertId() ; if err != nil {
		err = xerr.WithStack(err)
	    return
	}

	outbox = SQLOutbox{
		db: db,
		OutboxID: localMessageID,
	}
	return
}

const insertOutboxSQL = `
CREATE TABLE IF NOT EXISTS rabbitmq_outbox (
id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
exchange varchar(255) NOT NULL DEFAULT '',
routing_key varchar(255) NOT NULL DEFAULT '',
update_id char(22) NOT NULL DEFAULT '' COMMENT '用于实现并发查询的id',
business smallint(11) NOT NULL COMMENT '业务编号',
publish_json text NOT NULL COMMENT '要发送的消息和消息配置',
status tinyint(4) unsigned NOT NULL COMMENT '状态:1 等待重试 2 处理中 3 死信(发送完成的sql行会被删除)',
publish_count smallint(4) unsigned NOT NULL COMMENT '重试次数',
max_publish_times smallint(4) unsigned NOT NULL COMMENT '最大重试次数',
next_publish_time datetime NOT NULL COMMENT '下次重试最早时间',
create_time datetime NOT NULL,
PRIMARY KEY (id),
KEY update_id (update_id),
KEY status (status,next_publish_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='本地事务表/消息发件箱\n理论:https://be.nimo.run/theory/mq_outbox\ngo版本实现: https://github.com/goclub/rabbitmq';
`
func (c *ProxyChannel) SQLStartOutbox(ctx context.Context, db *sql.DB, onConsumeError func(err error)) (err error) {
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
	_, err = db.ExecContext(ctx, insertOutboxSQL) ; if err != nil {
	    return
	}
	c.opt.Outbox.Logger.Print("start sql consume outbox")
	if onConsumeError == nil {
		onConsumeError = func(err error) {
			log.Printf("%+v", err)
		}
	}
	consumeLoopInterval := c.opt.Outbox.ConsumeLoopInterval
	for {
		c.opt.Outbox.Logger.Print("query queue in " + consumeLoopInterval.String() + " ago")
		time.Sleep(consumeLoopInterval)
		err := func () (err error){
			updateID := MessageID()
			now := time.Now().In(c.opt.Outbox.TimeZone)
			// 先 NextPublishTime(2) 操作完成后再 NextPublishTime(publish_count)
			nextPublishTime := now.Add(c.opt.Outbox.NextPublishTime(2))
			query := `
UPDATE rabbitmq_outbox
SET 
 update_id = ?
,publish_count = publish_count + 1
,next_publish_time = ?
,status = ?
WHERE
    status != ?
AND next_publish_time < ?
AND publish_count < max_publish_times
LIMIT 1
`
			result, err := db.ExecContext(ctx, query,
				updateID,
				nextPublishTime,
				localMessageStatusProcessing,
				localMessageStatusDeadLetter,
				now,
			) ; if err != nil {
			    return
			}
			updateTotal, err := result.RowsAffected() ; if err != nil {
			    return
			}
			if updateTotal == 0 {
				// 如果发现无消息立即退出并充值等待时间
				consumeLoopInterval = c.opt.Outbox.ConsumeLoopInterval
				return
			}
			// 如果发现有消息则当前操作结束后立即进入下一个循环
			consumeLoopInterval = 0
			query = `SELECT id, publish_json, publish_count, max_publish_times FROM rabbitmq_outbox WHERE update_id = ? LIMIT 1`
			row := db.QueryRowContext(ctx, query, updateID)
			data := struct {
				ID int64
				Message []byte
				PublishCount uint16
				MaxPublishTimes uint16
			}{}
			err = row.Scan(&data.ID, &data.Message, &data.PublishCount, &data.MaxPublishTimes) ; if err != nil {
				if xerr.Is(err, sql.ErrNoRows) {
					err = xerr.New("goclub/rabbitmq: outbox update_id(" + updateID + ") must has")
					return
				}
				return
			}
			publish := Publish{}
			err = xjson.Unmarshal(data.Message, &publish) ; if err != nil {
			    return
			}
			err = c.Publish(publish) ; if err != nil {
				// dead letter
				if data.PublishCount >= data.MaxPublishTimes {
					c.opt.Outbox.Logger.Printf("goclub/rabbitmq:outbox: id(%d) dead letter", data.ID)
					query = `UPDATE rabbitmq_outbox SET status = ? WHERE id = ? LIMIT 1`
					_, updateErr := db.ExecContext(ctx, query, localMessageStatusDeadLetter, data.ID) ; if updateErr != nil {
						err = xerr.WrapPrefix(updateErr.Error(), err)
					    return
					}
				}
				// NextPublishTime
				query = `UPDATE rabbitmq_outbox SET next_publish_time = ? WHERE id = ? LIMIT 1`
				nextPublishTime := time.Now().In(c.opt.Outbox.TimeZone).Add(c.opt.Outbox.NextPublishTime(data.PublishCount))
				_, updateErr := db.ExecContext(ctx, query,
					nextPublishTime,
					data.ID,
				) ; if updateErr != nil {
					err = xerr.WrapPrefix(updateErr.Error(), err)
					return
				}
			    return
			}
			err = SQLOutbox{db: db, OutboxID: data.ID}.Delete(ctx) ; if err != nil {
				return
			}
			c.opt.Outbox.Logger.Printf("goclub/rabbitmq:outbox: id(%d) published", data.ID)
			return
		}() ; if err != nil {
		    onConsumeError(err)
		}
	}
}
type SQLOutbox struct {
	db *sql.DB
	OutboxID int64
}
func (l SQLOutbox) Delete(ctx context.Context) (err error) {
	_, err = l.db.ExecContext(ctx, "DELETE FROM `rabbitmq_outbox` WHERE id = ?", l.OutboxID) ; if err != nil {
		err = xerr.WithStack(err)
		return
	}
	return
}