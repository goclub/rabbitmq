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


type LocalMessageInsertOption struct {
	Business uint16
	Publish Publish
}
func (c *ProxyChannel) SQLInsertLocalMessage(ctx context.Context, db *sql.DB, tx *sql.Tx, opt LocalMessageInsertOption) (localMessage SQLLocalMessage, err error) {
	message, err  := xjson.Marshal(opt.Publish) ; if err != nil {
	    return
	}
	publishCount := 0
	maxPublishTimes := c.opt.LocalMessage.MaxPublishTimes
	utcNextPublishTime := time.Now().Add(c.opt.LocalMessage.MessageRetryInterval).In(c.opt.LocalMessage.TimeZone)
	createTime := time.Now().In(c.opt.LocalMessage.TimeZone)

	updateID := MessageID()
	values := []interface{}{
		opt.Publish.Exchange,
		opt.Publish.RoutingKey,
		updateID,

		opt.Business,
		message,
		localMessageStatusWaitPublish,

		publishCount,
		maxPublishTimes,
		utcNextPublishTime,
		createTime,
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO rabbitmq_local_message 
		(
		exchange, routing_key, update_id,
		business, message, status, 
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

	localMessage = SQLLocalMessage{
		db: db,
		LocalMessageID: localMessageID,
	}
	return
}

func (c *ProxyChannel) SQLConsumeLocalMessage(ctx context.Context, db *sql.DB, onError func(err error)) (err error) {
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
	c.opt.LocalMessage.Logger.Print("start sql consume local message")
	if onError == nil {
		onError = func(err error) {
			log.Printf("%+v", err)
		}
	}
	consumeLoopInterval := c.opt.LocalMessage.ConsumeLoopInterval
	for {
		c.opt.LocalMessage.Logger.Print("query queue in " + consumeLoopInterval.String() + " ago")
		time.Sleep(consumeLoopInterval)
		err := func () (err error){
			updateID := MessageID()
			now := time.Now().In(c.opt.LocalMessage.TimeZone)
			nextPublishTime := now.Add(c.opt.LocalMessage.ConsumeLoopInterval)
			query := `
UPDATE rabbitmq_local_message
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
				consumeLoopInterval = c.opt.LocalMessage.ConsumeLoopInterval
				return
			}
			// 如果发现有消息则当前操作结束后立即进入下一个循环
			consumeLoopInterval = 0
			query = `SELECT id, message, publish_count, max_publish_times FROM rabbitmq_local_message WHERE update_id = ? LIMIT 1`
			row := db.QueryRowContext(ctx, query, updateID)
			data := struct {
				ID int64
				Message []byte
				PublishCount uint16
				MaxPublishTimes uint16
			}{}
			err = row.Scan(&data.ID, &data.Message, &data.PublishCount, &data.MaxPublishTimes) ; if err != nil {
				if xerr.Is(err, sql.ErrNoRows) {
					err = xerr.New("goclub/rabbitmq: local_message update_id(" + updateID + ") must has")
					return
				}
				return
			}
			publish := Publish{}
			err = xjson.Unmarshal(data.Message, &publish) ; if err != nil {
			    return
			}
			err = c.Publish(publish)
			if err != nil {
				if data.PublishCount >= data.MaxPublishTimes {
					c.opt.LocalMessage.Logger.Printf("goclub/rabbitmq:local message: id(%d) dead letter", data.ID)
					query = `UPDATE rabbitmq_local_message SET status = ? WHERE id = ? LIMIT 1`
					_, updateErr := db.ExecContext(ctx, query, localMessageStatusDeadLetter, data.ID) ; if updateErr != nil {
						err = xerr.WrapPrefix("updateErr", err)
					    return
					}
				}
			    return
			}
			err = SQLLocalMessage{db: db, LocalMessageID: data.ID}.Delete(ctx) ; if err != nil {
				return
			}
			c.opt.LocalMessage.Logger.Printf("goclub/rabbitmq:local message: id(%d) published", data.ID)
			return
		}() ; if err != nil {
		    onError(err)
		}
	}
}
type SQLLocalMessage struct {
	db *sql.DB
	LocalMessageID int64
}
func (l SQLLocalMessage) Delete(ctx context.Context) (err error) {
	_, err = l.db.ExecContext(ctx, "DELETE FROM `rabbitmq_local_message` WHERE id = ?", l.LocalMessageID) ; if err != nil {
		err = xerr.WithStack(err)
		return
	}
	return
}