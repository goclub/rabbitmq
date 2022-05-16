package rab

import (
	"context"
	"database/sql"
	"fmt"
	xerr "github.com/goclub/error"
	xjson "github.com/goclub/json"
	"log"
	"strings"
	"time"
)
const localMessageStatusWaitPublish uint8 = 1
const localMessageStatusProcessing uint8 = 2
const localMessageStatusDeadLetter uint8 = 3


type OutboxInsertOption struct {
	Business uint16
	Publish Publish
}
func (c *ProxyChannel) SQLOutboxInsert(ctx context.Context, db *sql.DB, tx *sql.Tx, opt OutboxInsertOption) (outbox SQLOutbox, err error) {
	if opt.Business == 0 {
		err = xerr.New("goclub/rabbitmq: ProxyChannel{}.SQLOutboxInsert(ctx, db, tx, opt) opt.Business can not be 0")
		return
	}
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
		opt.Publish.Msg.MessageId,
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
		exchange, routing_key, message_id, update_id,
		business, publish_json, status, 
		publish_count, max_publish_times, next_publish_time, create_time 
		)
		VALUES
			(?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?)`, values...) ; if err != nil {
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
message_id char(36) NOT NULL DEFAULT '',
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
KEY status (status,next_publish_time),
KEY business (business),
KEY exchange (exchange,routing_key),
KEY message_id (message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='本地事务表/消息发件箱\n理论:https://be.nimo.run/theory/mq_outbox\ngo版本实现: https://github.com/goclub/rabbitmq';
`
func (c *ProxyChannel) SQLOutboxStartWork(ctx context.Context, db *sql.DB, onConsumeError func(err error)) (err error) {
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
				PublishJson []byte
				PublishCount uint16
				MaxPublishTimes uint16
			}{}
			err = row.Scan(&data.ID, &data.PublishJson, &data.PublishCount, &data.MaxPublishTimes) ; if err != nil {
				if xerr.Is(err, sql.ErrNoRows) {
					err = xerr.New("goclub/rabbitmq: outbox update_id(" + updateID + ") must has")
					return
				}
				return
			}
			publish := Publish{}
			err = xjson.Unmarshal(data.PublishJson, &publish) ; if err != nil {
				err = xerr.WrapPrefix(fmt.Sprintf("sql outbox id(%d) json unmarshal fail", data.ID), err)
			    return
			}
			err = c.Publish(publish) ; if err != nil {
				err = xerr.WrapPrefix(fmt.Sprintf("sql outbox id(%d) publish fail", data.ID), err)
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
			c.opt.Outbox.Logger.Printf("goclub/rabbitmq:sql outbox: id(%d) published", data.ID)
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

type ViewOutboxRequest struct {
	Exchange string `json:"exchange"`
	RoutingKey string `json:"routingKey"`
	MessageID string `json:"messageID"`
	Business uint16 `json:"business"`
	Status uint8 `json:"status"`
	OrderByDesc bool `json:"orderByDesc"`
	Page uint64 `json:"page" default:"1"`
	PerPage uint64 `json:"perPage" default:"10"`
}
type ViewOutbox struct {
	ID uint64 `json:"id"`
	Exchange string `json:"exchange"`
	RoutingKey string `json:"routingKey"`
	MessageID string `json:"messageID"`
	Business uint16 `json:"business"`
	PublishJson string `json:"publishJson"`
	Status uint8 `json:"status"`
	CreateTime time.Time `json:"createTime"`
}
func (c *ProxyChannel) SQLOutboxQuery(ctx context.Context, db *sql.DB, req ViewOutboxRequest) (list []ViewOutbox, total uint64, err error) {
	defer func() {
		if err != nil {
			err = xerr.WithStack(err)
		}
	}()
	if req.Page == 0 {
		req.Page = 1
	}
	if req.PerPage == 0 {
		req.PerPage = 10
	}
	query := []string{
		`SELECT id, exchange, routing_key, message_id, business, publish_json, status, create_time FROM rabbitmq_outbox `,
	}
	values := []interface{}{}
	where := []string{}
	if len(req.Exchange) != 0 {
		where = append(where, "exchange = ?")
		values = append(values, req.Exchange)
	}
	if len(req.RoutingKey) != 0 {
		where = append(where, "routing_key = ?")
		values = append(values, req.RoutingKey)
	}
	if len(req.MessageID) != 0 {
		where = append(where, "message_id = ?")
		values = append(values, req.MessageID)
	}
	if req.Business != 0 {
		where = append(where, "business = ?")
		values = append(values, req.Business)
	}
	if req.Status != 0 {
		where = append(where, "status = ?")
		values = append(values, req.Status)
	}
	whereString := ""
	if len(where) != 0 {
		whereString = "WHERE " + strings.Join(where, " AND ")
	}
	query = append(query, whereString)

	countQuery := `SELECT count(*) FROM rabbitmq_outbox ` + whereString
	row := db.QueryRow(countQuery, values...)
	err = row.Scan(&total) ; if err != nil {
	    return
	}

	if req.OrderByDesc {
		query = append(query, "ORDER BY id desc")
	}
	query = append(query, "LIMIT ? OFFSET ?")
	limit := req.PerPage
	offset := req.PerPage * (req.Page - 1)
	values = append(values, limit)
	values = append(values, offset)
	rows, err := db.QueryContext(ctx, strings.Join(query, "\n"), values...) ; if err != nil {
	    return
	}
	defer rows.Close()
	for rows.Next() {
		v := ViewOutbox{}
		err = rows.Scan(&v.ID, &v.Exchange, &v.RoutingKey, &v.MessageID, &v.Business, &v.PublishJson, &v.Status, &v.CreateTime) ; if err != nil {
		    return
		}
		list = append(list, v)
	}
	return
}

func (c *ProxyChannel) SQLOutboxSend(ctx context.Context, db *sql.DB, outboxIDList []uint64) (err error) {
	if len(outboxIDList) == 0 {
		return
	}
	var placeholder []string
	for i := 0; i < len(outboxIDList); i++ {
		placeholder = append(placeholder, "?")
	}
	query := `SELECT id, publish_json FROM rabbitmq_outbox WHERE id IN(`+ strings.Join(placeholder, ",") +`)`
	values := []interface{}{}
	for _, id := range outboxIDList {
		values = append(values, id)
	}
	rows, err := db.QueryContext(ctx, query, values...) ; if err != nil {
	    return
	}
	defer rows.Close()
	type Data struct {
		ID int64
		PublishJson []byte
	}
	for rows.Next() {
		var outboxID int64
		var publishJSON []byte
		err =  rows.Scan(&outboxID, &publishJSON,) ; if err != nil {
		    return
		}
		var publish Publish 
		err = xjson.Unmarshal(publishJSON, &publish) ; if err != nil {
			err = xerr.WrapPrefix(fmt.Sprintf("sql outbox id(%d) json unmarshal fail", outboxID), err)
		    return
		}
		err = c.Publish(publish) ; if err != nil {
			return
		}
		err = SQLOutbox{
			db: db,
			OutboxID: outboxID,
		}.Delete(ctx) ; if err != nil {
			return
		}
	}
	return
}
