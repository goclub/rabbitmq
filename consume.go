package rab

import (
	"context"
	"fmt"
	xerr "github.com/goclub/error"
	xsync "github.com/goclub/sync"
	"github.com/streadway/amqp"
)

// DeliveryResult create with rab.Ack() or rab.Reject()
type DeliveryResult struct {
	ack     bool
	requeue bool
	err     error
}

// 给 DeliveryResult 增加 Error 接口是为了避出现类似 rab.Ack() 或者 rab.Reject() 前面没有 return 的错误
func (result DeliveryResult) Error() string {
	if result.err != nil {
		return result.err.Error()
	}
	if result.ack {
		return "goclub/rabbitmq: ack"
	} else {
		return fmt.Sprintf("goclub/rabbitmq: reject requeue: %v", result.requeue)
	}
}
func Ack() DeliveryResult {
	return DeliveryResult{
		ack:     true,
		requeue: false,
		err:     nil,
	}
}

func Reject(err error, requeue bool) DeliveryResult {
	return DeliveryResult{
		ack:     false,
		requeue: requeue,
		err:     nil,
	}
}

type ConsumeDelivery struct {
	Delivery          amqp.Delivery
	RequeueMiddleware func(d *amqp.Delivery) (requeue bool)
	Handle            func(ctx context.Context, d *amqp.Delivery) DeliveryResult
}

func (h ConsumeDelivery) Do(ctx context.Context) (err error) {
	if h.Handle == nil {
		return xerr.New("goclub/rabbitmq: HandleDelivery{}.Do() Handle can not be nil")
	}
	if h.RequeueMiddleware == nil {
		return xerr.New("goclub/rabbitmq: HandleDelivery{}.Do() RequeueFilter can not be nil")
	}
	resultCh := make(chan DeliveryResult, 1)
	errCh, err := xsync.Go(func() (err error) {
		resultCh <- h.Handle(ctx, &h.Delivery)
		return nil
	})
	if err != nil {
		return
	}
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return err
	case err = <-errCh:
		return err
	case result := <-resultCh:
		if result.ack {
			return h.Delivery.Ack(false)
		} else {
			requeue := result.requeue
			if requeue {
				requeue = h.RequeueMiddleware(&h.Delivery)
			}
			err = h.Delivery.Reject(requeue)
			if err != nil {
				return
			}
			if result.err != nil {
				return result.err
			}
		}
	}
	return
}
