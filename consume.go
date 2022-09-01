package rab

import (
	"context"
	"fmt"
	xerr "github.com/goclub/error"
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
	panicCh := make(chan error, 1)
	go func() {
		defer func() {
			r := recover()
			if r != nil {
				var panicErr error
				switch v := r.(type) {
				case error:
					panicErr = v
				default:
					panicErr = xerr.New(fmt.Sprintf("%+v", r))
				}
				panicCh <- panicErr
			}
		}()
		resultCh <- h.Handle(ctx, &h.Delivery)
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		// 超时则重新入队, 特意忽略错误,因为已经存在错误
		_  = h.Delivery.Reject(h.RequeueMiddleware(&h.Delivery))
		return err
	case panicErr := <-panicCh:
		// 发生 panic 则重新入队, 特意忽略错误,因为已经存在错误
		_  = h.Delivery.Reject(h.RequeueMiddleware(&h.Delivery))
		return panicErr
	case result := <-resultCh:
		if result.ack {
			return h.Delivery.Ack(false)
		} else {
			if result.requeue == true {
				result.requeue = h.RequeueMiddleware(&h.Delivery)
			}
			// 特意忽略错误,因为已经存在错误
			_ = h.Delivery.Reject(result.requeue)
			if result.err != nil {
				return result.err
			}
		}
	}
	return
}
