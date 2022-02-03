package rab

import (
	"context"
	xerr "github.com/goclub/error"
	xsync "github.com/goclub/sync"
	"github.com/streadway/amqp"
)

// DeliveryResult create with rab.Ack() or rab.Reject()
type DeliveryResult struct {
	ack bool
	requeue bool
	err error
}
func Ack () DeliveryResult {
	return DeliveryResult{
		ack: true,
		requeue: false,
		err: nil,
	}
}

func Reject (err error, requeue bool) DeliveryResult {
	return DeliveryResult{
		ack: false,
		requeue: requeue,
		err: nil,
	}
}
type ConsumeDelivery struct {
	Delivery amqp.Delivery
	RequeueMiddleware func (d amqp.Delivery) (requeue bool)
	Handle func(d *amqp.Delivery) DeliveryResult
}
func (h ConsumeDelivery) Do(ctx context.Context) (err error) {
	if h.Handle == nil {
		return xerr.New("goclub/rabbitmq: HandleDelivery{}.Do() Handle can not be nil")
	}
	if h.RequeueMiddleware == nil {
		return xerr.New("goclub/rabbitmq: HandleDelivery{}.Do() RequeueFilter can not be nil")
	}
	resultCh := make(chan DeliveryResult, 1)
	errCh := xsync.Go(func() (err error) {
		resultCh <- h.Handle(&h.Delivery)
		return nil
	})
	select {
		case <-ctx.Done():
			err = ctx.Err()
		return err
		case  err = <- errCh:
			return err
		case result := <- resultCh:
			if result.ack {
				return h.Delivery.Ack(false)
			} else {
				requeue := result.requeue
				if requeue {
					requeue = h.RequeueMiddleware(h.Delivery)
				}
				err = h.Delivery.Reject(requeue) ; if err != nil {
				    return
				}
				if result.err != nil {
					return result.err
				}
			}
	}
	return
}
