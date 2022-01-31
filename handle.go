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
type HandleDelivery struct {
	Delivery amqp.Delivery
	RequeueMiddleware func (d amqp.Delivery) (requeue bool)
	Handle func(d amqp.Delivery) DeliveryResult
}
func (h HandleDelivery) Do(ctx context.Context) (err error) {
	if h.Handle == nil {
		return xerr.New("goclub/rabbitmq: HandleDelivery{}.Do() Handle can not be nil")
	}
	if h.RequeueMiddleware == nil {
		return xerr.New("goclub/rabbitmq: HandleDelivery{}.Do() RequeueFilter can not be nil")
	}
	resultCh := make(chan DeliveryResult)
	errRecoverCh := xsync.Go(func() (err error) {
		resultCh <- h.Handle(h.Delivery)
		return nil
	})
	select {
		case  errRecover := <- errRecoverCh:
			if errRecover.Err != nil {
				return errRecover.Err
			}
			if errRecover.Recover != nil {
				return xerr.New(fmt.Sprintf("%v", errRecover.Recover))
			}
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
		case <-ctx.Done():
			err = ctx.                                                                                                                                                                                                                                                                                                          Err()
		return err
	}
	return
}
