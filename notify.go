package rab

import (
	"fmt"
	"log"
	"runtime/debug"
)

var reconnectCallback []func(message string)
var OnReconnect  = func(callback func (message string)) {
	reconnectCallback = append(reconnectCallback, callback)
}
func reconnectNotify(message string) {
	if len(reconnectCallback) == 0 {
		log.Print(message)
	}
	for _, cb := range reconnectCallback {
		cb(message + "\n" + string(debug.Stack()))
	}
}

func notifyReconnect(message string) {
	reconnectNotify("goclub/rab:" + message)
}

func notifyReconnectf(format string, args ...interface{}) {
	reconnectNotify("goclub/rab:" + fmt.Sprintf(format, args...))
}
