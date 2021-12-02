package rab

import (
	"fmt"
	"log"
)

var OnReconnect  = func(message string) {
	log.Print(message)
}

func notifyReconnect(message string) {
	OnReconnect("goclub/rab:" + message)
}

func notifyReconnectf(format string, args ...interface{}) {
	OnReconnect("goclub/rab:" + fmt.Sprintf(format, args))
}
