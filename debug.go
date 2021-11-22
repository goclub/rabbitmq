package rab

import "log"

var Debug bool

func debug(args ...interface{}) {
	if !Debug {
		return
	}
	args = append([]interface{}{"goclub/rab:"}, args...)
	log.Print(args...)
}

func debugf(format string, args ...interface{}) {
	if !Debug {
		return
	}
	log.Printf("goclub/rab:" + format, args...)
}
