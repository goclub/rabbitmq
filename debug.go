package rabmq

import "log"

var Debug bool

func debug(args ...interface{}) {
	if !Debug {
		return
	}
	args = append([]interface{}{"goclub/rabmq:"}, args...)
	log.Print(args...)
}

func debugf(format string, args ...interface{}) {
	if !Debug {
		return
	}
	log.Printf("goclub/rabmq:" + format, args...)
}
