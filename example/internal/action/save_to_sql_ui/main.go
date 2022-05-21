package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func main () {
	log.Printf("%+v", run())
}

func run() (err error) {
	ctx := context.Background()
	// 连接 mysql
	db, err := sql.Open("mysql", "root:somepass@(localhost:3306)/goclub_boot?charset=utf8&loc=Local&parseTime=True") ; if err != nil {
		return
	}
	_=db
	_=ctx
	panic("待实现")
	return
}
