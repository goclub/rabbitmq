package rab

import gonanoid "github.com/matoous/go-nanoid/v2"

func MessageID() (messageID string) {
	messageID, err := gonanoid.Generate("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 22)
	if err != nil {
		// 因为入参是字母表和并且长度22不会导致错误,理论上不会出错.所以为了易用性将错误不抛出而是 panic
		panic(err)
	}
	return messageID
}