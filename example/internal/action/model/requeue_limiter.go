package m

import "sync"

var requeueCount struct{
	Data map[string]uint
	sync.Mutex
}
// 正式项目中请使用 https://github.com/goclub/redis#IncrLimiter 实现
func RequeueIncrLimiter (messageID string, maximum uint) bool {
	requeueCount.Lock()
	defer requeueCount.Unlock()
	count := requeueCount.Data[messageID]
	if count >= maximum {
		return false
	}
	requeueCount.Data[messageID]++
	return true
}
func init () {
	requeueCount.Data = map[string]uint{}
}