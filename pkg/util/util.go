package util

import "time"

// StopTimer 停止并且释放计时器请确保当前的 goroutine 是时间事件频道的唯一消费者
func StopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// RestTimer 停止计时器,释放事件,并且用d的时间长度来重置计时器
func RestTimer(timer *time.Timer, d time.Duration) {
	StopTimer(timer)
	timer.Reset(d)
}
