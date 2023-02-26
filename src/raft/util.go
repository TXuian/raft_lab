package raft

import "log"

// Debugging
// const Debug = true
const Debug = false 

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Min[T int32 | float32](left T, right T) T {
	if left < right { return left }
	return right
}