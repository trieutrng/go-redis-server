package main

import (
	"time"
)

type Memory struct {
	store  map[string]string
	expiry chan string
}

type Option struct {
	expiry time.Duration
}

func NewMemory() *Memory {
	memory := &Memory{
		store:  make(map[string]string),
		expiry: make(chan string),
	}

	// watch expiry event asynchronously
	go memory.expiryWatcher()

	return memory
}

func (m *Memory) Get(key string) string {
	val, ok := m.store[key]
	if !ok {
		return ""
	}
	return val
}

func (m *Memory) Put(key, val string, opts Option) {
	m.store[key] = val

	// TODO: need to move this to passive expiry + sweep actively
	// px is set
	if opts.expiry != time.Duration(0) {
		go func() {
			<-time.After(opts.expiry)
			m.expiry <- key
		}()
	}
}

func (m *Memory) expiryWatcher() {
	for expiredKey := range m.expiry {
		delete(m.store, expiredKey)
	}
}
