/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"sync"
	"time"
)

const (
	slowSetSleepDelay = 100 * time.Millisecond
)

type FailureTimestamp[T ~string] struct {
	FailureType T
	Timestamp   time.Time
}

type SlowSet[T ~string] struct {
	sync.RWMutex
	// retentionTime is the time after which an item will be removed from the set
	// this indicates, how long before an operation on pvc can be retried.
	retentionTime time.Duration
	workSet       map[string]FailureTimestamp[T]
}

func NewSlowSet[T ~string](retTime time.Duration) *SlowSet[T] {
	workSet := make(map[string]FailureTimestamp[T])
	return &SlowSet[T]{
		retentionTime: retTime,
		workSet:       workSet,
	}
}

// Add adds a pvcKey to the set with given failureType and
// adds with the current timestamp if key is not already present.
func (s *SlowSet[T]) Add(key string, failureType T) bool {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.workSet[key]; ok {
		return false
	}

	s.workSet[key] = FailureTimestamp[T]{FailureType: failureType, Timestamp: time.Now()}
	return true
}

func (s *SlowSet[T]) Contains(key string) bool {
	s.RLock()
	defer s.RUnlock()

	_, ok := s.workSet[key]
	return ok
}

func (s *SlowSet[T]) RemoveAll(key string) {
	s.Lock()
	defer s.Unlock()

	delete(s.workSet, key)
}

// Remove removes the key from the set only if key is present and the failureType matches.
// The reason we are also checking for failure type is because, we don't want controller expansion
// success to remove the key from the set which was added because of failed node expansion.
func (s *SlowSet[T]) Remove(key string, failureType T) bool {
	s.Lock()
	defer s.Unlock()

	failureTimestamp, ok := s.workSet[key]
	if ok && failureTimestamp.FailureType == failureType {
		delete(s.workSet, key)
		return true
	}
	return false
}

func (s *SlowSet[T]) TimeRemaining(key string) time.Duration {
	s.RLock()
	defer s.RUnlock()

	if startTimestamp, ok := s.workSet[key]; ok {
		return s.retentionTime - time.Since(startTimestamp.Timestamp)
	}
	return 0
}

func (s *SlowSet[T]) Run(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			time.Sleep(slowSetSleepDelay)
			for key, t := range s.workSet {
				if time.Since(t.Timestamp) > s.retentionTime {
					s.RemoveAll(key)
				}
			}
		}
	}
}
