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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
)

func TestSlowSet(t *testing.T) {
	nodeExpansionFailing := v1.PersistentVolumeClaimNodeResizeInfeasible
	controllerExpansionFailing := v1.PersistentVolumeClaimControllerResizeInfeasible

	tests := []struct {
		name          string
		retentionTime time.Duration
		testFunc      func(*SlowSet[v1.ClaimResourceStatus]) bool
	}{
		{
			name: "Should not change time of a key if added multiple times",
			testFunc: func(s *SlowSet[v1.ClaimResourceStatus]) bool {
				key := "key"
				s.Add(key, nodeExpansionFailing)
				time1 := s.workSet[key]
				s.Add(key, nodeExpansionFailing)
				time2 := s.workSet[key]
				return time1 == time2
			},
		},
		{
			name:          "Should remove key after retention time",
			retentionTime: 200 * time.Millisecond,
			testFunc: func(s *SlowSet[v1.ClaimResourceStatus]) bool {
				key := "key"
				s.Add(key, controllerExpansionFailing)
				time.Sleep(300 * time.Millisecond)
				return !s.Contains(key)
			},
		},
		{
			name:          "Should not remove key before retention time",
			retentionTime: 200 * time.Millisecond,
			testFunc: func(s *SlowSet[v1.ClaimResourceStatus]) bool {
				key := "key"
				s.Add(key, controllerExpansionFailing)
				time.Sleep(100 * time.Millisecond)
				return s.Contains(key)
			},
		},
		{
			name:          "Should return time remaining for added keys",
			retentionTime: 300 * time.Millisecond,
			testFunc: func(s *SlowSet[v1.ClaimResourceStatus]) bool {
				key := "key"
				s.Add(key, controllerExpansionFailing)
				time.Sleep(100 * time.Millisecond)
				timeRemaining := s.TimeRemaining(key)
				return timeRemaining > 0 && timeRemaining < 300*time.Millisecond
			},
		},
		{
			name:          "should not remove node expansion failing type if controller expansion failing type is removed",
			retentionTime: 200 * time.Millisecond,
			testFunc: func(s *SlowSet[v1.ClaimResourceStatus]) bool {
				key := "key"
				s.Add(key, nodeExpansionFailing)
				s.Remove(key, controllerExpansionFailing)
				time.Sleep(100 * time.Millisecond)
				return s.Contains(key)
			},
		},
		{
			name:          "should remove node expansion failing type if node expansion failing type is removed",
			retentionTime: 200 * time.Millisecond,
			testFunc: func(s *SlowSet[v1.ClaimResourceStatus]) bool {
				key := "key"
				s.Add(key, nodeExpansionFailing)
				s.Remove(key, nodeExpansionFailing)
				time.Sleep(100 * time.Millisecond)
				return !s.Contains(key)
			},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			s := NewSlowSet[v1.ClaimResourceStatus](test.retentionTime)
			stopCh := make(chan struct{}, 1)
			go s.Run(stopCh)
			defer close(stopCh)
			if !test.testFunc(s) {
				t.Errorf("Test failed")
			}
		})
	}
}
