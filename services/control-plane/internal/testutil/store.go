package testutil

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
)

type MockStore struct {
	mu           sync.Mutex
	leases       map[string]lease
	locks        map[string]lock
	workerStates   map[string]membership.WorkerState
	dlqMarkers     map[string]string
	deferredEvents map[string]deferredEvent
	acquireLockCalls int
}

type deferredEvent struct {
	payload   []byte
	expiresAt time.Time
}

type lease struct {
	member    hashring.Member
	expiresAt time.Time
}

type lock struct {
	incidentID string
	expiresAt  time.Time
}

func NewMockStore() *MockStore {
	return &MockStore{
		leases:         map[string]lease{},
		locks:          map[string]lock{},
		workerStates:   map[string]membership.WorkerState{},
		dlqMarkers:     map[string]string{},
		deferredEvents: map[string]deferredEvent{},
		acquireLockCalls: 0,
	}
}

func (s *MockStore) Register(_ context.Context, member hashring.Member, ttl time.Duration, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases[member.ID] = lease{member: member, expiresAt: now.Add(ttl)}
	return nil
}

func (s *MockStore) Refresh(_ context.Context, memberID string, ttl time.Duration, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.leases[memberID]
	if !ok {
		return nil
	}
	current.expiresAt = now.Add(ttl)
	s.leases[memberID] = current
	return nil
}

func (s *MockStore) ActiveMembers(_ context.Context, now time.Time) ([]hashring.Member, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	members := make([]hashring.Member, 0, len(s.leases))
	for id, current := range s.leases {
		if now.After(current.expiresAt) {
			delete(s.leases, id)
			continue
		}
		members = append(members, current.member)
	}
	return members, nil
}

func (s *MockStore) AcquireFSMLock(_ context.Context, workerID string, errorType string, incidentID string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquireLockCalls++
	key := workerID + ":" + errorType
	current, ok := s.locks[key]
	if ok && time.Now().Before(current.expiresAt) && current.incidentID != incidentID {
		return false, nil
	}
	s.locks[key] = lock{incidentID: incidentID, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (s *MockStore) AcquireLockCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireLockCalls
}

func (s *MockStore) ReleaseFSMLock(_ context.Context, workerID string, errorType string, incidentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := workerID + ":" + errorType
	current, ok := s.locks[key]
	if ok && current.incidentID == incidentID {
		delete(s.locks, key)
	}
	return nil
}

func (s *MockStore) SetWorkerState(_ context.Context, state membership.WorkerState, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := state.WorkerID + ":" + state.ErrorType
	s.workerStates[key] = state
	return nil
}

func (s *MockStore) GetWorkerState(_ context.Context, workerID string, errorType string) (*membership.WorkerState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := workerID + ":" + errorType
	state, ok := s.workerStates[key]
	if !ok {
		return nil, nil
	}
	return &state, nil
}

func (s *MockStore) DeleteWorkerState(_ context.Context, workerID string, errorType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := workerID + ":" + errorType
	delete(s.workerStates, key)
	return nil
}

func (s *MockStore) ListActiveWorkerStates(_ context.Context) ([]membership.WorkerState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	states := make([]membership.WorkerState, 0, len(s.workerStates))
	for _, state := range s.workerStates {
		states = append(states, state)
	}
	return states, nil
}

func (s *MockStore) SetDLQMarker(_ context.Context, workerID string, errorType string, markerData string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errorType)
	s.dlqMarkers[key] = markerData
	return nil
}

func (s *MockStore) GetDLQMarker(_ context.Context, workerID string, errorType string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errorType)
	marker, ok := s.dlqMarkers[key]
	if !ok {
		return "", nil
	}
	return marker, nil
}

func (s *MockStore) DeleteDLQMarker(_ context.Context, workerID string, errorType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errorType)
	delete(s.dlqMarkers, key)
	return nil
}

func (s *MockStore) ListDLQMarkers(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	markers := make([]string, 0, len(s.dlqMarkers))
	for key := range s.dlqMarkers {
		markers = append(markers, key)
	}
	return markers, nil
}

func (s *MockStore) DeferEvent(_ context.Context, workerID string, errorType string, eventType string, payload []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("aegis:defer:%s:%s:%s", workerID, errorType, eventType)
	s.deferredEvents[key] = deferredEvent{
		payload:   payload,
		expiresAt: time.Now().Add(ttl),
	}
	return nil
}

func (s *MockStore) GetDeferredEvent(_ context.Context, workerID string, errorType string, eventType string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("aegis:defer:%s:%s:%s", workerID, errorType, eventType)
	event, ok := s.deferredEvents[key]
	if !ok || time.Now().After(event.expiresAt) {
		return nil, nil
	}
	return event.payload, nil
}

func (s *MockStore) DeleteDeferredEvent(_ context.Context, workerID string, errorType string, eventType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("aegis:defer:%s:%s:%s", workerID, errorType, eventType)
	delete(s.deferredEvents, key)
	return nil
}

func (s *MockStore) ScanExpiringDeferredEvents(_ context.Context, tolerance time.Duration) ([]membership.WorkerState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	now := time.Now()
	var states []membership.WorkerState
	
	// Mock Store doesn't perfectly simulate etcd string splits easily without importing strings,
	// but we can parse the key manually for testing purposes or just mock it.
	// We'll import strings if we need to. Wait, testutil/store.go doesn't import strings yet.
	// I should import strings. I'll do that in another block.
	for key, event := range s.deferredEvents {
		ttl := event.expiresAt.Sub(now)
		if ttl >= 0 && ttl < tolerance {
			// Extract workerID and errorType from "aegis:defer:<workerID>:<errorType>:<eventType>"
			// I'll parse it simply assuming the mock is only used for tests that create proper keys.
			var prefix, workerID, errorType, eventType string
			fmt.Sscanf(key, "%s:%s:%s:%s:%s", &prefix, &prefix, &workerID, &errorType, &eventType)
			states = append(states, membership.WorkerState{
				WorkerID:  workerID,
				ErrorType: errorType,
			})
		}
	}
	return states, nil
}
