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
	workerStates map[string]membership.WorkerState
	dlqMarkers   map[string]string
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
		leases:       map[string]lease{},
		locks:        map[string]lock{},
		workerStates: map[string]membership.WorkerState{},
		dlqMarkers:   map[string]string{},
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

func (s *MockStore) AcquireIncidentLock(_ context.Context, workerID string, incidentID string, ttl time.Duration, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.locks[workerID]
	if ok && now.Before(current.expiresAt) && current.incidentID != incidentID {
		return false, nil
	}
	s.locks[workerID] = lock{incidentID: incidentID, expiresAt: now.Add(ttl)}
	return true, nil
}

func (s *MockStore) ReleaseIncidentLock(_ context.Context, workerID string, incidentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.locks[workerID]
	if ok && current.incidentID == incidentID {
		delete(s.locks, workerID)
	}
	return nil
}

func (s *MockStore) SetWorkerState(_ context.Context, state membership.WorkerState) error {
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
