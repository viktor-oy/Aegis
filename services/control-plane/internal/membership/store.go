package membership

import (
	"context"
	"sync"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
)

type Store interface {
	Register(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error
	Refresh(ctx context.Context, memberID string, ttl time.Duration, now time.Time) error
	ActiveMembers(ctx context.Context, now time.Time) ([]hashring.Member, error)
	AcquireIncidentLock(ctx context.Context, workerID string, incidentID string, ttl time.Duration, now time.Time) (bool, error)
}

type InMemoryStore struct {
	mu      sync.Mutex
	leases  map[string]lease
	locks   map[string]lock
}

type lease struct {
	member    hashring.Member
	expiresAt time.Time
}

type lock struct {
	incidentID string
	expiresAt   time.Time
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		leases: map[string]lease{},
		locks:  map[string]lock{},
	}
}

func (s *InMemoryStore) Register(_ context.Context, member hashring.Member, ttl time.Duration, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases[member.ID] = lease{member: member, expiresAt: now.Add(ttl)}
	return nil
}

func (s *InMemoryStore) Refresh(_ context.Context, memberID string, ttl time.Duration, now time.Time) error {
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

func (s *InMemoryStore) ActiveMembers(_ context.Context, now time.Time) ([]hashring.Member, error) {
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

func (s *InMemoryStore) AcquireIncidentLock(_ context.Context, workerID string, incidentID string, ttl time.Duration, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.locks[workerID]
	if ok && now.Before(current.expiresAt) && current.incidentID != incidentID {
		return false, nil
	}
	s.locks[workerID] = lock{incidentID: incidentID, expiresAt: now.Add(ttl)}
	return true, nil
}

