package heartbeat

import (
	"container/heap"
	"sync"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type WorkerState struct {
	WorkerID        string
	LastHeartbeatAt time.Time
	DeadlineAt      time.Time
	State           state.WorkerHealthState
	Generation      uint64
}

type HeartbeatDeadline struct {
	WorkerID   string
	DeadlineAt time.Time
	Generation uint64
}

type Tracker struct {
	mu      sync.Mutex
	timeout time.Duration
	workers map[string]WorkerState
	queue   deadlineHeap
}

func NewTracker(timeout time.Duration) *Tracker {
	return &Tracker{
		timeout: timeout,
		workers: map[string]WorkerState{},
		queue:   deadlineHeap{},
	}
}

func (t *Tracker) Observe(workerID string, now time.Time) WorkerState {
	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.workers[workerID]
	current.WorkerID = workerID
	current.LastHeartbeatAt = now
	current.DeadlineAt = now.Add(t.timeout)
	current.State = state.WorkerHealthy
	current.Generation++
	t.workers[workerID] = current
	heap.Push(&t.queue, HeartbeatDeadline{
		WorkerID:   workerID,
		DeadlineAt: current.DeadlineAt,
		Generation: current.Generation,
	})
	return current
}

func (t *Tracker) NextDeadline() (HeartbeatDeadline, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.queue.Len() > 0 {
		item := t.queue[0]
		current, ok := t.workers[item.WorkerID]
		if !ok || current.Generation != item.Generation {
			heap.Pop(&t.queue)
			continue
		}
		return item, true
	}
	return HeartbeatDeadline{}, false
}

func (t *Tracker) Expired(now time.Time) []WorkerState {
	t.mu.Lock()
	defer t.mu.Unlock()

	var expired []WorkerState
	for t.queue.Len() > 0 {
		item := t.queue[0]
		if now.Before(item.DeadlineAt) {
			break
		}
		heap.Pop(&t.queue)
		current, ok := t.workers[item.WorkerID]
		if !ok || current.Generation != item.Generation {
			continue
		}
		if now.Before(current.DeadlineAt) {
			continue
		}
		current.State = state.WorkerSuspected
		t.workers[item.WorkerID] = current
		expired = append(expired, current)
	}
	return expired
}

func (t *Tracker) Snapshot(workerID string) (WorkerState, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	current, ok := t.workers[workerID]
	return current, ok
}

type deadlineHeap []HeartbeatDeadline

func (h deadlineHeap) Len() int { return len(h) }

func (h deadlineHeap) Less(i, j int) bool {
	if h[i].DeadlineAt.Equal(h[j].DeadlineAt) {
		return h[i].WorkerID < h[j].WorkerID
	}
	return h[i].DeadlineAt.Before(h[j].DeadlineAt)
}

func (h deadlineHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *deadlineHeap) Push(x any) { *h = append(*h, x.(HeartbeatDeadline)) }

func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

