package membership

import (
	"context"
	"encoding/json"
	"fmt"

	"strings"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdStore struct {
	client *clientv3.Client
}

func NewEtcdStore(endpoints []string) (*EtcdStore, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &EtcdStore{
		client: cli,
	}, nil
}

func (s *EtcdStore) Register(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error {
	return s.Refresh(ctx, member, ttl, now)
}

func (s *EtcdStore) Refresh(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error {
	// Instead of ZSET, we use a lease for each member.
	// We store the key `cp:active_members/<member_id>` with the lease.
	leaseResp, err := s.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}

	memberJSON, err := json.Marshal(member)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("aegis:cp:chashring:active_members/%s", member.ID)
	_, err = s.client.Put(ctx, key, string(memberJSON), clientv3.WithLease(leaseResp.ID))
	return err
}

func (s *EtcdStore) ActiveMembers(ctx context.Context, now time.Time) ([]hashring.Member, error) {
	// Fetch all keys with prefix `cp:active_members/`
	resp, err := s.client.Get(ctx, "aegis:cp:chashring:active_members/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var members []hashring.Member
	for _, kv := range resp.Kvs {
		var member hashring.Member
		if err := json.Unmarshal(kv.Value, &member); err != nil {
			continue
		}
		members = append(members, member)
	}
	return members, nil
}

func (s *EtcdStore) AcquireFSMLock(ctx context.Context, workerID string, errorType string, incidentID string, ttl time.Duration) (bool, error) {
	key := fmt.Sprintf("aegis:cp:state:fsm:lock:%s:%s", workerID, errorType)

	// Create a lease for the TTL
	leaseResp, err := s.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return false, err
	}

	// Strictly require CreateRevision == 0 for isolation (mutual exclusion).
	// We wont attempt a re-entrant TTL reset if the value matches incidentID,
	// because two distinct CP replicas could process the same incident concurrently,
	// defeating mutual exclusion.
	txn := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, incidentID, clientv3.WithLease(leaseResp.ID)))

	txnResp, err := txn.Commit()
	if err != nil {
		return false, err
	}

	return txnResp.Succeeded, nil
}

func (s *EtcdStore) ReleaseFSMLock(ctx context.Context, workerID string, errorType string, incidentID string) error {
	key := fmt.Sprintf("aegis:cp:state:fsm:lock:%s:%s", workerID, errorType)

	txn := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", incidentID)).
		Then(clientv3.OpDelete(key))

	_, err := txn.Commit()
	return err
}

func (s *EtcdStore) SetWorkerState(ctx context.Context, state WorkerState, topic string, partition int, offset int64) error {
	key := fmt.Sprintf("aegis:cp:state:worker:%s:%s", state.WorkerID, state.ErrorType)
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// If it's a Kafka event, enforce fencing
	if topic != "" {
		fencingKey := fmt.Sprintf("aegis:cp:state:fencing:%s:%d", topic, partition)
		offsetStr := fmt.Sprintf("%020d", offset) // zero-pad to 20 digits for lexicographical comparison

		getResp, err := s.client.Get(ctx, fencingKey)
		if err != nil {
			return err
		}

		if len(getResp.Kvs) == 0 {
			// Doesn't exist yet, we require CreateRevision == 0 in Txn to prevent races
			txnResp, err := s.client.Txn(ctx).
				If(clientv3.Compare(clientv3.CreateRevision(fencingKey), "=", 0)).
				Then(
					clientv3.OpPut(key, string(data)),
					clientv3.OpPut(fencingKey, offsetStr),
				).
				Commit()
			if err != nil {
				return err
			}
			if !txnResp.Succeeded {
				return fmt.Errorf("fencing token race condition: offset %d rejected", offset)
			}
			return nil
		}

		// It exists, compare lexicographically in Go, then enforce in Txn via ModRevision
		storedOffsetStr := string(getResp.Kvs[0].Value)
		modRev := getResp.Kvs[0].ModRevision

		if offsetStr <= storedOffsetStr {
			return fmt.Errorf("fencing token rejected: incoming offset %d is not greater than stored offset for partition %d", offset, partition)
		}

		// Execute Txn ensuring ModRevision hasn't changed
		txnResp, err := s.client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(fencingKey), "=", modRev)).
			Then(
				clientv3.OpPut(key, string(data)),
				clientv3.OpPut(fencingKey, offsetStr),
			).
			Commit()
		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			return fmt.Errorf("fencing token race condition on update: offset %d rejected", offset)
		}
		return nil
	}

	// No fencing required, just put
	_, err = s.client.Put(ctx, key, string(data))
	return err
}

func (s *EtcdStore) GetWorkerState(ctx context.Context, workerID string, errorType string) (*WorkerState, error) {
	key := fmt.Sprintf("aegis:cp:state:worker:%s:%s", workerID, errorType)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var state WorkerState
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *EtcdStore) DeleteWorkerState(ctx context.Context, workerID string, errorType string) error {
	key := fmt.Sprintf("aegis:cp:state:worker:%s:%s", workerID, errorType)
	_, err := s.client.Delete(ctx, key)
	return err
}

func (s *EtcdStore) ListActiveWorkerStates(ctx context.Context) ([]WorkerState, error) {
	resp, err := s.client.Get(ctx, "aegis:cp:state:worker:", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var states []WorkerState
	for _, kv := range resp.Kvs {
		var state WorkerState
		if err := json.Unmarshal(kv.Value, &state); err == nil {
			states = append(states, state)
		}
	}
	return states, nil
}

func (s *EtcdStore) SetDLQMarker(ctx context.Context, workerID string, errorType string, markerData string) error {
	key := fmt.Sprintf("aegis:cp:state:dlq:corrupt:%s:%s", workerID, errorType)
	_, err := s.client.Put(ctx, key, markerData)
	return err
}

func (s *EtcdStore) GetDLQMarker(ctx context.Context, workerID string, errorType string) (string, error) {
	key := fmt.Sprintf("aegis:cp:state:dlq:corrupt:%s:%s", workerID, errorType)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (s *EtcdStore) DeleteDLQMarker(ctx context.Context, workerID string, errorType string) error {
	key := fmt.Sprintf("aegis:cp:state:dlq:corrupt:%s:%s", workerID, errorType)
	_, err := s.client.Delete(ctx, key)
	return err
}

func (s *EtcdStore) ListDLQMarkers(ctx context.Context) ([]string, error) {
	resp, err := s.client.Get(ctx, "aegis:cp:state:dlq:corrupt:", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var markers []string
	for _, kv := range resp.Kvs {
		markers = append(markers, string(kv.Key))
	}
	return markers, nil
}

func (s *EtcdStore) DeferEvent(ctx context.Context, workerID string, errorType string, eventType string, payload []byte, ttl time.Duration) error {
	key := fmt.Sprintf("aegis:cp:state:defer:%s:%s:%s", workerID, errorType, eventType)

	leaseResp, err := s.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}

	_, err = s.client.Put(ctx, key, string(payload), clientv3.WithLease(leaseResp.ID))
	return err
}

func (s *EtcdStore) GetDeferredEvent(ctx context.Context, workerID string, errorType string, eventType string) ([]byte, error) {
	key := fmt.Sprintf("aegis:cp:state:defer:%s:%s:%s", workerID, errorType, eventType)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func (s *EtcdStore) DeleteDeferredEvent(ctx context.Context, workerID string, errorType string, eventType string) error {
	key := fmt.Sprintf("aegis:cp:state:defer:%s:%s:%s", workerID, errorType, eventType)
	_, err := s.client.Delete(ctx, key)
	return err
}

func (s *EtcdStore) ScanExpiringDeferredEvents(ctx context.Context, tolerance time.Duration) ([]WorkerState, error) {
	resp, err := s.client.Get(ctx, "aegis:cp:state:defer:", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var states []WorkerState
	for _, kv := range resp.Kvs {
		key := string(kv.Key)

		leaseID := kv.Lease
		if leaseID == 0 {
			continue // No lease? shouldn't happen based on how we write it
		}

		timeToLiveResp, err := s.client.TimeToLive(ctx, clientv3.LeaseID(leaseID))
		if err != nil {
			continue
		}

		ttl := time.Duration(timeToLiveResp.TTL) * time.Second
		if ttl >= 0 && ttl < tolerance {
			prefix := "aegis:cp:state:defer:"
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			remainder := strings.TrimPrefix(key, prefix)
			parts := strings.SplitN(remainder, ":", 3)
			if len(parts) >= 2 {
				workerID := parts[0]
				errorType := parts[1]
				states = append(states, WorkerState{
					WorkerID:  workerID,
					ErrorType: errorType,
				})
			}
		}
	}
	return states, nil
}
