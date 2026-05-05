package membership

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr string) *RedisStore {
	return &RedisStore{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
	}
}

func (s *RedisStore) Register(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error {
	return s.Refresh(ctx, member, ttl, now)
}

func (s *RedisStore) Refresh(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error {
	expiresAt := now.Add(ttl).UnixMilli()
	memberJSON, err := json.Marshal(member)
	if err != nil {
		return err
	}

	// Add member to the active_members sorted set with score = expiresAt
	err = s.client.ZAdd(ctx, "cp:active_members", redis.Z{
		Score:  float64(expiresAt),
		Member: string(memberJSON), // Stores full member data including ID and Address
	}).Err()
	return err
}

func (s *RedisStore) ActiveMembers(ctx context.Context, now time.Time) ([]hashring.Member, error) {
	// Clean up expired members first
	err := s.client.ZRemRangeByScore(ctx, "cp:active_members", "-inf", fmt.Sprintf("(%d", now.UnixMilli())).Err()
	if err != nil {
		return nil, err
	}

	// Fetch unexpired members
	results, err := s.client.ZRangeByScore(ctx, "cp:active_members", &redis.ZRangeBy{
		Min: strconv.FormatInt(now.UnixMilli(), 10),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}

	var members []hashring.Member
	for _, m := range results {
		var member hashring.Member
		if err := json.Unmarshal([]byte(m), &member); err != nil {
			continue // Skip malformed entries
		}
		members = append(members, member)
	}
	return members, nil
}

const acquireLockScript = `
local current = redis.call('GET', KEYS[1])
if current == false or current == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
else
    return 0
end
`

func (s *RedisStore) AcquireIncidentLock(ctx context.Context, workerID string, incidentID string, ttl time.Duration, now time.Time) (bool, error) {
	key := "incident:lock:" + workerID
	res, err := s.client.Eval(ctx, acquireLockScript, []string{key}, incidentID, ttl.Milliseconds()).Result()
	if err != nil {
		return false, err
	}
	success, ok := res.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected lua result type")
	}
	return success == 1, nil
}

const releaseLockScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
`

func (s *RedisStore) ReleaseIncidentLock(ctx context.Context, workerID string, incidentID string) error {
	key := "incident:lock:" + workerID
	_, err := s.client.Eval(ctx, releaseLockScript, []string{key}, incidentID).Result()
	return err
}

func (s *RedisStore) SetWorkerState(ctx context.Context, state WorkerState) error {
	key := fmt.Sprintf("aegis:cp:worker:state:%s:%s", state.WorkerID, state.ErrorType)
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	// Store indefinitely without TTL expiration
	return s.client.Set(ctx, key, data, 0).Err()
}

func (s *RedisStore) GetWorkerState(ctx context.Context, workerID string, errorType string) (*WorkerState, error) {
	key := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errorType)
	data, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state WorkerState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *RedisStore) DeleteWorkerState(ctx context.Context, workerID string, errorType string) error {
	key := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errorType)
	return s.client.Del(ctx, key).Err()
}

func (s *RedisStore) ListActiveWorkerStates(ctx context.Context) ([]WorkerState, error) {
	var states []WorkerState
	iter := s.client.Scan(ctx, 0, "aegis:cp:worker:state:*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		data, err := s.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}
		var state WorkerState
		if err := json.Unmarshal(data, &state); err == nil {
			states = append(states, state)
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return states, nil
}

func (s *RedisStore) SetDLQMarker(ctx context.Context, workerID string, errorType string, markerData string) error {
	key := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errorType)
	// Store indefinitely without TTL expiration
	return s.client.Set(ctx, key, markerData, 0).Err()
}

func (s *RedisStore) GetDLQMarker(ctx context.Context, workerID string, errorType string) (string, error) {
	key := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errorType)
	val, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

func (s *RedisStore) DeleteDLQMarker(ctx context.Context, workerID string, errorType string) error {
	key := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errorType)
	return s.client.Del(ctx, key).Err()
}

func (s *RedisStore) ListDLQMarkers(ctx context.Context) ([]string, error) {
	var markers []string
	iter := s.client.Scan(ctx, 0, "aegis:cp:dlq:corrupt:*", 100).Iterator()
	for iter.Next(ctx) {
		markers = append(markers, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return markers, nil
}
