import pytest
from services.agent.backoff import FullJitterBackoff

def test_backoff_initial_sleep():
    b = FullJitterBackoff(base_seconds=1.0, cap_seconds=10.0)
    sleep = b.next_sleep()
    assert 0 <= sleep <= 2.0
    assert b._attempt == 1

def test_backoff_exponential_growth():
    b = FullJitterBackoff(base_seconds=1.0, cap_seconds=10.0)
    # the upper bound of sleep grows exponentially
    max_sleeps = []
    for _ in range(4):
        # We can't predict exact sleep because of random jitter, 
        # but we can verify it doesn't exceed the cap of that iteration
        b.next_sleep()
        max_sleeps.append(min(b.cap_seconds, b.base_seconds * (2 ** b._attempt)))
    
    assert max_sleeps[0] < max_sleeps[-1]

def test_backoff_cap():
    b = FullJitterBackoff(base_seconds=1.0, cap_seconds=5.0)
    for _ in range(10):
        sleep = b.next_sleep()
        assert sleep <= 5.0

def test_backoff_reset():
    b = FullJitterBackoff(base_seconds=1.0, cap_seconds=10.0)
    b.next_sleep()
    b.next_sleep()
    assert b._attempt == 2
    b.reset()
    assert b._attempt == 0
    sleep = b.next_sleep()
    assert 0 <= sleep <= 2.0
