import pytest
from services.agent.client import ControlPlaneDiscovery


def test_discovery_no_urls():
    with pytest.raises(ValueError, match="at least one bootstrap"):
        ControlPlaneDiscovery([])

def test_discovery_cycle():
    d = ControlPlaneDiscovery(["url1", "url2"])
    assert d.next_target() == "url1"
    assert d.next_target() == "url2"
    assert d.next_target() == "url1"

def test_discovery_accept():
    d = ControlPlaneDiscovery(["url1", "url2"])
    # Target from cycle
    assert d.next_target() == "url1"
    decision = d.accept("url1")
    assert decision.accepted is True
    assert decision.target == "url1"
    assert decision.redirected is False
    
    # After accept, hint is locked
    assert d.next_target() == "url1"

def test_discovery_redirect():
    d = ControlPlaneDiscovery(["url1", "url2"])
    decision = d.redirect("url3")
    assert decision.accepted is False
    assert decision.redirected is True
    assert decision.target == "url3"
    
    # next target is the hint
    assert d.next_target() == "url3"

def test_discovery_owner_failed():
    d = ControlPlaneDiscovery(["url1", "url2"])
    d.redirect("url3")
    assert d.next_target() == "url3"
    
    # owner fails
    delay = d.owner_failed()
    assert delay > 0
    # hint is cleared, should fall back to bootstrap cycle
    assert d.next_target() == "url1"
