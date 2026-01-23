from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from itertools import cycle

from .backoff import FullJitterBackoff


@dataclass(frozen=True)
class OwnerDecision:
    accepted: bool
    target: str
    redirected: bool
    message: str


class ControlPlaneDiscovery:
    def __init__(self, bootstrap_urls: Iterable[str], backoff: FullJitterBackoff | None = None) -> None:
        urls = list(bootstrap_urls)
        if not urls:
            raise ValueError("at least one bootstrap control-plane URL is required")
        self._bootstrap_urls = urls
        self._bootstrap_cycle = cycle(urls)
        self._backoff = backoff or FullJitterBackoff()
        self.last_owner_hint: str | None = None

    def next_target(self) -> str:
        return self.last_owner_hint or next(self._bootstrap_cycle)

    def accept(self, target: str) -> OwnerDecision:
        self._backoff.reset()
        self.last_owner_hint = target
        return OwnerDecision(True, target, False, "accepted by owner")

    def redirect(self, owner_hint: str) -> OwnerDecision:
        self.last_owner_hint = owner_hint
        return OwnerDecision(False, owner_hint, True, "redirected to owner")

    def owner_failed(self) -> float:
        self.last_owner_hint = None
        return self._backoff.next_sleep()

