from __future__ import annotations

from dataclasses import dataclass
from random import Random


@dataclass
class FullJitterBackoff:
    base_seconds: float = 0.25
    cap_seconds: float = 15.0
    seed: int | None = None

    def __post_init__(self) -> None:
        self._random = Random(self.seed)
        self._attempt = 0

    def next_sleep(self) -> float:
        self._attempt += 1
        upper = min(self.cap_seconds, self.base_seconds * (2 ** self._attempt))
        return self._random.uniform(0, upper)

    def reset(self) -> None:
        self._attempt = 0

