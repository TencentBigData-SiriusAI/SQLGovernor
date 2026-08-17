"""Bayesian confidence tracking for error patterns."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass
class BetaTracker:
    """Beta(α, β) distribution for tracking pattern reliability.

    Skeptical prior: α₀=1, β₀=2 → initial confidence = 1/3.
    Requires multiple consistent observations before activating.
    """
    alpha: float = 1.0    # prior + confirmed observations
    beta: float = 2.0     # prior + counter-observations

    @property
    def confidence(self) -> float:
        """E[θ] = α / (α + β)"""
        return self.alpha / (self.alpha + self.beta)

    @property
    def observations(self) -> int:
        """Total observations (excluding prior)."""
        return int(self.alpha + self.beta - 3)  # subtract prior (1 + 2)

    def observe_positive(self):
        """Error pattern confirmed again."""
        self.alpha += 1.0

    def observe_negative(self):
        """Same context but error did NOT occur (counter-evidence)."""
        self.beta += 1.0


# Activation thresholds — set high to avoid noise injection
THRESHOLD_WARN = 0.75      # WARN: inject DB facts — needs high confidence (multiple observations)
THRESHOLD_HINT = 0.65      # HINT: inject error description — moderate bar
THRESHOLD_EXAMPLE = 0.55   # EXAMPLE: inject similar case — still need reasonable confidence


class ConfidenceTracker:
    """Tracks confidence for all error patterns.

    Key = (db_id, column_or_table, error_type) → BetaTracker
    """

    def __init__(self):
        self._trackers: Dict[Tuple[str, str, str], BetaTracker] = {}

    def get_confidence(self, db_id: str, anchor: str, error_type: str) -> float:
        key = (db_id, anchor, error_type)
        tracker = self._trackers.get(key)
        return tracker.confidence if tracker else 0.33  # prior confidence

    def observe(self, db_id: str, anchor: str, error_type: str, is_positive: bool = True):
        key = (db_id, anchor, error_type)
        if key not in self._trackers:
            self._trackers[key] = BetaTracker()
        if is_positive:
            self._trackers[key].observe_positive()
        else:
            self._trackers[key].observe_negative()

    def get_tracker(self, db_id: str, anchor: str, error_type: str) -> BetaTracker:
        key = (db_id, anchor, error_type)
        if key not in self._trackers:
            self._trackers[key] = BetaTracker()
        return self._trackers[key]
