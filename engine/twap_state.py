"""Shared settlement TWAP state predicates."""
from __future__ import annotations

from decimal import Decimal


def _to_decimal(value, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None:
            return default
        return Decimal(str(value))
    except Exception:
        return default


def is_settlement_twap_active_or_pending_delivery(
    state,
    dust: Decimal = Decimal("0.0001"),
) -> bool:
    """Return True while settlement TWAP owns the combo until delivery settles it."""
    if not getattr(state, "_settlement_twap_started", False):
        return False

    twap_task = getattr(state, "_settlement_twap_task", None)
    if twap_task is not None:
        try:
            if not twap_task.done():
                return True
        except Exception:
            return True

    if getattr(state, "_settlement_twap_result", None):
        return True

    snapshot_qty = _to_decimal(getattr(state, "_settlement_twap_qty_snapshot", Decimal("0")))
    accumulated = getattr(state, "_settlement_twap_accumulated", None)
    accumulated_filled = Decimal("0")
    if isinstance(accumulated, dict):
        accumulated_filled = _to_decimal(accumulated.get("filled"))

    return snapshot_qty > dust or accumulated_filled > dust
