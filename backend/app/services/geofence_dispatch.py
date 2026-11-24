"""围栏共同处置派发服务的兼容导入路径。

主实现见 ``app.services.fence_dispatch`` 与
``app.services.geofence_rule.GeofenceRuleService``。
"""
from __future__ import annotations

from app.services.fence_dispatch import (
    FenceDispatchService,
    JointDispatchMixin,
    derive_entry_hits,
)
from app.services.geofence_rule import GeofenceRuleService


class GeofenceDispatchService(GeofenceRuleService):
    """围栏判定 + 共同值守派发的合并服务，命名上更贴近“围栏派发”导入猜测。"""


__all__ = [
    "FenceDispatchService",
    "GeofenceDispatchService",
    "JointDispatchMixin",
    "GeofenceRuleService",
    "derive_entry_hits",
]
