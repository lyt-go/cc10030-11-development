"""处置单领域服务入口：共同值守班组对同一真实进入只能派发/领取一个有效处置单。

实现位于 ``app.services.fence_dispatch``，本模块保留独立导入路径
``app.services.disposal_order.DisposalOrderService``。
"""
from __future__ import annotations

from app.services.fence_dispatch import (
    DisposalOrderService,
    DispatchConflictError,
    DuplicateDispatchError,
    FenceDispatchError,
    OpResult,
    TeamPermissionError,
    CrossTeamClaimError,
)

__all__ = [
    "DisposalOrderService",
    "DispatchConflictError",
    "DuplicateDispatchError",
    "FenceDispatchError",
    "OpResult",
    "TeamPermissionError",
    "CrossTeamClaimError",
]
