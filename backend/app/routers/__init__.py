"""业务路由汇总：工作流语义路由优先注册，通用资源路由兜底。"""
from __future__ import annotations

from app.routers import fence_workflow, resource

# fence_workflow 的 /api/fence-workflow/... 含与 /api/{module}/{entry_id}
# 同段数的 GET 路径，必须放在通用资源路由之前注册。
ROUTERS = [fence_workflow, resource]
