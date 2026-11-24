"""共同值守围栏处置工作流 HTTP 入口。

服务层是主能力载体；这里额外提供一组语义化 REST 路径，便于接口级复查/联调。
所有请求体均为自由 JSON，字段命名对中英文/近义参数保持宽容。
"""
from __future__ import annotations

import inspect
from typing import Any

from fastapi import APIRouter, Body, HTTPException

from app.services.fence_dispatch import FenceDispatchService

router = APIRouter(prefix="/api/fence-workflow", tags=["共同值守围栏处置"])
service = FenceDispatchService()


def _kwargs(payload: Any) -> dict[str, Any]:
    return payload if isinstance(payload, dict) else {}


@router.post("/hits")
@router.post("/hits/calculate")
def calculate_hits(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    report = payload.get("report") or payload.get("quality_report") or payload
    hits = service.calculate_valid_hits_from_quality_report(report, **{
        k: v for k, v in payload.items() if k not in ("report", "quality_report")})
    return {"ok": True, "total": len(hits), "items": list(hits), "hits": list(hits)}


@router.post("/quality-reports/freeze")
def freeze_report(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return service.freeze_quality_report(**payload)


@router.post("/candidates")
def register_candidate(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    result = service.register_candidate(**payload)
    return {"ok": result["ok"], "message": result["message"], "entry": result.get("entry")}


@router.post("/candidates/{candidate_ref}/open")
def open_candidate(candidate_ref: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    result = service.open_candidate(candidate_ref, **payload)
    return {"ok": result["ok"], "message": result["message"], "entry": result.get("entry")}


@router.post("/reviews/timing")
@router.post("/candidates/{candidate_ref}/timing-review")
def timing_review(candidate_ref: str | None = None,
                  payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    if candidate_ref is not None:
        payload = {**payload, "candidate_ref": candidate_ref}
    result = service.complete_timing_review(**payload)
    return {"ok": result["ok"], "message": result["message"], "entry": result.get("entry"),
            "segment": result.get("segment")}


@router.post("/disposals")
@router.post("/orders")
def submit_disposal(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    result = service.submit_disposal(**payload)
    return {
        "ok": result["ok"], "message": result["message"],
        "entry": result.get("entry"), "order": result.get("order"),
        "created": result.get("created"), "claimed": result.get("claimed"),
        "code": result.get("code"), "dispatch_count": result.get("dispatch_count"),
    }


@router.get("/orders")
def list_orders(entry_key: str | None = None, batch_key: str | None = None) -> dict[str, Any]:
    items = service.list_disposal_orders(entry_key=entry_key, batch_key=batch_key)
    return {"items": items, "total": len(items)}


@router.get("/orders/{order_ref}")
def get_order(order_ref: str) -> dict[str, Any]:
    order = service.get_disposal_order(order_ref)
    if order is None:
        raise HTTPException(status_code=404, detail="处置单不存在")
    return order


@router.post("/orders/{order_ref}/claim")
def claim_order(order_ref: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    result = service.claim_disposal_order(order_ref, **payload)
    if not result["ok"] and result.get("code") == "CROSS_TEAM_FORBIDDEN":
        raise HTTPException(status_code=403, detail=result["message"])
    return {"ok": result["ok"], "message": result["message"], "order": result.get("order"),
            "code": result.get("code")}


@router.get("/orders/{order_ref}/review")
@router.get("/orders/{order_ref}/evidence")
def review_order(order_ref: str, team: str | None = None) -> dict[str, Any]:
    # 只读复查：不做归属拦截
    return service.review_order(order_ref, team=team)


@router.post("/orders/{order_ref}/sign")
def sign_order(order_ref: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    result = service.sign_joint_window(order_ref, **payload)
    return {"ok": result["ok"], "message": result["message"],
            "joint_signed": result.get("joint_signed"),
            "signed_count": result.get("signed_count")}


@router.post("/windows")
def create_window(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    result = service.create_joint_window(**payload)
    return {"ok": result["ok"], "window": result.get("window")}


@router.post("/windows/{window_ref}/sign")
def sign_window(window_ref: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    result = service.sign_joint_window(window_ref=window_ref, **payload)
    return {"ok": result["ok"], "joint_signed": result.get("joint_signed")}


@router.get("/trace")
def trace(entry_key: str | None = None) -> dict[str, Any]:
    return service.trace_entry(entry_key)


# 通用方法调用入口：/api/fence-workflow/invoke/submit_disposal
_PUBLIC_METHODS = {
    name for name, fn in inspect.getmembers(FenceDispatchService, predicate=inspect.isfunction)
    if not name.startswith("_")
}


@router.post("/invoke/{method_name}")
def invoke(method_name: str, payload: dict[str, Any] = Body(default_factory=dict)) -> Any:
    if method_name not in _PUBLIC_METHODS:
        raise HTTPException(status_code=404, detail=f"未知工作流方法：{method_name}")
    fn = getattr(service, method_name)
    args = payload.pop("args", []) if isinstance(payload, dict) else []
    result = fn(*args, **payload)
    if isinstance(result, (list, dict)):
        return result
    return {"ok": True, "result": result}
