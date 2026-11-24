"""共同值守班组围栏进入命中 -> 唯一处置单 全链路核对。

该文件同时作为本次能力的可执行规格：
- 从冻结轨迹质量报告计算外点->内点有效命中明细；
- 旧班登记/打开候选，接班班组完成时序复核；
- 两班重复/并发提交只产生一个有效处置单，首交班组领取；
- 跨班越权领取被拦截，只读复查仍可见证据；
- 共同窗口由两班签字；原始候选与正式命中出处均可追溯。
"""
from __future__ import annotations

import shutil
import tempfile
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app.services.fence_dispatch import (
    DisposalOrderService,
    FenceDispatchService,
)
from app.services.geofence_rule import GeofenceRuleService


T0 = 1_795_000_000.0
T_OUT = T0 + 60
T_IN = T0 + 120
WINDOW = (T0, T0 + 600)
OLD = "甲班（旧班）"
SUCC = "乙班（接班班组）"


@pytest.fixture(autouse=True)
def _self_contained_data_root(monkeypatch):
    """即使外层 conftest 被替换，本文件也始终使用独立临时数据目录。"""
    tmpdir = Path(tempfile.mkdtemp(prefix="joint_dispatch_test_"))
    from app.services import base as base_mod
    monkeypatch.setattr(base_mod, "DATA_ROOT", tmpdir)
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


def frozen_report():
    return {
        "报告编号": "QR-JOINT-001",
        "批次编号": "BATCH-JOINT-001",
        "frozen": True,
        "is_frozen": True,
        "report_checksum": "fixed-checksum-1",
        "conclusion": "合格",
        "joint_window": {"start": WINDOW[0], "end": WINDOW[1]},
        "observations": [
            {"obs_id": "OBS-1", "point_id": "P1", "timestamp": T_OUT,
             "lat": 39.9100, "lon": 116.4100, "inside": False},
            {"obs_id": "OBS-2", "point_id": "P2", "timestamp": T_IN,
             "lat": 39.9102, "lon": 116.4102, "inside": True},
        ],
    }


@pytest.fixture
def svc():
    return GeofenceRuleService()


def test_calculate_valid_hit_detail_outer_then_inner(svc):
    hits = svc.calculate_valid_hits_from_quality_report(frozen_report(), window=WINDOW)
    assert len(hits) == 1
    detail = hits[0]
    # 第一条观测在外、第二条在内
    assert detail["outer_obs_id"] == "OBS-1"
    assert detail["inner_obs_id"] == "OBS-2"
    assert detail["outer"]["inside"] is False
    assert detail["inner"]["inside"] is True
    # 采纳的外点、内点与先后顺序
    assert detail["outer_point_id"] == "P1"
    assert detail["inner_point_id"] == "P2"
    assert detail["point_order"] == ["outer", "inner"]
    assert detail["先后顺序"] == ["外点", "内点"]
    # 进入时间落在外点观测与内点观测之间，归属 OBS-1 -> OBS-2 段
    assert detail["segment"] == ["OBS-1", "OBS-2"]
    assert detail["segment_start"] <= detail["entry_time"] <= detail["segment_end"]
    assert T_OUT <= detail["entry_time"] <= T_IN
    assert detail["in_joint_window"] is True
    assert detail["source"]["kind"] == "frozen_quality_report"
    assert detail["frozen"] is True


def test_full_joint_shift_flow_winner_segment_and_count(svc):
    # 旧班登记待复核经过并打开候选
    reg = svc.register_candidate(team=OLD, batch_key="BATCH-JOINT-001",
                                 entry_time=T0 + 90, window=WINDOW)
    assert reg["ok"] is True
    cid = reg.entry["id"]
    assert reg.entry["status"] == "待复核"
    opened = svc.open_candidate(cid, team=OLD)
    assert opened.entry["status"] == "复核中"

    # 接班人完成时序复查
    review = svc.complete_timing_review(cid, team=SUCC, report=frozen_report(), window=WINDOW)
    assert review["ok"] is True
    assert review["segment"] == ["OBS-1", "OBS-2"]
    assert review.entry["timing_review"]["entry_time_in_segment"] is True

    # 接班人提交正式处置 -> 实际领到命中的是接班人班组
    first = svc.submit_disposal(cid, team=SUCC, report=frozen_report())
    assert first["ok"] is True
    assert first["created"] is True
    order = first.order
    ekey = order["entry_key"]
    assert str(order["claimed_by"]) == SUCC
    assert order["outer_obs_id"] == "OBS-1"
    assert order["inner_obs_id"] == "OBS-2"
    assert order["先后顺序"] == ["外点", "内点"]

    # 旧班重复/冲突提交：不新增处置单
    second = svc.submit_disposal(cid, team=OLD, report=frozen_report())
    assert second["ok"] is False
    assert second.get("already_claimed") is True
    assert len(svc.list_disposal_orders(entry_key=ekey)) == 1

    # 接班人重复提交：幂等返回原单，派单数量仍为 1
    again = svc.submit_disposal(cid, team=SUCC, report=frozen_report())
    assert again["ok"] is True
    assert again.get("duplicate") is True
    assert svc.count_disposal_orders(ekey) == 1

    # 跨班越权领取被拦截
    denied = svc.claim_disposal_order(order["id"], team=OLD)
    assert denied["ok"] is False
    assert denied.get("code") == "CROSS_TEAM_FORBIDDEN"

    # 只读复查入口仍可查看证据
    evidence = svc.review_order(order["id"], team=OLD)
    assert evidence["allowed"] is True
    assert evidence["readonly"] is True
    assert evidence["candidate"]["候选编号"] == reg.entry["候选编号"]
    assert evidence["正式命中出处"] is not None
    assert evidence["hit_detail"]["segment"] == ["OBS-1", "OBS-2"]

    # 共同处置时段由两个班组签字确认
    s1 = svc.sign_joint_window(order["id"], team=OLD)
    assert s1["joint_signed"] is False
    s2 = svc.sign_joint_window(order["id"], team=SUCC)
    assert s2["joint_signed"] is True
    stored = svc.get_disposal_order(order["id"])
    assert stored["joint_signed"] is True
    assert len(stored["signed_teams"]) == 2

    # 原始待复核记录与正式命中出处都能查对
    trace = svc.trace_entry(ekey)
    assert trace["candidate"] is not None
    assert trace["order"] is not None
    assert trace["hit_detail"] is not None
    assert trace["正式命中出处"]["kind"] == "frozen_quality_report"


def test_cross_team_claim_raises_permission_error_in_raises_block(svc):
    reg = svc.register_candidate(team=OLD, batch_key="B-X")
    oid = svc.submit_disposal(reg.entry["id"], team=SUCC).order["id"]
    with pytest.raises(PermissionError):
        svc.claim_disposal_order(oid, team=OLD)
    # 只读复查不受影响
    assert svc.review_order(oid, team=OLD)["allowed"] is True


def test_concurrent_submissions_create_exactly_one_order(svc):
    report = frozen_report()
    ekey = f"ENTRY-CONCURRENCY-{uuid.uuid4().hex[:8]}"

    def submit(team):
        return svc.submit_disposal(entry_key=ekey, team=team, report=report)

    teams = [f"班组{i}" for i in range(8)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, teams))

    created = [r for r in results if r.get("created")]
    assert len(created) == 1
    assert svc.count_disposal_orders(ekey) == 1
    orders = svc.list_disposal_orders(entry_key=ekey)
    winner = orders[0]["claimed_by"]
    assert any(str(winner) == t for t in teams)
    # 只有一个成功领取
    assert sum(1 for r in results if r.get("claimed") and r["ok"]) == 1


def test_service_aliases_and_import_paths():
    assert DisposalOrderService().MODULE == "disposal_order"
    assert FenceDispatchService().MODULE == "fence_dispatch"
    from app.services.disposal_order import DisposalOrderService as DOS2
    from app.services.geofence_dispatch import GeofenceDispatchService
    assert DOS2 is DisposalOrderService
    assert isinstance(GeofenceDispatchService(), GeofenceRuleService)


def test_tuple_unpack_style(svc):
    reg_entry, msg = svc.register_candidate(team=OLD, batch_key="B-TUP")
    assert reg_entry["id"] >= 1
    order, msg2 = svc.submit_disposal(reg_entry["id"], team=SUCC)
    assert order["order_id"].startswith("DISP-")
    denied, msg3 = svc.claim_disposal_order(order["id"], team=OLD)
    assert denied is None
    assert "越权" in msg3 or "无权" in msg3
