"""The Power BI write-back user data function, run with a stub ``fabric.functions``."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import ROOT

FUNCTION_APP = ROOT / "fabric" / "user_data_functions" / "review_writeback" / "function_app.py"


class UserThrownError(Exception):
    def __init__(self, message: str, properties: dict[str, Any]) -> None:
        super().__init__(message)
        self.properties = properties


class _UDF:
    def connection(self, **_: Any):  # type: ignore[no-untyped-def]
        return lambda f: f

    def context(self, **_: Any):  # type: ignore[no-untyped-def]
        return lambda f: f

    def function(self):  # type: ignore[no-untyped-def]
        return lambda f: f


@pytest.fixture(scope="module")
def app():  # type: ignore[no-untyped-def]
    fabric = types.ModuleType("fabric")
    functions = types.ModuleType("fabric.functions")
    functions.UserDataFunctions = _UDF  # type: ignore[attr-defined]
    functions.UserThrownError = UserThrownError  # type: ignore[attr-defined]
    functions.FabricSqlConnection = object  # type: ignore[attr-defined]
    functions.UserDataFunctionContext = object  # type: ignore[attr-defined]
    fabric.functions = functions  # type: ignore[attr-defined]
    sys.modules["fabric"], sys.modules["fabric.functions"] = fabric, functions
    spec = importlib.util.spec_from_file_location("function_app", FUNCTION_APP)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module
    for name in ("fabric", "fabric.functions"):
        sys.modules.pop(name, None)


class FakeDb:
    """Tiny in-memory stand-in for the review schema, answering the function's SQL."""

    def __init__(self, stewards: dict[str, str], queue: dict[str, str]) -> None:
        self.stewards, self.queue = stewards, queue
        self.log: list[tuple[Any, ...]] = []
        self.approvals: list[tuple[Any, ...]] = []
        self.commits = self.rollbacks = 0
        self._pending: list[tuple[str, tuple[Any, ...]]] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        pass


class FakeCursor:
    def __init__(self, db: FakeDb) -> None:
        self.db, self.rowcount, self._result = db, -1, None

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        db = self.db
        if sql.startswith("SELECT role"):
            role = db.stewards.get(params[0])
            self._result = (role,) if role else None
        elif sql.startswith("SELECT status"):
            status = db.queue.get(params[0])
            self._result = (status, "MATCH_REVIEW", 0.62, "LOW") if status else None
        elif sql.startswith("UPDATE"):
            new_status, _, pair = params
            if db.queue.get(pair) in {"PENDING", "HELD"}:
                db.queue[pair] = new_status
                self.rowcount = 1
            else:
                self.rowcount = 0
        elif sql.startswith("INSERT INTO review.decision_log"):
            db.log.append(params)
            self._result = (len(db.log),)
        elif sql.startswith("INSERT INTO review.approval"):
            db.approvals.append(params)
        else:  # pragma: no cover
            raise AssertionError(sql)

    def fetchone(self):  # type: ignore[no-untyped-def]
        return self._result

    def close(self) -> None:
        pass


def _db() -> FakeDb:
    return FakeDb({"steward@x.com": "steward", "viewer@x.com": "viewer"}, {"p1": "PENDING", "p2": "DECIDED"})


def test_validation(app) -> None:  # type: ignore[no-untyped-def]
    assert app.validate_request(" p1 ", "approve", "") == ("p1", "APPROVE", "")
    with pytest.raises(UserThrownError, match="APPROVE, REJECT or HOLD"):
        app.validate_request("p1", "maybe", "")
    with pytest.raises(UserThrownError, match="reason"):
        app.validate_request("p1", "REJECT", "no")
    with pytest.raises(UserThrownError, match="Select a pair"):
        app.validate_request("", "APPROVE", "")


def test_approve_writes_log_and_approval(app) -> None:  # type: ignore[no-untyped-def]
    db = _db()
    message = app.record_decision(db, "steward@x.com", None, "inv-1", "p1", "APPROVE", "")
    assert "approved" in message
    assert db.queue["p1"] == "DECIDED" and db.commits == 1
    assert db.log[0][:2] == ("p1", "APPROVE") and db.approvals[0][3] == "DECIDED"


def test_hold_keeps_item_open(app) -> None:  # type: ignore[no-untyped-def]
    db = _db()
    app.record_decision(db, "steward@x.com", None, None, "p1", "HOLD", "need more info")
    assert db.queue["p1"] == "HELD"


def test_non_steward_is_refused(app) -> None:  # type: ignore[no-untyped-def]
    db = _db()
    with pytest.raises(UserThrownError, match="not registered"):
        app.record_decision(db, "viewer@x.com", None, None, "p1", "APPROVE", "")
    assert db.log == [] and db.rollbacks == 1


def test_second_click_loses(app) -> None:  # type: ignore[no-untyped-def]
    db = _db()
    with pytest.raises(UserThrownError, match="already decided"):
        app.record_decision(db, "steward@x.com", None, None, "p2", "APPROVE", "")
    assert db.log == [] and db.commits == 0


def test_entry_point_uses_signed_in_user(app) -> None:  # type: ignore[no-untyped-def]
    db = _db()

    class Conn:
        def connect(self) -> FakeDb:
            return db

    ctx = types.SimpleNamespace(
        executing_user={"PreferredUsername": "Steward@X.com", "Oid": "x"}, invocation_id="i"
    )
    assert "approved" in app.submit_decision(Conn(), ctx, "p1", "approve")
    assert db.log[0][3] == "steward@x.com"


def test_function_file_follows_fabric_rules() -> None:
    text = Path(FUNCTION_APP).read_text(encoding="utf-8")
    assert "import fabric.functions as fn" in text and "udf = fn.UserDataFunctions()" in text
    assert "def submit_decision(" in text and ") -> str:" in text
