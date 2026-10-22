"""Fabric User Data Functions: steward write-back from the Power BI review page.

A Power BI button (translytical task flow) calls ``submit_decision`` with the selected
pair, the decision and a reason. In one transaction the function:

1. checks the caller is an active steward (``review.steward``),
2. moves the queue item out of PENDING with a *conditional* UPDATE, so if two stewards
   click at the same time only the first one wins,
3. appends the decision to ``review.decision_log`` and an audit row to ``review.approval``.

The decision log is mirrored to OneLake; the next pipeline run applies it to Gold.

Fabric rules followed here: parameters are camelCase and type-annotated, the function
returns ``str`` (required for Power BI buttons), and errors meant for the user are raised
as ``fn.UserThrownError``. Report users need the *Execute* permission on this item.
"""

from __future__ import annotations

import logging
from typing import Any

import fabric.functions as fn

udf = fn.UserDataFunctions()

DECISIONS = frozenset({"APPROVE", "REJECT", "HOLD"})
DECIDING_ROLES = frozenset({"steward", "lead"})
MIN_REASON_LENGTH = 5
MAX_REASON_LENGTH = 500

SQL_STEWARD = "SELECT role FROM review.steward WHERE upn = ? AND is_active = 1"
SQL_ITEM = "SELECT status, review_type, confidence, band FROM review.review_queue WHERE pair_id = ?"
SQL_CLAIM = (
    "UPDATE review.review_queue SET status = ?, decided_by = ?, decided_at = SYSUTCDATETIME(), "
    "updated_at = SYSUTCDATETIME() WHERE pair_id = ? AND status IN ('PENDING', 'HELD')"
)
SQL_LOG = (
    "INSERT INTO review.decision_log (pair_id, decision, reason, decided_by, decided_by_oid, invocation_id) "
    "OUTPUT INSERTED.decision_id VALUES (?, ?, ?, ?, TRY_CONVERT(UNIQUEIDENTIFIER, ?), ?)"
)
SQL_APPROVAL = (
    "INSERT INTO review.approval (decision_id, pair_id, previous_status, new_status, approver_upn, "
    "approver_role, confidence, band) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)


def validate_request(pair_id: str, decision: str, reason: str) -> tuple[str, str, str]:
    """Normalise and validate the inputs; raise UserThrownError with a readable message."""
    pair = (pair_id or "").strip()
    choice = (decision or "").strip().upper()
    text = (reason or "").strip()
    if not pair:
        raise fn.UserThrownError("Select a pair to review first.", {"pairId": pair_id})
    if choice not in DECISIONS:
        raise fn.UserThrownError("Decision must be APPROVE, REJECT or HOLD.", {"decision": decision})
    if choice in {"REJECT", "HOLD"} and len(text) < MIN_REASON_LENGTH:
        raise fn.UserThrownError("Please give a short reason for a reject or hold.", {"decision": choice})
    return pair, choice, text[:MAX_REASON_LENGTH]


def record_decision(
    connection: Any,
    upn: str,
    oid: str | None,
    invocation_id: str | None,
    pair_id: str,
    decision: str,
    reason: str,
) -> str:
    """Run the checks and writes in one transaction. ``connection`` is a DB-API connection."""
    cursor = connection.cursor()
    try:
        cursor.execute(SQL_STEWARD, (upn,))
        row = cursor.fetchone()
        if row is None or row[0] not in DECIDING_ROLES:
            raise fn.UserThrownError("You are not registered as a data steward.", {"user": upn})
        role = row[0]

        cursor.execute(SQL_ITEM, (pair_id,))
        item = cursor.fetchone()
        if item is None:
            raise fn.UserThrownError("This pair is no longer in the review queue.", {"pairId": pair_id})
        previous_status, _review_type, confidence, band = item

        new_status = "HELD" if decision == "HOLD" else "DECIDED"
        cursor.execute(SQL_CLAIM, (new_status, upn, pair_id))
        if cursor.rowcount != 1:
            connection.rollback()
            raise fn.UserThrownError("Another steward has already decided this pair.", {"pairId": pair_id})

        cursor.execute(SQL_LOG, (pair_id, decision, reason or None, upn, oid, invocation_id))
        decision_id = cursor.fetchone()[0]
        cursor.execute(
            SQL_APPROVAL, (decision_id, pair_id, previous_status, new_status, upn, role, confidence, band)
        )
        connection.commit()
    except fn.UserThrownError:
        connection.rollback()
        raise
    except Exception:
        connection.rollback()
        logging.exception("submit_decision failed for pair %s", pair_id)
        raise
    finally:
        cursor.close()
    verb = {"APPROVE": "approved", "REJECT": "rejected", "HOLD": "put on hold"}[decision]
    return f"Pair {pair_id} {verb}. It will be applied to the golden record in the next run."


@udf.connection(argName="reviewDb", alias="ReviewDB")
@udf.context(argName="invocationContext")
@udf.function()
def submit_decision(
    reviewDb: fn.FabricSqlConnection,
    invocationContext: fn.UserDataFunctionContext,
    pairId: str,
    decision: str,
    reason: str = "",
) -> str:
    """Record a steward decision for one review queue item (called from Power BI)."""
    pair, choice, text = validate_request(pairId, decision, reason)
    user = invocationContext.executing_user or {}
    upn = str(user.get("PreferredUsername") or "").lower()
    if not upn:
        raise fn.UserThrownError("Could not identify the signed-in user.", {})
    connection = reviewDb.connect()
    try:
        return record_decision(
            connection, upn, user.get("Oid"), invocationContext.invocation_id, pair, choice, text
        )
    finally:
        connection.close()
