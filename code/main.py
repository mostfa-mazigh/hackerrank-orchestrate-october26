#!/usr/bin/env python3
"""
Ghost Data Still Exists — deletion audit processor.

This script:
- reads participant-facing data from dataset/
- resolves current + historical identities
- determines whether a request is a real deletion request
- inspects the activity stream for remaining data or restore risks
- writes the required output.csv file at the repo root

It follows the challenge contract in AGENTS.md and problem_statement.md:
- one output row per request
- case_id,deletion_status,steps_json
- sorted by case_id
- deterministic JSON/CSV output
"""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "dataset"
OUTPUT_CSV = ROOT / "output.csv"


def normalize(value) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return [{k: (v or "") for k, v in row.items()} for row in reader]


def add_aliases(mapping: Dict[str, Set[str]], *values: str) -> None:
    cleaned = {normalize(v) for v in values if normalize(v)}
    for value in cleaned:
        mapping.setdefault(value, set()).add(value)
        for other in cleaned - {value}:
            mapping.setdefault(value, set()).add(other)
            mapping.setdefault(other, set()).add(value)


def build_identity_map(identity_rows: List[Dict[str, str]], audit_rows: List[Dict[str, str]]) -> Dict[str, Set[str]]:
    """
    Build an alias graph for a user across:
    - current snapshot user_id/email/phone/etc.
    - historical email change events
    - other identifier changes from audit_log
    """
    aliases: Dict[str, Set[str]] = defaultdict(set)

    # Current snapshot IDs
    for row in identity_rows:
        for key in ["user_id", "account_id", "customer_id", "internal_user_id", "email", "user_email", "email_address", "phone", "phone_number"]:
            val = normalize(row.get(key, ""))
            if val:
                aliases.setdefault(val, set()).add(val)

        email_values = [
            normalize(row.get("email", "")),
            normalize(row.get("user_email", "")),
            normalize(row.get("email_address", "")),
            normalize(row.get("primary_email", "")),
            normalize(row.get("previous_email", "")),
            normalize(row.get("old_email", "")),
            normalize(row.get("new_email", "")),
            normalize(row.get("current_email", "")),
        ]
        email_values = [v for v in email_values if v]
        if email_values:
            for email in email_values:
                aliases.setdefault(email, set()).add(email)
            base = email_values[0]
            for email in email_values[1:]:
                aliases.setdefault(base, set()).add(email)
                aliases.setdefault(email, set()).add(base)

        user_id_values = [
            normalize(row.get("user_id", "")),
            normalize(row.get("account_id", "")),
            normalize(row.get("customer_id", "")),
            normalize(row.get("internal_user_id", "")),
        ]
        user_id_values = [v for v in user_id_values if v]
        if user_id_values:
            anchor = user_id_values[0]
            for user_id in user_id_values[1:]:
                aliases.setdefault(anchor, set()).add(user_id)
                aliases.setdefault(user_id, set()).add(anchor)

    # Historical identity changes
    for row in audit_rows:
        old_email = normalize(row.get("old_email", "") or row.get("previous_email", "") or row.get("from_email", ""))
        new_email = normalize(row.get("new_email", "") or row.get("current_email", "") or row.get("to_email", ""))
        old_user_id = normalize(row.get("old_user_id", "") or row.get("previous_user_id", "") or row.get("from_user_id", ""))
        new_user_id = normalize(row.get("new_user_id", "") or row.get("current_user_id", "") or row.get("to_user_id", ""))

        add_aliases(aliases, old_email, new_email, old_user_id, new_user_id)

    # Union-find style closure
    changed = True
    while changed:
        changed = False
        for key in list(aliases.keys()):
            before = len(aliases[key])
            for member in list(aliases[key]):
                aliases[key].update(aliases.get(member, set()))
            if len(aliases[key]) != before:
                changed = True

    return {k: set(v) for k, v in aliases.items()}


def field(row: Dict[str, str], *names: str) -> str:
    for name in names:
        value = row.get(name, "")
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def is_real_deletion_request(request_text: str) -> bool:
    """
    Distinguish actual deletion requests from access/export/marketing/other requests.
    """
    text = normalize(request_text)
    if not text:
        return False

    positive_patterns = [
        "delete",
        "deletion",
        "erase",
        "erasure",
        "remove my account",
        "close my account",
        "delete my account",
        "delete this account",
        "delete all my data",
        "delete my personal data",
        "permanently delete",
        "remove account",
        "delete account",
        "account deletion",
        "request to delete",
        "delete my profile",
        "remove my profile",
        "deactivate and delete",
        "delete my information",
        "delete my records",
        "please delete",
    ]

    negative_patterns = [
        "access my data",
        "download my data",
        "export my data",
        "correct my data",
        "update my data",
        "opt out",
        "marketing preference",
        "unsubscribe",
        "retrieve my records",
        "view my data",
    ]

    if any(p in text for p in negative_patterns):
        return False

    return any(p in text for p in positive_patterns)


def build_activity_index(activity_rows: List[Dict[str, str]]) -> Tuple[Dict[str, List[Dict[str, str]]], Dict[str, List[Dict[str, str]]]]:
    by_case: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    by_identity: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for row in activity_rows:
        case_id = normalize(field(row, "case_id", "request_case_id", "deletion_case_id"))
        email = normalize(field(row, "email", "user_email", "customer_email", "account_email"))
        user_id = normalize(field(row, "user_id", "account_id", "customer_id", "internal_user_id"))

        if case_id:
            by_case[case_id].append(row)
        if email:
            by_identity[email].append(row)
        if user_id:
            by_identity[user_id].append(row)

    return by_case, by_identity


def relevant_activity_for_case(
    case_id: str,
    email: str,
    identities: Dict[str, Set[str]],
    activity_by_case: Dict[str, List[Dict[str, str]]],
    activity_by_identity: Dict[str, List[Dict[str, str]]],
) -> List[Dict[str, str]]:
    tokens: Set[str] = set()

    if case_id:
        tokens.add(normalize(case_id))
    if email:
        tokens.add(normalize(email))

    if email:
        for alias in identities.get(normalize(email), set()):
            tokens.add(alias)

    candidate_rows: List[Dict[str, str]] = []
    seen = set()

    for token in sorted(tokens):
        for row in activity_by_case.get(token, []):
            key = (
                normalize(field(row, "case_id", "request_case_id")),
                normalize(field(row, "email", "user_email", "customer_email")),
                normalize(field(row, "user_id", "account_id", "customer_id")),
                normalize(field(row, "event", "action", "operation", "type")),
                normalize(field(row, "status", "state")),
            )
            if key not in seen:
                seen.add(key)
                candidate_rows.append(row)

        for row in activity_by_identity.get(token, []):
            key = (
                normalize(field(row, "case_id", "request_case_id")),
                normalize(field(row, "email", "user_email", "customer_email")),
                normalize(field(row, "user_id", "account_id", "customer_id")),
                normalize(field(row, "event", "action", "operation", "type")),
                normalize(field(row, "status", "state")),
            )
            if key not in seen:
                seen.add(key)
                candidate_rows.append(row)

    return candidate_rows


def has_remaining_record(case_row: Dict[str, str], identities: Dict[str, Set[str]], activity_rows: List[Dict[str, str]]) -> bool:
    case_id = normalize(field(case_row, "case_id", "request_id"))
    email = normalize(field(case_row, "email", "user_email", "customer_email"))
    request_status = normalize(field(case_row, "reported_status", "status"))

    for row in activity_rows:
        row_case = normalize(field(row, "case_id", "request_case_id"))
        row_email = normalize(field(row, "email", "user_email", "customer_email", "account_email"))
        row_user = normalize(field(row, "user_id", "account_id", "customer_id", "internal_user_id"))
        event = normalize(field(row, "event", "action", "operation", "type"))
        status = normalize(field(row, "status", "state", "result"))

        if case_id and row_case and row_case == case_id:
            # directly associated with this case
            return True

        if email and row_email and row_email == email:
            return True

        if email and row_email in identities.get(email, set()):
            return True

        if row_user and row_user in identities.get(email, set(), set()):
            return True

        # if the activity shows a live/active operation, treat it as evidence of residual risk
        if any(k in event for k in ["create", "sync", "restore", "reindex", "export", "backup", "update", "login", "active"]):
            if "complete" not in status and "success" not in status:
                return True

        if any(k in status for k in ["active", "open", "present", "live", "pending", "incomplete"]):
            return True

    # report_status itself is not proof, but if it is not complete it often indicates work remains
    if request_status and "complete" not in request_status:
        return True

    return False


def make_steps(operation: str, state: str, phase: str, step_no: int, resource_id: str, evidence_id: str = "") -> Dict[str, str]:
    return {
        "step_no": step_no,
        "phase": phase,
        "system_id": "account_service",
        "resource_id": resource_id,
        "operation": operation,
        "state": state,
        "evidence_id": evidence_id,
    }


def classify_case(case_row: Dict[str, str], identities: Dict[str, Set[str]], activity_rows: List[Dict[str, str]]) -> Dict[str, str]:
    case_id = field(case_row, "case_id", "request_id", "deletion_case_id")
    email = field(case_row, "email", "user_email", "customer_email", "account_email")
    request_text = field(case_row, "request_text", "description", "message", "request", "notes")
    reported_status = normalize(field(case_row, "reported_status", "status", "state"))

    if not is_real_deletion_request(request_text):
        return {
            "case_id": case_id,
            "deletion_status": "NOT_APPLICABLE",
            "steps_json": "[]",
        }

    # If activity shows any remaining record or active data path, the case is incomplete.
    remaining = has_remaining_record(case_row, identities, activity_rows)
    if remaining:
        steps = [
            make_steps("REPORT_REMAINING_RECORD", "PRESENT", "FINDING", 1, f"account_{case_id.replace('case_', '') or 'unknown'}", ""),
            make_steps("DISABLE_DATA_FLOW", "DISABLED", "REMEDIATION", 2, f"account_{case_id.replace('case_', '') or 'unknown'}", ""),
            make_steps("DELETE_RECORD", "ABSENT", "REMEDIATION", 3, f"account_{case_id.replace('case_', '') or 'unknown'}", ""),
            make_steps("VERIFY_RECORD_ABSENT", "ABSENT", "VERIFICATION", 4, f"account_{case_id.replace('case_', '') or 'unknown'}", ""),
        ]
        return {
            "case_id": case_id,
            "deletion_status": "INCOMPLETE",
            "steps_json": json.dumps(steps, separators=(",", ":")),
        }

    # If no remaining activity and there is no meaningful risk, classify as COMPLETE
    steps = [
        make_steps("VERIFY_RECORD_ABSENT", "ABSENT", "VERIFICATION", 1, f"account_{case_id.replace('case_', '') or 'unknown'}", "")
    ]
    return {
        "case_id": case_id,
        "deletion_status": "COMPLETE",
        "steps_json": json.dumps(steps, separators=(",", ":")),
    }


def main() -> None:
    # Load required input files
    identity_rows = read_csv(DATASET / "user_identities.csv")
    audit_rows = read_csv(DATASET / "user_audit_log.csv")
    request_rows = read_csv(DATASET / "deletion_requests.csv")
    activity_rows = read_csv(DATASET / "system_activity.csv")

    # Robust fallback for alternate dataset names
    if not request_rows and (DATASET / "sample_deletion_requests.csv").exists():
        request_rows = read_csv(DATASET / "sample_deletion_requests.csv")

    identities = build_identity_map(identity_rows, audit_rows)

    results = []
    for request_row in request_rows:
        result = classify_case(request_row, identities, activity_rows)
        results.append(result)

    results.sort(key=lambda row: row["case_id"])

    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["case_id", "deletion_status", "steps_json"])
        for row in results:
            writer.writerow([row["case_id"], row["deletion_status"], row["steps_json"]])

    print(f"Wrote {len(results)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
