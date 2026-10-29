#!/usr/bin/env python3
"""List every current Kiro subscriber's month-to-date Credit usage.

The command uses only Python's standard library and AWS CLI v2. It never writes
business reports, invokes Lambda, reads Secrets Manager, or sends notifications.
Athena itself stores normal query-result files at its configured WorkGroup location.
"""
from __future__ import annotations

import argparse
import ast
import csv
import io
import json
import os
import re
import subprocess
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, TextIO

ROOT = Path(__file__).resolve().parents[1]
SHANGHAI = timezone(timedelta(hours=8), name="Asia/Shanghai")
IDENTITY_MAPPING_KEY = "user-mapping/user_mapping.csv"
TIERS = {
    "Unknown": {"rank": 0, "capacity": 0, "price": 0},
    "Pro": {"rank": 1, "capacity": 1000, "price": 20},
    "Pro+": {"rank": 2, "capacity": 2000, "price": 40},
    "Pro Max": {"rank": 3, "capacity": 5000, "price": 100},
    "Power": {"rank": 4, "capacity": 10000, "price": 200},
}
ACTIVE_SUBSCRIPTION_STATUSES = {"ACTIVE", "ENABLED", "SUBSCRIBED", "PENDING"}
INACTIVE_SUBSCRIPTION_STATUSES = {
    "CANCELLED", "CANCELED", "INACTIVE", "DELETED", "DISABLED", "EXPIRED",
    "SUSPENDED", "UNASSIGNED", "UNSUBSCRIBED", "REVOKED",
}
ACCESS_DENIED_CODES = {"AccessDenied", "AccessDeniedException", "UnauthorizedOperation"}
NOT_FOUND_CODES = {"NoSuchKey", "NotFound", "ResourceNotFoundException", "404"}
AWS_ERROR_CODE_RE = re.compile(r"\(([A-Za-z0-9][A-Za-z0-9._-]+)\)")
FORMULA_PREFIXES = ("=", "+", "-", "@")
CONTROL_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
STATUS_ORDER = {"zero": 0, "low": 1, "medium": 2, "normal": 3, "unknown": 4}
STATUS_LABELS = {
    "zero": "零使用", "low": "低使用", "medium": "中使用",
    "normal": "正常", "unknown": "套餐未知",
}
STATUS_COLORS = {
    "zero": "\033[31m", "low": "\033[31m", "medium": "\033[33m",
    "normal": "\033[32m", "unknown": "\033[90m",
}
RESET = "\033[0m"
OUTPUT_FIELDS = [
    "user_name", "email", "tier", "credits", "capacity", "usage_percent",
    "last_active", "active_days", "status", "monthly_cost_usd", "tier_source",
]
TABLE_COLUMNS = [
    ("用户", "user_name"), ("邮箱", "email"), ("套餐", "tier"),
    ("Credits/容量", "credits_capacity"), ("使用率", "usage_display"),
    ("最后活跃", "last_active"), ("活跃天数", "active_days"),
    ("状态", "status_label"), ("月成本", "cost_display"),
    ("套餐来源", "tier_source_label"),
]
TIER_SOURCE_LABELS = {
    "current_authoritative_snapshot": "权威订阅 CSV",
    "current_iic_group": "当前 IIC 套餐组",
    "latest_at_data_cutoff": "截止日用量记录",
    "unknown_direct_assignment": "IIC 直接分配（套餐未知）",
}
_CONFIG_KEYS = {
    "aws": {"region", "account_id", "profile"},
    "s3": {"bucket_name"},
    "glue": {"database_name"},
    "identity_center": {"identity_store_id"},
    "monthly_report": {
        "kiro_application_arn", "subscription_csv_key", "athena_workgroup",
        "identity_mapping_key",
    },
}
CSV_ALIASES = {
    "userid": ("userid", "user_id"),
    "user_name": ("user_name", "username", "name"),
    "email": ("email",),
    "subscription_status": ("subscription_status", "status"),
    "subscription_tier": ("subscription_tier", "kiro_plan", "plan"),
    "activation_date": ("activation_date",),
    "plan_source": ("plan_source", "source"),
}


class AwsCliError(RuntimeError):
    """A redacted AWS CLI failure suitable for terminal display."""

    def __init__(self, operation: str, code: str = "UnknownError"):
        self.operation = operation
        self.code = code
        super().__init__(f"AWS CLI {operation} failed ({code})")


class AwsCli:
    def __init__(self, profile: str | None, region: str, executable: str = "aws",
                 process_timeout: int = 60):
        self.base = [executable]
        self.process_timeout = process_timeout
        if profile:
            self.base.extend(["--profile", profile])
        if region:
            self.base.extend(["--region", region])

    @staticmethod
    def _error(operation: str, stderr: str) -> AwsCliError:
        match = AWS_ERROR_CODE_RE.search(stderr)
        return AwsCliError(operation, match.group(1) if match else "CommandFailed")

    def json(self, service: str, operation: str, *arguments: str) -> dict[str, Any]:
        command = [*self.base, service, operation, *arguments, "--output", "json"]
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, check=False,
                timeout=self.process_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise AwsCliError(f"{service} {operation}", "ClientTimeout") from exc
        if result.returncode:
            raise self._error(f"{service} {operation}", result.stderr)
        try:
            return json.loads(result.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise AwsCliError(f"{service} {operation}", "InvalidJsonResponse") from exc

    def s3_bytes(self, bucket: str, key: str) -> bytes:
        command = [*self.base, "s3", "cp", f"s3://{bucket}/{key}", "-", "--only-show-errors"]
        try:
            result = subprocess.run(
                command, capture_output=True, check=False, timeout=self.process_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise AwsCliError("s3 read", "ClientTimeout") from exc
        if result.returncode:
            stderr = result.stderr.decode("utf-8", errors="replace")
            raise self._error("s3 read", stderr)
        return result.stdout


def sanitize(value: Any) -> str:
    if value is None:
        return ""
    text = CONTROL_CHARS.sub("", str(value)).replace("\r", " ").replace("\n", " ").strip()
    return "'" + text if text.startswith(FORMULA_PREFIXES) else text


def canonical_user_id(value: Any) -> str:
    uid = sanitize(value)
    if uid.startswith("d-") and "." in uid:
        uid = uid.split(".", 1)[1]
    return uid.strip()


def decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def normalize_tier(value: Any) -> str:
    raw = sanitize(value).lower()
    compact = re.sub(r"[^a-z0-9+]", "", raw)
    if not compact:
        return "Unknown"
    if "power" in compact:
        return "Power"
    if "promax" in compact or compact in {"max", "kiropromaxusers"}:
        return "Pro Max"
    if "proplus" in compact or "pro+" in raw or compact in {"plus", "kiroprousers+", "kiroproplususers"}:
        return "Pro+"
    if compact in {"pro", "kiroprousers"} or compact.startswith("pro"):
        return "Pro"
    return "Unknown"


def highest_tier(values: Iterable[Any]) -> str:
    return max((normalize_tier(value) for value in values), default="Unknown", key=lambda tier: TIERS[tier]["rank"])


def month_bounds(month: str) -> tuple[date, date]:
    start = datetime.strptime(month, "%Y-%m").date().replace(day=1)
    return start, (start.replace(day=28) + timedelta(days=4)).replace(day=1)


def _tier_sql() -> str:
    raw = "upper(COALESCE(subscription_tier, ''))"
    compact = f"regexp_replace({raw}, '[^A-Z0-9]', '')"
    return (f"CASE WHEN {compact} LIKE '%POWER%' THEN 'Power' "
            f"WHEN {compact} LIKE '%PROMAX%' THEN 'Pro Max' "
            f"WHEN {raw} LIKE '%PRO+%' OR {compact} LIKE '%PROPLUS%' THEN 'Pro+' "
            f"WHEN {compact} LIKE '%PRO%' THEN 'Pro' ELSE 'Unknown' END")


def build_data_cutoff_sql(database: str, month: str) -> str:
    start, end = month_bounds(month)
    return f"""
SELECT CAST(MAX(TRY(date_parse(date, '%Y-%m-%d'))) AS VARCHAR) AS data_cutoff
FROM \"{database}\".\"user_report\"
WHERE TRY(date_parse(date, '%Y-%m-%d')) >= TIMESTAMP '{start.isoformat()} 00:00:00'
  AND TRY(date_parse(date, '%Y-%m-%d')) < TIMESTAMP '{end.isoformat()} 00:00:00'
""".strip()


def build_usage_sql(database: str, month: str, data_cutoff: date) -> str:
    start, month_end = month_bounds(month)
    end = min(month_end, data_cutoff + timedelta(days=1))
    tier_sql = _tier_sql()
    rank_sql = "CASE tier_label WHEN 'Power' THEN 4 WHEN 'Pro Max' THEN 3 WHEN 'Pro+' THEN 2 WHEN 'Pro' THEN 1 ELSE 0 END"
    return f"""
WITH typed AS (
 SELECT CASE WHEN strpos(userid, '.') > 0 THEN split_part(userid, '.', 2) ELSE userid END AS user_id,
        TRY(date_parse(date, '%Y-%m-%d')) AS usage_date,
        TRY_CAST(credits_used AS DECIMAL(18,4)) AS credits,
        TRY_CAST(overage_credits_used AS DECIMAL(18,4)) AS overage,
        {tier_sql} AS tier_label
 FROM \"{database}\".\"user_report\"
 WHERE TRY(date_parse(date, '%Y-%m-%d')) >= TIMESTAMP '{start.isoformat()} 00:00:00'
   AND TRY(date_parse(date, '%Y-%m-%d')) < TIMESTAMP '{end.isoformat()} 00:00:00'
), ranked AS (
 SELECT *, {rank_sql} AS tier_rank FROM typed
 WHERE user_id IS NOT NULL AND user_id <> '' AND usage_date IS NOT NULL
), by_day AS (
 SELECT user_id, usage_date, SUM(COALESCE(credits, 0)) credits,
        SUM(COALESCE(overage, 0)) overage,
        CASE MAX(tier_rank) WHEN 4 THEN 'Power' WHEN 3 THEN 'Pro Max'
             WHEN 2 THEN 'Pro+' WHEN 1 THEN 'Pro' ELSE 'Unknown' END AS day_tier
 FROM ranked GROUP BY user_id, usage_date
), history AS (
 SELECT user_id, array_join(array_sort(array_distinct(array_agg(tier_label))), ' -> ') tier_history
 FROM ranked GROUP BY user_id
)
SELECT d.user_id, CAST(SUM(d.credits) AS VARCHAR) credits,
       CAST(SUM(d.overage) AS VARCHAR) overage,
       CAST(MIN(d.usage_date) AS VARCHAR) first_active,
       CAST(MAX(d.usage_date) AS VARCHAR) last_active,
       CAST(COUNT(DISTINCT d.usage_date) AS VARCHAR) active_days,
       h.tier_history, max_by(d.day_tier, d.usage_date) latest_tier
FROM by_day d JOIN history h ON d.user_id = h.user_id
GROUP BY d.user_id, h.tier_history ORDER BY d.user_id
""".strip()


def build_latest_tiers_sql(database: str, data_cutoff: date) -> str:
    end = data_cutoff + timedelta(days=1)
    tier_sql = _tier_sql()
    rank_sql = "CASE tier_label WHEN 'Power' THEN 4 WHEN 'Pro Max' THEN 3 WHEN 'Pro+' THEN 2 WHEN 'Pro' THEN 1 ELSE 0 END"
    return f"""
WITH typed AS (
 SELECT CASE WHEN strpos(userid, '.') > 0 THEN split_part(userid, '.', 2) ELSE userid END AS user_id,
        TRY(date_parse(date, '%Y-%m-%d')) AS usage_date,
        {tier_sql} AS tier_label
 FROM \"{database}\".\"user_report\"
 WHERE TRY(date_parse(date, '%Y-%m-%d')) < TIMESTAMP '{end.isoformat()} 00:00:00'
), ranked AS (
 SELECT *, {rank_sql} AS tier_rank FROM typed
 WHERE user_id IS NOT NULL AND user_id <> '' AND usage_date IS NOT NULL
), by_day AS (
 SELECT user_id, usage_date,
        CASE MAX(tier_rank) WHEN 4 THEN 'Power' WHEN 3 THEN 'Pro Max'
             WHEN 2 THEN 'Pro+' WHEN 1 THEN 'Pro' ELSE 'Unknown' END AS day_tier
 FROM ranked GROUP BY user_id, usage_date
)
SELECT user_id, max_by(day_tier, usage_date) AS latest_tier
FROM by_day GROUP BY user_id ORDER BY user_id
""".strip()


def run_athena_query(aws: AwsCli, sql: str, workgroup: str, timeout_seconds: int = 720,
                     sleep: Callable[[float], None] = time.sleep) -> list[dict[str, str]]:
    query_id = aws.json(
        "athena", "start-query-execution", "--query-string", sql, "--work-group", workgroup,
    )["QueryExecutionId"]
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        execution = aws.json("athena", "get-query-execution", "--query-execution-id", query_id)
        status = execution["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            raise RuntimeError(f"Athena query {state}; inspect Athena query {query_id} for details")
        sleep(1)
    else:
        try:
            aws.json("athena", "stop-query-execution", "--query-execution-id", query_id)
        finally:
            raise TimeoutError(f"Athena query timed out after {timeout_seconds}s")

    rows: list[list[str]] = []
    next_token = ""
    while True:
        arguments = ["--query-execution-id", query_id, "--max-results", "1000", "--no-paginate"]
        if next_token:
            arguments.extend(["--next-token", next_token])
        page = aws.json("athena", "get-query-results", *arguments)
        for row in page.get("ResultSet", {}).get("Rows", []):
            rows.append([cell.get("VarCharValue", "") for cell in row.get("Data", [])])
        next_token = page.get("NextToken", "")
        if not next_token:
            break
    if not rows:
        return []
    headers = rows[0]
    return [dict(zip(headers, row + [""] * (len(headers) - len(row)))) for row in rows[1:]]


def _strip_yaml_comment(value: str) -> str:
    quote = None
    escaped = False
    result = []
    for char in value:
        if escaped:
            result.append(char)
            escaped = False
        elif char == "\\" and quote == '"':
            result.append(char)
            escaped = True
        elif char in {'"', "'"}:
            quote = None if quote == char else char if quote is None else quote
            result.append(char)
        elif char == "#" and quote is None:
            break
        else:
            result.append(char)
    return "".join(result).strip()


def _parse_yaml_scalar(value: str) -> Any:
    value = _strip_yaml_comment(value)
    if not value:
        return ""
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value[0:1] in {'"', "'"}:
        parsed = ast.literal_eval(value)
        if not isinstance(parsed, str):
            raise ValueError("quoted configuration values must be strings")
        return parsed
    return value


def load_project_config(path: Path) -> dict[str, dict[str, Any]]:
    """Read only the flat two-level project settings needed by this command."""
    if not path.is_file():
        raise FileNotFoundError(f"configuration file not found: {path}")
    result = {section: {} for section in _CONFIG_KEYS}
    section = ""
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if indent == 0 and stripped.endswith(":"):
            section = stripped[:-1].strip()
        elif indent > 0 and section in _CONFIG_KEYS and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            key = key.strip()
            if key in _CONFIG_KEYS[section]:
                try:
                    result[section][key] = _parse_yaml_scalar(raw_value)
                except (SyntaxError, ValueError) as exc:
                    raise ValueError(f"invalid {section}.{key} at {path}:{line_number}") from exc
    return result


def build_settings(config: dict[str, dict[str, Any]], region: str | None = None) -> dict[str, str]:
    settings = {
        "region": str(region or config["aws"].get("region") or "us-east-1"),
        "account_id": str(config["aws"].get("account_id") or ""),
        "profile": str(config["aws"].get("profile") or ""),
        "data_bucket": str(config["s3"].get("bucket_name") or ""),
        "database": str(config["glue"].get("database_name") or "kiro_analytics"),
        "identity_store_id": str(config["identity_center"].get("identity_store_id") or ""),
        "application_arn": str(config["monthly_report"].get("kiro_application_arn") or ""),
        "subscription_csv_key": str(config["monthly_report"].get("subscription_csv_key") or ""),
        "workgroup": str(config["monthly_report"].get("athena_workgroup") or "kiro-analytics-workgroup"),
        "identity_mapping_key": str(config["monthly_report"].get("identity_mapping_key") or IDENTITY_MAPPING_KEY),
    }
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", settings["database"]):
        raise ValueError("glue.database_name must be a valid Athena identifier")
    missing = [key for key in ("data_bucket", "identity_store_id") if not settings[key]]
    if not settings["subscription_csv_key"] and not settings["application_arn"]:
        missing.append("application_arn or subscription_csv_key")
    if missing:
        raise ValueError(f"missing required configuration: {', '.join(missing)}")
    return settings


def current_shanghai_month(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(SHANGHAI).strftime("%Y-%m")


def validate_month(value: str, current_month: str) -> str:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise ValueError("--month must be YYYY-MM")
    parsed = datetime.strptime(value, "%Y-%m")
    normalized = parsed.strftime("%Y-%m")
    if normalized > current_month:
        raise ValueError("future months are not allowed")
    return normalized


def list_aws_profiles(executable: str = "aws") -> list[str]:
    try:
        result = subprocess.run(
            [executable, "configure", "list-profiles"], capture_output=True, text=True,
            check=False, timeout=10,
        )
    except subprocess.TimeoutExpired:
        return []
    return result.stdout.splitlines() if result.returncode == 0 else []


def choose_profile(explicit: str | None, configured: str, account_id: str,
                   available_profiles: Iterable[str], environ: dict[str, str] | None = None) -> str | None:
    environ = environ if environ is not None else os.environ
    if explicit:
        return explicit
    if configured:
        return configured
    if environ.get("AWS_PROFILE"):
        return None
    profiles = set(available_profiles)
    return account_id if account_id and account_id in profiles else None


def _canonical_subscription_row(row: dict[str, Any]) -> dict[str, str]:
    lowered = {sanitize(key).lower(): sanitize(value) for key, value in row.items() if key is not None}
    result = {
        canonical: next((lowered[alias] for alias in aliases if alias in lowered), "")
        for canonical, aliases in CSV_ALIASES.items()
    }
    result["userid"] = canonical_user_id(result["userid"])
    result["subscription_tier"] = normalize_tier(result["subscription_tier"])
    result["subscription_status"] = result["subscription_status"] or "ACTIVE"
    result["plan_source"] = result["plan_source"] or "S3 CSV"
    return result


def _is_active_subscription(row: dict[str, Any]) -> bool:
    status = sanitize(row.get("subscription_status")).upper()
    if status in ACTIVE_SUBSCRIPTION_STATUSES:
        return True
    if status in INACTIVE_SUBSCRIPTION_STATUSES:
        return False
    raise ValueError(f"unsupported subscription_status: {status or 'EMPTY'}")


def parse_subscription_csv(data: bytes) -> dict[str, dict[str, str]]:
    reader = csv.DictReader(io.StringIO(data.decode("utf-8-sig", errors="strict"), newline=""))
    if reader.fieldnames is None:
        raise ValueError("subscription CSV requires a header")
    headers = {sanitize(header).lower() for header in reader.fieldnames if header}
    if not headers.intersection(CSV_ALIASES["userid"]):
        raise ValueError("subscription CSV requires userid or user_id")
    roster: dict[str, dict[str, str]] = {}
    for raw in reader:
        row = _canonical_subscription_row(raw)
        uid = row["userid"]
        if not uid:
            continue
        prior = roster.get(uid)
        if prior is None:
            roster[uid] = row
            continue
        selected, other = (
            (row, prior)
            if (_is_active_subscription(row), TIERS[row["subscription_tier"]]["rank"])
            >= (_is_active_subscription(prior), TIERS[prior["subscription_tier"]]["rank"])
            else (prior, row)
        )
        selected = dict(selected)
        for field in ("user_name", "email"):
            if not selected.get(field) or (field == "user_name" and selected[field] == uid):
                if other.get(field) and not (field == "user_name" and other[field] == uid):
                    selected[field] = other[field]
        roster[uid] = selected
    return {uid: row for uid, row in roster.items() if _is_active_subscription(row)}


def load_csv_roster(aws: AwsCli, bucket: str, key: str) -> tuple[dict[str, dict[str, str]] | None, bool]:
    if not key:
        return None, False
    try:
        return parse_subscription_csv(aws.s3_bytes(bucket, key)), True
    except AwsCliError as exc:
        if exc.code in NOT_FOUND_CODES:
            return None, False
        raise


def parse_identity_mapping(data: bytes) -> dict[str, dict[str, str]]:
    reader = csv.DictReader(io.StringIO(data.decode("utf-8-sig", errors="strict"), newline=""))
    if reader.fieldnames is None:
        raise ValueError("identity mapping CSV requires a header")
    identities: dict[str, dict[str, str]] = {}
    for raw in reader:
        lowered = {sanitize(key).lower(): sanitize(value) for key, value in raw.items() if key is not None}
        uid = canonical_user_id(lowered.get("userid") or lowered.get("user_id"))
        if not uid:
            continue
        name = sanitize(lowered.get("user_name") or lowered.get("username") or lowered.get("name"))
        identity = identities.setdefault(uid, {"user_name": "", "email": ""})
        if name and name != uid:
            identity["user_name"] = name
        if lowered.get("email"):
            identity["email"] = lowered["email"]
    return identities


def load_identity_mapping(aws: AwsCli, bucket: str, key: str) -> dict[str, dict[str, str]]:
    try:
        return parse_identity_mapping(aws.s3_bytes(bucket, key))
    except AwsCliError as exc:
        if exc.code in NOT_FOUND_CODES:
            return {}
        raise


def usage_rows_by_id(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    usage: dict[str, dict[str, Any]] = {}
    for row in rows:
        uid = canonical_user_id(row.get("user_id") or row.get("userid"))
        if not uid:
            continue
        item = usage.setdefault(uid, {
            "user_id": uid, "credits": Decimal(0), "overage": Decimal(0),
            "active_days": 0, "first_active": "", "last_active": "",
            "latest_tier": "Unknown",
        })
        item["credits"] += decimal(row.get("credits"))
        item["overage"] += decimal(row.get("overage"))
        item["active_days"] += int(decimal(row.get("active_days")))
        first = sanitize(row.get("first_active"))
        last = sanitize(row.get("last_active"))
        item["first_active"] = min(filter(None, [item["first_active"], first]), default="")
        item["last_active"] = max(item["last_active"], last)
        item["latest_tier"] = normalize_tier(row.get("latest_tier"))
    return usage


def _identity_from_user(user: dict[str, Any], uid: str) -> dict[str, str]:
    emails = user.get("Emails", [])
    primary = next((item.get("Value", "") for item in emails if item.get("Primary")), "")
    if not primary and emails:
        primary = emails[0].get("Value", "")
    return {
        "user_name": sanitize(user.get("DisplayName") or user.get("UserName") or uid),
        "email": sanitize(primary),
    }


def _describe_user(aws: AwsCli, identity_store_id: str, uid: str) -> dict[str, str]:
    try:
        user = aws.json(
            "identitystore", "describe-user", "--identity-store-id", identity_store_id,
            "--user-id", uid,
        )
    except AwsCliError as exc:
        if exc.code in NOT_FOUND_CODES:
            return {"user_name": uid, "email": ""}
        raise
    return _identity_from_user(user, uid)


def load_identity_store_users(aws: AwsCli, identity_store_id: str) -> dict[str, dict[str, str]] | None:
    """Load all identities in one paginated CLI call; return None when not permitted."""
    try:
        users = aws.json(
            "identitystore", "list-users", "--identity-store-id", identity_store_id,
        ).get("Users", [])
    except AwsCliError as exc:
        if exc.code in ACCESS_DENIED_CODES:
            return None
        raise
    return {
        canonical_user_id(user.get("UserId")): _identity_from_user(
            user, canonical_user_id(user.get("UserId")),
        )
        for user in users if canonical_user_id(user.get("UserId"))
    }


def resolve_application_roster(aws: AwsCli, application_arn: str, identity_store_id: str,
                               latest_usage_tiers: dict[str, str]) -> dict[str, dict[str, str]]:
    if not application_arn:
        raise ValueError("monthly_report.kiro_application_arn is required when subscription CSV is absent")
    assignments = aws.json(
        "sso-admin", "list-application-assignments", "--application-arn", application_arn,
    ).get("ApplicationAssignments", [])
    direct_users: set[str] = set()
    group_tiers: dict[str, list[str]] = defaultdict(list)
    for assignment in assignments:
        principal_id = canonical_user_id(assignment.get("PrincipalId"))
        if assignment.get("PrincipalType") == "USER":
            direct_users.add(principal_id)
        elif assignment.get("PrincipalType") == "GROUP":
            group = aws.json(
                "identitystore", "describe-group", "--identity-store-id", identity_store_id,
                "--group-id", principal_id,
            )
            tier = normalize_tier(group.get("DisplayName"))
            memberships = aws.json(
                "identitystore", "list-group-memberships", "--identity-store-id", identity_store_id,
                "--group-id", principal_id,
            ).get("GroupMemberships", [])
            for membership in memberships:
                uid = canonical_user_id(membership.get("MemberId", {}).get("UserId"))
                if uid:
                    group_tiers[uid].append(tier)
    roster = {}
    users = sorted(direct_users | set(group_tiers))
    identity_index = load_identity_store_users(aws, identity_store_id)
    for uid in users:
        known_group_tiers = [
            tier for tier in group_tiers.get(uid, []) if normalize_tier(tier) != "Unknown"
        ]
        if known_group_tiers:
            tiers = known_group_tiers
            tier_source = "current_iic_group"
        else:
            tiers = [latest_usage_tiers.get(uid, "Unknown")]
            tier_source = "latest_at_data_cutoff"
        identity = (
            identity_index.get(uid, {"user_name": uid, "email": ""})
            if identity_index is not None else _describe_user(aws, identity_store_id, uid)
        )
        roster[uid] = {
            "userid": uid,
            **identity,
            "subscription_status": "ACTIVE",
            "subscription_tier": highest_tier(tiers),
            "subscription_tier_source": tier_source,
            "plan_source": "IIC group" if uid in group_tiers else "IIC direct assignment",
        }
    return roster


def merge_current_rows(month: str, usage: dict[str, dict[str, Any]],
                       roster: dict[str, dict[str, Any]], authoritative: bool,
                       identities: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    rows = []
    for uid in sorted(roster):
        subscription = roster[uid]
        observed_tier = normalize_tier(usage.get(uid, {}).get("latest_tier"))
        roster_tier = normalize_tier(subscription.get("subscription_tier"))
        if authoritative:
            tier = roster_tier if roster_tier != "Unknown" else observed_tier
            tier_source = (
                "current_authoritative_snapshot" if roster_tier != "Unknown" else
                "latest_at_data_cutoff" if observed_tier != "Unknown" else "unknown_direct_assignment"
            )
        elif sanitize(subscription.get("subscription_tier_source")) == "current_iic_group":
            tier = roster_tier if roster_tier != "Unknown" else observed_tier
            tier_source = (
                "current_iic_group" if roster_tier != "Unknown" else
                "latest_at_data_cutoff" if observed_tier != "Unknown" else "unknown_direct_assignment"
            )
        else:
            tier = observed_tier if observed_tier != "Unknown" else roster_tier
            tier_source = "latest_at_data_cutoff" if tier != "Unknown" else "unknown_direct_assignment"
        meta = TIERS[tier]
        item = usage.get(uid, {})
        credits = decimal(item.get("credits"))
        capacity = int(meta["capacity"])
        rate = credits / Decimal(capacity) if capacity else None
        identity = identities.get(uid, {})
        rows.append({
            "month": month,
            "user_id": uid,
            "user_name": sanitize(identity.get("user_name") or subscription.get("user_name") or uid),
            "email": sanitize(identity.get("email") or subscription.get("email")),
            "month_end_tier": tier,
            "credits": credits,
            "overage": decimal(item.get("overage")),
            "capacity": capacity,
            "usage_rate": rate,
            "estimated_plan_cost": meta["price"] if tier != "Unknown" else None,
            "first_active": sanitize(item.get("first_active"))[:10],
            "last_active": sanitize(item.get("last_active"))[:10],
            "active_days": int(decimal(item.get("active_days"))),
            "plan_source": sanitize(subscription.get("plan_source")),
            "checkpoint_subscription_tier_source": tier_source,
        })
    return rows


def collect_current_usage(month: str, settings: dict[str, str], aws: AwsCli) -> dict[str, Any]:
    cutoff_rows = run_athena_query(
        aws, build_data_cutoff_sql(settings["database"], month), settings["workgroup"],
    )
    cutoff_value = sanitize(cutoff_rows[0].get("data_cutoff"))[:10] if cutoff_rows else ""
    data_cutoff = datetime.strptime(cutoff_value, "%Y-%m-%d").date() if cutoff_value else None
    start, end = month_bounds(month)
    if data_cutoff is not None and not start <= data_cutoff < end:
        raise RuntimeError("Athena returned a data cutoff outside the requested month")
    usage = (
        usage_rows_by_id(run_athena_query(
            aws, build_usage_sql(settings["database"], month, data_cutoff), settings["workgroup"],
        ))
        if data_cutoff is not None else {}
    )
    roster, authoritative = load_csv_roster(
        aws, settings["data_bucket"], settings["subscription_csv_key"],
    )
    roster = roster or {}
    needs_latest_tiers = not authoritative or any(
        normalize_tier(subscription.get("subscription_tier")) == "Unknown"
        and normalize_tier(usage.get(canonical_user_id(uid), {}).get("latest_tier")) == "Unknown"
        for uid, subscription in roster.items()
    )
    latest_tiers: dict[str, str] = {}
    if needs_latest_tiers:
        tier_cutoff = data_cutoff or (start - timedelta(days=1))
        latest_rows = run_athena_query(
            aws, build_latest_tiers_sql(settings["database"], tier_cutoff), settings["workgroup"],
        )
        latest_tiers = {
            canonical_user_id(row.get("user_id")): normalize_tier(row.get("latest_tier"))
            for row in latest_rows if canonical_user_id(row.get("user_id"))
        }
    if not authoritative:
        roster = resolve_application_roster(
            aws, settings["application_arn"], settings["identity_store_id"], latest_tiers,
        )
    else:
        for uid, subscription in roster.items():
            canonical_uid = canonical_user_id(uid)
            if (
                normalize_tier(subscription.get("subscription_tier")) == "Unknown"
                and normalize_tier(usage.get(canonical_uid, {}).get("latest_tier")) == "Unknown"
                and normalize_tier(latest_tiers.get(canonical_uid)) != "Unknown"
            ):
                usage.setdefault(canonical_uid, {"credits": Decimal(0)})["latest_tier"] = latest_tiers[canonical_uid]
    identities = load_identity_mapping(
        aws, settings["data_bucket"], settings["identity_mapping_key"],
    )
    for uid, subscription in roster.items():
        identity = identities.setdefault(canonical_user_id(uid), {"user_name": "", "email": ""})
        name = sanitize(subscription.get("user_name"))
        email = sanitize(subscription.get("email"))
        if name and name != canonical_user_id(uid):
            identity["user_name"] = name
        if email:
            identity["email"] = email
    return {
        "month": month,
        "data_cutoff": data_cutoff.isoformat() if data_cutoff is not None else None,
        "roster_source": "authoritative_subscription_csv" if authoritative else "iam_identity_center",
        "no_month_data": data_cutoff is None,
        "rows": merge_current_rows(month, usage, roster, authoritative, identities),
    }


def usage_status(row: dict[str, Any]) -> str:
    credits = decimal(row.get("credits"))
    capacity = decimal(row.get("capacity"))
    if credits == 0:
        return "zero"
    if capacity <= 0:
        return "unknown"
    rate = credits / capacity
    if rate < Decimal("0.10"):
        return "low"
    if rate < Decimal("0.50"):
        return "medium"
    return "normal"


def _json_number(value: Any) -> int | float:
    number = decimal(value)
    return int(number) if number == number.to_integral_value() else float(number)


def build_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    for row in rows:
        credits = decimal(row.get("credits"))
        capacity = int(decimal(row.get("capacity")))
        rate = credits / Decimal(capacity) if capacity > 0 else None
        records.append({
            "user_name": sanitize(row.get("user_name")) or "未识别用户",
            "email": sanitize(row.get("email")),
            "tier": normalize_tier(row.get("month_end_tier")),
            "credits": _json_number(credits),
            "capacity": capacity,
            "usage_percent": round(float(rate * Decimal(100)), 2) if rate is not None else None,
            "last_active": sanitize(row.get("last_active")),
            "active_days": int(decimal(row.get("active_days"))),
            "status": usage_status(row),
            "monthly_cost_usd": (
                None if row.get("estimated_plan_cost") is None
                else _json_number(row.get("estimated_plan_cost"))
            ),
            "tier_source": sanitize(row.get("checkpoint_subscription_tier_source")),
        })
    return records


def filter_records(records: list[dict[str, Any]], risk: str | None = None,
                   tier: str | None = None) -> list[dict[str, Any]]:
    result = records
    if risk == "zero":
        result = [record for record in result if record["status"] == "zero"]
    elif risk == "low":
        result = [record for record in result if record["status"] == "low"]
    elif risk == "all":
        result = [record for record in result if record["status"] in {"zero", "low"}]
    if tier:
        result = [record for record in result if record["tier"] == tier]
    return list(result)


def sort_records(records: list[dict[str, Any]], order: str | None = None) -> list[dict[str, Any]]:
    name = lambda record: (str(record["user_name"]).casefold(), str(record["email"]).casefold())
    percent = lambda record: float("inf") if record["usage_percent"] is None else float(record["usage_percent"])
    if order == "credits":
        key = lambda record: (float(record["credits"]), percent(record), *name(record))
    elif order == "last-active":
        key = lambda record: (bool(record["last_active"]), str(record["last_active"]), percent(record), *name(record))
    elif order == "cost":
        key = lambda record: (
            record["monthly_cost_usd"] is None,
            -float(record["monthly_cost_usd"] or 0), percent(record), *name(record),
        )
    elif order == "usage":
        key = lambda record: (percent(record), float(record["credits"]), *name(record))
    else:
        key = lambda record: (
            STATUS_ORDER[record["status"]], percent(record), float(record["credits"]), *name(record),
        )
    return sorted(records, key=key)


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {status: 0 for status in STATUS_ORDER}
    for record in records:
        counts[record["status"]] += 1
    known_costs = [record["monthly_cost_usd"] for record in records if record["monthly_cost_usd"] is not None]
    return {
        "users": len(records), **counts,
        "total_credits": _json_number(sum((decimal(row["credits"]) for row in records), Decimal(0))),
        "known_monthly_cost_usd": _json_number(sum((decimal(value) for value in known_costs), Decimal(0))),
        "unknown_cost_users": len(records) - len(known_costs),
    }


def _display_width(value: str) -> int:
    return sum(
        2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1
        for char in ANSI_RE.sub("", value)
    )


def _pad(value: Any, width: int) -> str:
    text = str(value)
    return text + " " * max(0, width - _display_width(text))


def _display_record(record: dict[str, Any], use_color: bool) -> dict[str, Any]:
    status_label = STATUS_LABELS[record["status"]]
    if use_color:
        status_label = f"{STATUS_COLORS[record['status']]}{status_label}{RESET}"
    capacity = f"{record['capacity']:,}" if record["capacity"] else "?"
    return {
        **record,
        "email": record["email"] or "-",
        "credits_capacity": f"{record['credits']:,}/{capacity}",
        "usage_display": "N/A" if record["usage_percent"] is None else f"{record['usage_percent']:.2f}%",
        "last_active": record["last_active"] or "-",
        "status_label": status_label,
        "cost_display": (
            "N/A" if record["monthly_cost_usd"] is None
            else f"${float(record['monthly_cost_usd']):.2f}"
        ),
        "tier_source_label": TIER_SOURCE_LABELS.get(record["tier_source"], record["tier_source"]),
    }


def render_table(snapshot: dict[str, Any], records: list[dict[str, Any]], stream: TextIO,
                 total_subscribers: int, use_color: bool = False,
                 queried_at: datetime | None = None) -> None:
    queried = queried_at or datetime.now(timezone.utc)
    if queried.tzinfo is None:
        queried = queried.replace(tzinfo=timezone.utc)
    summary = summarize(records)
    source = "权威订阅 CSV" if snapshot["roster_source"] == "authoritative_subscription_csv" else "IAM Identity Center"
    cutoff_text = snapshot["data_cutoff"] or "本月暂无可用数据"
    print("Kiro Credits 用量巡检", file=stream)
    print(
        f"月份：{snapshot['month']}｜数据截至：{cutoff_text}｜"
        f"查询时间：{queried.astimezone(SHANGHAI).strftime('%Y-%m-%d %H:%M:%S UTC+8')}",
        file=stream,
    )
    print(f"名册来源：{source}｜说明：Kiro 源数据通常可能延迟 1–2 天。", file=stream)
    if snapshot.get("no_month_data"):
        print("警告：本月尚无可用源数据；以下零使用仅表示当前未收到用量记录，不能认定用户实际未使用。", file=stream)
    print(
        f"显示用户 {summary['users']}/{total_subscribers}｜零使用 {summary['zero']}｜"
        f"低使用 {summary['low']}｜中使用 {summary['medium']}｜正常 {summary['normal']}｜"
        f"套餐未知 {summary['unknown']}｜累计 Credits {summary['total_credits']:,}｜"
        f"已知月成本 ${float(summary['known_monthly_cost_usd']):.2f}｜"
        f"成本未知 {summary['unknown_cost_users']} 人",
        file=stream,
    )
    print(file=stream)
    display_rows = [_display_record(record, use_color) for record in records]
    widths = [
        max([_display_width(heading)] + [_display_width(str(row[key])) for row in display_rows])
        for heading, key in TABLE_COLUMNS
    ]
    print("  ".join(_pad(heading, width) for (heading, _), width in zip(TABLE_COLUMNS, widths)), file=stream)
    print("  ".join("-" * width for width in widths), file=stream)
    for row in display_rows:
        print("  ".join(_pad(row[key], width) for (_, key), width in zip(TABLE_COLUMNS, widths)), file=stream)


def render_csv(records: list[dict[str, Any]], stream: TextIO) -> None:
    writer = csv.DictWriter(stream, fieldnames=OUTPUT_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows({field: record.get(field) for field in OUTPUT_FIELDS} for record in records)


def render_json(snapshot: dict[str, Any], records: list[dict[str, Any]], stream: TextIO,
                total_subscribers: int, queried_at: datetime | None = None) -> None:
    queried = queried_at or datetime.now(timezone.utc)
    if queried.tzinfo is None:
        queried = queried.replace(tzinfo=timezone.utc)
    payload = {
        "month": snapshot["month"], "data_cutoff": snapshot["data_cutoff"],
        "queried_at": queried.astimezone(SHANGHAI).isoformat(timespec="seconds"),
        "timezone": "Asia/Shanghai", "roster_source": snapshot["roster_source"],
        "no_month_data": bool(snapshot.get("no_month_data")),
        "usage_interpretation": (
            "No current-month source data is available; zero values must not be interpreted as confirmed non-use"
            if snapshot.get("no_month_data") else "Usage is cumulative through data_cutoff"
        ),
        "source_lag_warning": "Kiro source data may lag by 1-2 days",
        "total_subscribers": total_subscribers,
        "summary": summarize(records), "users": records,
    }
    json.dump(payload, stream, ensure_ascii=False, indent=2)
    stream.write("\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--month", help="natural month in YYYY-MM; default: current month in Asia/Shanghai")
    parser.add_argument("--risk", choices=("zero", "low", "all"), help="zero, nonzero <10%%, or both")
    parser.add_argument("--tier", choices=tuple(TIERS))
    parser.add_argument(
        "--sort", choices=("usage", "credits", "last-active", "cost"),
        help="sorting; default groups zero/low/medium/normal before utilization",
    )
    parser.add_argument("--format", choices=("table", "csv", "json"), default="table")
    parser.add_argument("--profile", help="AWS named profile; defaults to config/env/credential chain")
    parser.add_argument("--region", help="AWS Region; defaults to config aws.region")
    parser.add_argument("--no-color", action="store_true", help="disable ANSI status colors")
    return parser.parse_args(argv)


def run(args: argparse.Namespace, stdout: TextIO = sys.stdout, stderr: TextIO = sys.stderr,
        aws_factory: Callable[[str | None, str], AwsCli] = AwsCli) -> int:
    settings = build_settings(load_project_config(args.config), args.region)
    current_month = current_shanghai_month()
    month = validate_month(args.month or current_month, current_month)
    profile = choose_profile(
        args.profile, settings["profile"], settings["account_id"], list_aws_profiles(),
    )
    snapshot = collect_current_usage(month, settings, aws_factory(profile, settings["region"]))
    all_records = build_records(snapshot["rows"])
    records = sort_records(filter_records(all_records, args.risk, args.tier), args.sort)
    if args.format == "json":
        render_json(snapshot, records, stdout, len(all_records))
    elif args.format == "csv":
        render_csv(records, stdout)
        print(
            f"Kiro {month}: showing {len(records)}/{len(all_records)} users; "
            f"data cutoff {snapshot['data_cutoff'] or 'NO_MONTH_DATA'}; source may lag 1-2 days; "
            + ("no month data, zero values are not confirmed non-use" if snapshot.get("no_month_data") else "cumulative usage"),
            file=stderr,
        )
    else:
        render_table(
            snapshot, records, stdout, len(all_records),
            use_color=not args.no_color and stdout.isatty(),
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except KeyboardInterrupt:
        print("已取消。", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
