import importlib.util
import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "list_current_credit_usage", ROOT / "scripts" / "list_current_credit_usage.py",
)
usage_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(usage_cli)


class UsageCliTests(unittest.TestCase):
    def sample_records(self):
        return [
            {
                "user_name": "正常用户", "email": "normal@example.com", "tier": "Pro",
                "credits": 600, "capacity": 1000, "usage_percent": 60.0,
                "last_active": "2026-09-08", "active_days": 6, "status": "normal",
                "monthly_cost_usd": 20, "tier_source": "latest_at_data_cutoff",
            },
            {
                "user_name": "零使用用户", "email": "zero@example.com", "tier": "Pro+",
                "credits": 0, "capacity": 2000, "usage_percent": 0.0,
                "last_active": "", "active_days": 0, "status": "zero",
                "monthly_cost_usd": 40, "tier_source": "current_iic_group",
            },
            {
                "user_name": "低使用用户", "email": "low@example.com", "tier": "Pro",
                "credits": 50, "capacity": 1000, "usage_percent": 5.0,
                "last_active": "2026-09-02", "active_days": 1, "status": "low",
                "monthly_cost_usd": 20, "tier_source": "current_authoritative_snapshot",
            },
            {
                "user_name": "中使用用户", "email": "medium@example.com", "tier": "Pro",
                "credits": 300, "capacity": 1000, "usage_percent": 30.0,
                "last_active": "2026-09-07", "active_days": 4, "status": "medium",
                "monthly_cost_usd": 20, "tier_source": "latest_at_data_cutoff",
            },
        ]

    def test_simple_config_loader_ignores_notification_secrets(self):
        text = '''
aws:
  region: us-east-1 # comment
  account_id: "123456789012"
s3:
  bucket_name: "usage#bucket"
glue:
  database_name: kiro_analytics
identity_center:
  identity_store_id: d-example
monthly_report:
  kiro_application_arn: "arn:aws:sso::123456789012:application/example"
  subscription_csv_key: ""
  feishu_prod_secret_arn: "must-not-be-loaded"
'''
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text(text, encoding="utf-8")
            config = usage_cli.load_project_config(path)
        self.assertEqual("usage#bucket", config["s3"]["bucket_name"])
        self.assertEqual("123456789012", config["aws"]["account_id"])
        self.assertNotIn("feishu_prod_secret_arn", config["monthly_report"])
        settings = usage_cli.build_settings(config)
        self.assertEqual("kiro-analytics-workgroup", settings["workgroup"])
        self.assertEqual(usage_cli.IDENTITY_MAPPING_KEY, settings["identity_mapping_key"])

    def test_aws_cli_errors_are_redacted_and_processes_are_bounded(self):
        aws = usage_cli.AwsCli(None, "us-east-1", process_timeout=7)
        failed = Mock(
            returncode=1, stdout="",
            stderr="An error occurred (AccessDeniedException): alice@example.com is denied",
        )
        with patch.object(usage_cli.subprocess, "run", return_value=failed) as run:
            with self.assertRaises(usage_cli.AwsCliError) as caught:
                aws.json("identitystore", "list-users")
        self.assertEqual("AccessDeniedException", caught.exception.code)
        self.assertNotIn("alice@example.com", str(caught.exception))
        self.assertEqual(7, run.call_args.kwargs["timeout"])
        numeric = usage_cli.AwsCli._error("s3 read", "An error occurred (404): missing@example.com")
        self.assertEqual("404", numeric.code)
        self.assertNotIn("missing@example.com", str(numeric))
        missing = Mock()
        missing.s3_bytes.side_effect = numeric
        self.assertEqual((None, False), usage_cli.load_csv_roster(missing, "bucket", "missing.csv"))
        self.assertEqual({}, usage_cli.load_identity_mapping(missing, "bucket", "missing.csv"))

        with patch.object(
            usage_cli.subprocess, "run",
            side_effect=usage_cli.subprocess.TimeoutExpired(["aws"], 7),
        ):
            with self.assertRaisesRegex(usage_cli.AwsCliError, "ClientTimeout"):
                aws.s3_bytes("bucket", "key")
            with self.assertRaisesRegex(usage_cli.AwsCliError, "ClientTimeout"):
                aws.json("athena", "get-query-execution")
            self.assertEqual([], usage_cli.list_aws_profiles())

        transient = Mock()
        transient.json.side_effect = usage_cli.AwsCliError(
            "identitystore list-users", "ThrottlingException",
        )
        with self.assertRaisesRegex(usage_cli.AwsCliError, "ThrottlingException"):
            usage_cli.load_identity_store_users(transient, "store")

        athena = Mock()
        athena.json.side_effect = [
            {"QueryExecutionId": "qid-redacted"},
            {"QueryExecution": {"Status": {
                "State": "FAILED", "StateChangeReason": "alice@example.com in s3://private/path",
            }}},
        ]
        with self.assertRaises(RuntimeError) as athena_error:
            usage_cli.run_athena_query(athena, "SELECT", "wg")
        self.assertIn("qid-redacted", str(athena_error.exception))
        self.assertNotIn("alice@example.com", str(athena_error.exception))
        self.assertNotIn("s3://private", str(athena_error.exception))

    def test_settings_validation_month_and_profile_selection(self):
        config = {section: {} for section in usage_cli._CONFIG_KEYS}
        with self.assertRaisesRegex(ValueError, "data_bucket"):
            usage_cli.build_settings(config)
        config["s3"]["bucket_name"] = "bucket"
        config["identity_center"]["identity_store_id"] = "store"
        config["monthly_report"]["kiro_application_arn"] = "app"
        config["glue"]["database_name"] = "db-name; DROP TABLE"
        with self.assertRaisesRegex(ValueError, "Athena identifier"):
            usage_cli.build_settings(config)
        boundary = datetime(2026, 9, 30, 17, 0, tzinfo=timezone.utc)
        self.assertEqual("2026-10", usage_cli.current_shanghai_month(boundary))
        self.assertEqual("2026-09", usage_cli.validate_month("2026-09", "2026-10"))
        with self.assertRaisesRegex(ValueError, "future"):
            usage_cli.validate_month("2026-11", "2026-10")
        with self.assertRaisesRegex(ValueError, "YYYY-MM"):
            usage_cli.validate_month("2026-9", "2026-10")
        self.assertEqual("explicit", usage_cli.choose_profile("explicit", "configured", "123", ["123"], {}))
        self.assertEqual("configured", usage_cli.choose_profile(None, "configured", "123", ["123"], {}))
        self.assertEqual("123", usage_cli.choose_profile(None, "", "123", ["123"], {}))
        self.assertIsNone(
            usage_cli.choose_profile(None, "", "123", ["123"], {"AWS_PROFILE": "environment"}),
        )

    def test_sql_is_cutoff_consistent_and_deterministic(self):
        cutoff = datetime.strptime("2026-09-08", "%Y-%m-%d").date()
        usage_sql = usage_cli.build_usage_sql("kiro_analytics", "2026-09", cutoff)
        latest_sql = usage_cli.build_latest_tiers_sql("kiro_analytics", cutoff)
        self.assertIn("2026-09-01 00:00:00", usage_sql)
        self.assertIn("2026-09-09 00:00:00", usage_sql)
        self.assertIn("CASE MAX(tier_rank)", usage_sql)
        self.assertIn("max_by(d.day_tier, d.usage_date)", usage_sql)
        self.assertIn("2026-09-09 00:00:00", latest_sql)
        self.assertIn("CASE MAX(tier_rank)", latest_sql)
        self.assertIn("LIKE '%PRO+%'", usage_sql)
        self.assertIn("LIKE '%PROPLUS%'", usage_sql)
        self.assertLess(usage_sql.index("LIKE '%PRO+%'"), usage_sql.rindex("LIKE '%PRO%'"))
        self.assertNotIn("2026-10-01 00:00:00", usage_sql)

    def test_athena_query_polls_and_paginates(self):
        aws = Mock()
        aws.json.side_effect = [
            {"QueryExecutionId": "qid"},
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
            {
                "ResultSet": {"Rows": [
                    {"Data": [{"VarCharValue": "user_id"}, {"VarCharValue": "credits"}]},
                    {"Data": [{"VarCharValue": "u1"}, {"VarCharValue": "5"}]},
                ]},
                "NextToken": "next",
            },
            {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "u2"}]}]}},
        ]
        sleep = Mock()
        rows = usage_cli.run_athena_query(aws, "SELECT", "wg", sleep=sleep)
        self.assertEqual(
            [{"user_id": "u1", "credits": "5"}, {"user_id": "u2", "credits": ""}], rows,
        )
        sleep.assert_called_once_with(1)
        self.assertIn("--next-token", aws.json.call_args_list[-1].args)
        self.assertIn("--no-paginate", aws.json.call_args_list[-1].args)

    def test_roster_and_identity_csv_parsing(self):
        roster = usage_cli.parse_subscription_csv((
            "user_id,name,email,plan,status\n"
            "d-store.u1,Alice,alice@example.com,Pro,ACTIVE\n"
            "u1,u1,,Power,CANCELLED\n"
            "u2,Former,former@example.com,Power,INACTIVE\n"
        ).encode("utf-8-sig"))
        self.assertEqual("Pro", roster["u1"]["subscription_tier"])
        self.assertEqual("Alice", roster["u1"]["user_name"])
        self.assertNotIn("u2", roster)
        with self.assertRaisesRegex(ValueError, "unsupported subscription_status"):
            usage_cli.parse_subscription_csv(
                b"userid,user_name,subscription_tier,subscription_status\nu3,Unknown,Pro,FUTURE_STATE\n"
            )
        identities = usage_cli.parse_identity_mapping((
            "userid,username,email,subscription_tier\n"
            "u1,=Formula,alice@example.com,Power\n"
        ).encode())
        self.assertEqual("'=Formula", identities["u1"]["user_name"])
        self.assertNotIn("subscription_tier", identities["u1"])
        self.assertEqual({}, usage_cli.parse_subscription_csv(
            b"userid,user_name,subscription_tier,subscription_status\n"
        ))
        self.assertEqual("Power", usage_cli.highest_tier(["Pro", "Pro+", "Power"]))

    def test_unknown_current_tier_falls_back_to_observed_tier(self):
        usage = {"u1": {"latest_tier": "Pro", "credits": 20}}
        identity = {"u1": {"user_name": "用户", "email": ""}}
        for authoritative, source in ((True, "S3 CSV"), (False, "IIC group")):
            rows = usage_cli.merge_current_rows("2026-09", usage, {"u1": {
                "userid": "u1", "subscription_tier": "Unknown", "plan_source": source,
            }}, authoritative, identity)
            self.assertEqual("Pro", rows[0]["month_end_tier"])
            self.assertEqual("latest_at_data_cutoff", rows[0]["checkpoint_subscription_tier_source"])

    def test_status_build_filter_and_sort(self):
        rows = [
            {
                "user_name": "零", "email": "", "month_end_tier": "Pro", "credits": 0,
                "capacity": 1000, "last_active": "", "active_days": 0,
                "estimated_plan_cost": 20,
                "checkpoint_subscription_tier_source": "latest_at_data_cutoff",
            },
            {
                "user_name": "低", "email": "low@example.com", "month_end_tier": "Pro+",
                "credits": 100, "capacity": 2000, "last_active": "2026-09-02",
                "active_days": 1, "estimated_plan_cost": 40,
                "checkpoint_subscription_tier_source": "current_iic_group",
            },
            {
                "user_name": "未知", "email": "", "month_end_tier": "Unknown", "credits": 3,
                "capacity": 0, "last_active": "2026-09-01", "active_days": 1,
                "estimated_plan_cost": None,
                "checkpoint_subscription_tier_source": "unknown_direct_assignment",
            },
        ]
        records = usage_cli.build_records(rows)
        self.assertEqual(["zero", "low", "unknown"], [record["status"] for record in records])
        self.assertIsNone(records[2]["usage_percent"])
        self.assertIsNone(records[2]["monthly_cost_usd"])
        self.assertEqual("N/A", usage_cli._display_record(records[2], False)["cost_display"])
        summary = usage_cli.summarize(records)
        self.assertEqual(1, summary["unknown_cost_users"])
        self.assertEqual(60, summary["known_monthly_cost_usd"])
        self.assertEqual("low", usage_cli.usage_status({"credits": Decimal("99.99"), "capacity": 1000}))
        self.assertEqual("medium", usage_cli.usage_status({"credits": 100, "capacity": 1000}))
        self.assertEqual("medium", usage_cli.usage_status({"credits": Decimal("499.99"), "capacity": 1000}))
        self.assertEqual("normal", usage_cli.usage_status({"credits": 500, "capacity": 1000}))
        self.assertEqual(["零"], [r["user_name"] for r in usage_cli.filter_records(records, "zero")])
        self.assertEqual(["低"], [r["user_name"] for r in usage_cli.filter_records(records, "low")])
        self.assertEqual(["零", "低"], [r["user_name"] for r in usage_cli.filter_records(records, "all")])
        self.assertEqual(["低"], [r["user_name"] for r in usage_cli.filter_records(records, tier="Pro+")])
        self.assertEqual(
            ["零使用用户", "低使用用户", "中使用用户", "正常用户"],
            [record["user_name"] for record in usage_cli.sort_records(self.sample_records())],
        )
        self.assertEqual(
            ["零使用用户", "低使用用户", "中使用用户", "正常用户"],
            [r["user_name"] for r in usage_cli.sort_records(self.sample_records(), "last-active")],
        )
        self.assertEqual(
            ["零使用用户", "低使用用户", "中使用用户", "正常用户"],
            [r["user_name"] for r in usage_cli.sort_records(self.sample_records(), "cost")],
        )
        self.assertEqual(
            ["零使用用户", "低使用用户", "中使用用户", "正常用户"],
            [r["user_name"] for r in usage_cli.sort_records(self.sample_records(), "credits")],
        )
        self.assertEqual(
            ["零使用用户", "低使用用户", "中使用用户", "正常用户"],
            [r["user_name"] for r in usage_cli.sort_records(self.sample_records(), "usage")],
        )
        self.assertEqual(
            ["低使用用户"],
            [r["user_name"] for r in usage_cli.filter_records(self.sample_records(), "all", "Pro")],
        )
        sortable = [
            {"user_name": "A", "email": "", "credits": 60, "usage_percent": 60.0,
             "monthly_cost_usd": 20, "last_active": "2026-09-08", "status": "normal"},
            {"user_name": "B", "email": "", "credits": 500, "usage_percent": 5.0,
             "monthly_cost_usd": 100, "last_active": "2026-09-02", "status": "low"},
            {"user_name": "C", "email": "", "credits": 30, "usage_percent": 30.0,
             "monthly_cost_usd": 40, "last_active": "", "status": "medium"},
        ]
        self.assertEqual(["B", "C", "A"], [r["user_name"] for r in usage_cli.sort_records(sortable)])
        self.assertEqual(["B", "C", "A"], [r["user_name"] for r in usage_cli.sort_records(sortable, "usage")])
        self.assertEqual(["C", "A", "B"], [r["user_name"] for r in usage_cli.sort_records(sortable, "credits")])
        self.assertEqual(["C", "B", "A"], [r["user_name"] for r in usage_cli.sort_records(sortable, "last-active")])
        self.assertEqual(["B", "C", "A"], [r["user_name"] for r in usage_cli.sort_records(sortable, "cost")])

    def test_table_csv_and_json_rendering(self):
        snapshot = {
            "month": "2026-09", "data_cutoff": "2026-09-08",
            "roster_source": "iam_identity_center",
        }
        records = usage_cli.sort_records(self.sample_records())
        table = io.StringIO()
        usage_cli.render_table(
            snapshot, records, table, total_subscribers=4, use_color=True,
            queried_at=datetime(2026, 9, 10, 6, 30, tzinfo=timezone.utc),
        )
        rendered = table.getvalue()
        self.assertIn("Kiro Credits 用量巡检", rendered)
        self.assertIn("数据截至：2026-09-08", rendered)
        self.assertIn("查询时间：2026-09-10 14:30:00 UTC+8", rendered)
        self.assertIn("显示用户 4/4｜零使用 1｜低使用 1｜中使用 1｜正常 1", rendered)
        self.assertIn("\033[31m零使用\033[0m", rendered)
        self.assertIn("当前 IIC 套餐组", rendered)

        csv_output = io.StringIO()
        usage_cli.render_csv(records, csv_output)
        csv_text = csv_output.getvalue()
        self.assertTrue(csv_text.startswith("user_name,email,tier,credits,capacity"))
        self.assertEqual(usage_cli.OUTPUT_FIELDS, csv_text.splitlines()[0].split(","))
        self.assertNotIn("\033[", csv_text)
        self.assertEqual(5, len(csv_text.splitlines()))

        json_output = io.StringIO()
        usage_cli.render_json(
            snapshot, records, json_output, total_subscribers=4,
            queried_at=datetime(2026, 9, 10, 6, 30, tzinfo=timezone.utc),
        )
        payload = json.loads(json_output.getvalue())
        self.assertEqual("Asia/Shanghai", payload["timezone"])
        self.assertEqual(
            {"month", "data_cutoff", "queried_at", "timezone", "roster_source",
             "no_month_data", "usage_interpretation", "source_lag_warning",
             "total_subscribers", "summary", "users"},
            set(payload),
        )
        self.assertEqual(usage_cli.OUTPUT_FIELDS, list(payload["users"][0]))
        self.assertEqual(4, payload["total_subscribers"])
        self.assertEqual(4, len(payload["users"]))
        self.assertNotIn("\033[", json_output.getvalue())

    def test_roster_resolution_batches_identity_lookup_and_falls_back(self):
        aws = Mock()
        aws.json.side_effect = [
            {"ApplicationAssignments": [
                {"PrincipalType": "USER", "PrincipalId": "u1"},
                {"PrincipalType": "USER", "PrincipalId": "u2"},
            ]},
            {"Users": [
                {"UserId": "u1", "DisplayName": "用户一", "Emails": [{"Value": "u1@example.com", "Primary": True}]},
                {"UserId": "u2", "DisplayName": "用户二", "Emails": []},
            ]},
        ]
        roster = usage_cli.resolve_application_roster(
            aws, "app", "store", {"u1": "Pro", "u2": "Pro+"},
        )
        self.assertEqual({"u1", "u2"}, set(roster))
        self.assertEqual("用户一", roster["u1"]["user_name"])
        self.assertEqual("u1@example.com", roster["u1"]["email"])
        self.assertEqual(2, aws.json.call_count)
        self.assertEqual("list-users", aws.json.call_args_list[1].args[1])

        fallback = Mock()
        fallback.json.side_effect = [
            {"ApplicationAssignments": [{"PrincipalType": "USER", "PrincipalId": "u1"}]},
            usage_cli.AwsCliError("identitystore list-users", "AccessDeniedException"),
            {"UserId": "u1", "DisplayName": "回退用户", "Emails": []},
        ]
        fallback_roster = usage_cli.resolve_application_roster(
            fallback, "app", "store", {"u1": "Pro"},
        )
        self.assertEqual("回退用户", fallback_roster["u1"]["user_name"])
        self.assertEqual("describe-user", fallback.json.call_args_list[2].args[1])

        groups = Mock()
        groups.json.side_effect = [
            {"ApplicationAssignments": [
                {"PrincipalType": "GROUP", "PrincipalId": "g-unknown"},
                {"PrincipalType": "GROUP", "PrincipalId": "g-power"},
            ]},
            {"DisplayName": "Unrecognized group"},
            {"GroupMemberships": [{"MemberId": {"UserId": "u1"}}]},
            {"DisplayName": "Kiro-Power-users"},
            {"GroupMemberships": [{"MemberId": {"UserId": "u1"}}]},
            {"Users": [{"UserId": "u1", "DisplayName": "组用户", "Emails": []}]},
        ]
        group_roster = usage_cli.resolve_application_roster(
            groups, "app", "store", {"u1": "Pro+"},
        )
        self.assertEqual("Power", group_roster["u1"]["subscription_tier"])
        self.assertEqual("current_iic_group", group_roster["u1"]["subscription_tier_source"])

        unknown_group = Mock()
        unknown_group.json.side_effect = [
            {"ApplicationAssignments": [{"PrincipalType": "GROUP", "PrincipalId": "g1"}]},
            {"DisplayName": "Unrecognized group"},
            {"GroupMemberships": [{"MemberId": {"UserId": "u1"}}]},
            {"Users": [{"UserId": "u1", "DisplayName": "组用户", "Emails": []}]},
        ]
        unknown_roster = usage_cli.resolve_application_roster(
            unknown_group, "app", "store", {"u1": "Pro+"},
        )
        self.assertEqual("Pro+", unknown_roster["u1"]["subscription_tier"])
        self.assertEqual("latest_at_data_cutoff", unknown_roster["u1"]["subscription_tier_source"])

    def test_no_month_data_still_lists_current_roster_without_false_cutoff(self):
        aws = Mock()
        settings = {
            "database": "db", "workgroup": "wg", "data_bucket": "data",
            "subscription_csv_key": "current.csv", "application_arn": "app",
            "identity_store_id": "store", "identity_mapping_key": "mapping.csv",
        }
        roster = {"u1": {
            "userid": "u1", "user_name": "尚无数据用户", "subscription_tier": "Pro",
            "subscription_status": "ACTIVE",
        }}
        with patch.object(usage_cli, "run_athena_query", return_value=[{"data_cutoff": ""}]) as query, \
                patch.object(usage_cli, "load_csv_roster", return_value=(roster, True)), \
                patch.object(usage_cli, "load_identity_mapping", return_value={}):
            snapshot = usage_cli.collect_current_usage("2026-09", settings, aws)
        self.assertIsNone(snapshot["data_cutoff"])
        self.assertTrue(snapshot["no_month_data"])
        self.assertEqual(1, query.call_count)
        self.assertEqual(Decimal(0), snapshot["rows"][0]["credits"])
        output = io.StringIO()
        usage_cli.render_table(snapshot, usage_cli.build_records(snapshot["rows"]), output, 1)
        self.assertIn("数据截至：本月暂无可用数据", output.getvalue())
        self.assertIn("不能认定用户实际未使用", output.getvalue())

    def test_full_collection_uses_only_allowlisted_read_operations(self):
        class FakeAws:
            def __init__(self):
                self.calls = []
                self.query_sql = {}
                self.next_query = 0
                self.s3_reads = []

            def json(self, service, operation, *arguments):
                self.calls.append((service, operation))
                if (service, operation) == ("athena", "start-query-execution"):
                    self.next_query += 1
                    query_id = f"q{self.next_query}"
                    self.query_sql[query_id] = arguments[arguments.index("--query-string") + 1]
                    return {"QueryExecutionId": query_id}
                if (service, operation) == ("athena", "get-query-execution"):
                    return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
                if (service, operation) == ("athena", "get-query-results"):
                    query_id = arguments[arguments.index("--query-execution-id") + 1]
                    sql = self.query_sql[query_id]
                    if "MAX(TRY(date_parse" in sql:
                        values = [["data_cutoff"], ["2026-09-08"]]
                    elif "CAST(SUM(d.credits)" in sql:
                        values = [
                            ["user_id", "credits", "overage", "first_active", "last_active", "active_days", "tier_history", "latest_tier"],
                            ["u1", "10", "0", "2026-09-02", "2026-09-08", "2", "Pro", "Pro"],
                        ]
                    else:
                        values = [["user_id", "latest_tier"], ["u1", "Pro"]]
                    return {"ResultSet": {"Rows": [
                        {"Data": [{"VarCharValue": value} for value in row]} for row in values
                    ]}}
                if (service, operation) == ("sso-admin", "list-application-assignments"):
                    return {"ApplicationAssignments": [{"PrincipalType": "USER", "PrincipalId": "u1"}]}
                if (service, operation) == ("identitystore", "list-users"):
                    return {"Users": [{"UserId": "u1", "DisplayName": "用户", "Emails": []}]}
                raise AssertionError(f"unexpected AWS operation: {service} {operation}")

            def s3_bytes(self, bucket, key):
                self.s3_reads.append((bucket, key))
                return b"userid,username,email\nu1,User,user@example.com\n"

        aws = FakeAws()
        settings = {
            "database": "db", "workgroup": "wg", "data_bucket": "data",
            "subscription_csv_key": "", "application_arn": "app",
            "identity_store_id": "store", "identity_mapping_key": "mapping.csv",
        }
        snapshot = usage_cli.collect_current_usage("2026-09", settings, aws)
        self.assertEqual(1, len(snapshot["rows"]))
        self.assertEqual([("data", "mapping.csv")], aws.s3_reads)
        self.assertLessEqual(set(aws.calls), {
            ("athena", "start-query-execution"), ("athena", "get-query-execution"),
            ("athena", "get-query-results"),
            ("sso-admin", "list-application-assignments"),
            ("identitystore", "list-users"),
        })
        prohibited = {"lambda", "secretsmanager", "put-object", "delete-object", "invoke"}
        self.assertFalse(any(service in prohibited or operation in prohibited for service, operation in aws.calls))

    def test_authoritative_unknown_without_month_usage_uses_historical_cutoff_tier(self):
        aws = Mock()
        settings = {
            "database": "db", "workgroup": "wg", "data_bucket": "data",
            "subscription_csv_key": "current.csv", "application_arn": "app",
            "identity_store_id": "store", "identity_mapping_key": "mapping.csv",
        }
        roster = {"u1": {
            "userid": "u1", "user_name": "未知套餐用户", "subscription_tier": "Unknown",
            "subscription_status": "ACTIVE",
        }}
        with patch.object(usage_cli, "run_athena_query", side_effect=[
            [{"data_cutoff": "2026-09-08"}],
            [{"user_id": "other", "credits": "10", "latest_tier": "Pro"}],
            [{"user_id": "u1", "latest_tier": "Pro+"}],
        ]) as query, patch.object(
            usage_cli, "load_csv_roster", return_value=(roster, True),
        ), patch.object(usage_cli, "load_identity_mapping", return_value={}):
            snapshot = usage_cli.collect_current_usage("2026-09", settings, aws)
        self.assertEqual(3, query.call_count)
        self.assertEqual(1, len(snapshot["rows"]))
        self.assertEqual(Decimal(0), snapshot["rows"][0]["credits"])
        self.assertEqual("Pro+", snapshot["rows"][0]["month_end_tier"])
        self.assertEqual(
            "latest_at_data_cutoff",
            snapshot["rows"][0]["checkpoint_subscription_tier_source"],
        )

    def test_collect_authoritative_roster_is_cutoff_consistent(self):
        aws = Mock()
        settings = {
            "database": "db", "workgroup": "wg", "data_bucket": "data",
            "subscription_csv_key": "current.csv", "application_arn": "app",
            "identity_store_id": "store", "identity_mapping_key": "mapping.csv",
        }
        roster = {
            "u1": {"userid": "u1", "user_name": "零用户", "subscription_tier": "Pro+"},
            "u2": {"userid": "u2", "user_name": "活跃用户", "subscription_tier": "Pro"},
        }
        query_results = [
            [{"data_cutoff": "2026-09-08 00:00:00.000"}],
            [{
                "user_id": "u2", "credits": "50", "overage": "0",
                "first_active": "2026-09-02", "last_active": "2026-09-08",
                "active_days": "2", "latest_tier": "Pro+",
            }],
        ]
        with patch.object(usage_cli, "run_athena_query", side_effect=query_results) as query, \
                patch.object(usage_cli, "load_csv_roster", return_value=(roster, True)), \
                patch.object(usage_cli, "load_identity_mapping", return_value={}):
            snapshot = usage_cli.collect_current_usage("2026-09", settings, aws)
        self.assertEqual("authoritative_subscription_csv", snapshot["roster_source"])
        self.assertEqual({"u1", "u2"}, {row["user_id"] for row in snapshot["rows"]})
        by_id = {row["user_id"]: row for row in snapshot["rows"]}
        self.assertEqual(Decimal(0), by_id["u1"]["credits"])
        self.assertEqual("Pro", by_id["u2"]["month_end_tier"])
        self.assertEqual("current_authoritative_snapshot", by_id["u2"]["checkpoint_subscription_tier_source"])
        self.assertIn("2026-09-09 00:00:00", query.call_args_list[1].args[1])
        self.assertFalse(any(call.args[:2] == ("s3api", "put-object") for call in aws.json.call_args_list))

    def test_collect_iic_roster_prefers_current_group_tier(self):
        aws = Mock()
        settings = {
            "database": "db", "workgroup": "wg", "data_bucket": "data",
            "subscription_csv_key": "", "application_arn": "app",
            "identity_store_id": "store", "identity_mapping_key": "mapping.csv",
        }
        usage_rows = [{
            "user_id": "u1", "credits": "50", "overage": "0",
            "first_active": "2026-09-02", "last_active": "2026-09-08",
            "active_days": "2", "latest_tier": "Pro",
        }]
        roster = {"u1": {
            "userid": "u1", "user_name": "组用户", "subscription_tier": "Pro+",
            "subscription_status": "ACTIVE", "plan_source": "IIC group",
            "subscription_tier_source": "current_iic_group",
        }}
        with patch.object(usage_cli, "run_athena_query", side_effect=[
            [{"data_cutoff": "2026-09-08"}], usage_rows,
            [{"user_id": "u1", "latest_tier": "Pro"}],
        ]) as query, patch.object(
            usage_cli, "load_csv_roster", return_value=(None, False),
        ), patch.object(
            usage_cli, "resolve_application_roster", return_value=roster,
        ) as resolve, patch.object(
            usage_cli, "load_identity_mapping", return_value={},
        ):
            snapshot = usage_cli.collect_current_usage("2026-09", settings, aws)
        row = snapshot["rows"][0]
        self.assertEqual("iam_identity_center", snapshot["roster_source"])
        self.assertEqual("Pro+", row["month_end_tier"])
        self.assertEqual(2000, row["capacity"])
        self.assertEqual("current_iic_group", row["checkpoint_subscription_tier_source"])
        self.assertIn("2026-09-09 00:00:00", query.call_args_list[2].args[1])
        self.assertEqual("Pro", resolve.call_args.args[-1]["u1"])

    def test_run_honors_tty_color_and_no_color(self):
        args = usage_cli.argparse.Namespace(
            config=Path("unused.yaml"), month="2026-09", risk=None, tier=None,
            sort=None, format="table", profile="profile", region="us-east-1",
            no_color=False,
        )
        settings = {
            "region": "us-east-1", "account_id": "123", "profile": "",
            "data_bucket": "bucket", "database": "db", "identity_store_id": "store",
            "application_arn": "app", "subscription_csv_key": "", "workgroup": "wg",
            "identity_mapping_key": "mapping.csv",
        }
        snapshot = {
            "month": "2026-09", "data_cutoff": "2026-09-08",
            "roster_source": "iam_identity_center",
            "rows": [{
                "user_name": "零用户", "email": "", "month_end_tier": "Pro",
                "credits": 0, "capacity": 1000, "last_active": "", "active_days": 0,
                "estimated_plan_cost": 20,
                "checkpoint_subscription_tier_source": "latest_at_data_cutoff",
            }],
        }
        class TtyBuffer(io.StringIO):
            def isatty(self):
                return True
        with patch.object(usage_cli, "load_project_config", return_value={}), \
                patch.object(usage_cli, "build_settings", return_value=settings), \
                patch.object(usage_cli, "current_shanghai_month", return_value="2026-09"), \
                patch.object(usage_cli, "list_aws_profiles", return_value=[]), \
                patch.object(usage_cli, "collect_current_usage", return_value=snapshot):
            colored = TtyBuffer()
            self.assertEqual(0, usage_cli.run(args, stdout=colored, aws_factory=Mock()))
            self.assertIn("\033[31m零使用\033[0m", colored.getvalue())
            args.no_color = True
            plain = TtyBuffer()
            self.assertEqual(0, usage_cli.run(args, stdout=plain, aws_factory=Mock()))
            self.assertNotIn("\033[", plain.getvalue())
            args.format = "csv"
            snapshot["data_cutoff"] = None
            snapshot["no_month_data"] = True
            csv_stdout, csv_stderr = io.StringIO(), io.StringIO()
            self.assertEqual(0, usage_cli.run(
                args, stdout=csv_stdout, stderr=csv_stderr, aws_factory=Mock(),
            ))
            self.assertTrue(csv_stdout.getvalue().startswith("user_name,email,tier"))
            self.assertNotIn("Kiro 2026-09", csv_stdout.getvalue())
            self.assertIn("Kiro 2026-09", csv_stderr.getvalue())
            self.assertIn("zero values are not confirmed non-use", csv_stderr.getvalue())
            args.format = "json"
            json_stdout, json_stderr = io.StringIO(), io.StringIO()
            self.assertEqual(0, usage_cli.run(
                args, stdout=json_stdout, stderr=json_stderr, aws_factory=Mock(),
            ))
            json_payload = json.loads(json_stdout.getvalue())
            self.assertEqual("2026-09", json_payload["month"])
            self.assertTrue(json_payload["no_month_data"])
            self.assertIn("must not be interpreted", json_payload["usage_interpretation"])
            self.assertEqual("", json_stderr.getvalue())

    def test_argument_contract(self):
        defaults = usage_cli.parse_args([])
        self.assertIsNone(defaults.sort)
        self.assertIsNone(defaults.risk)
        self.assertEqual("table", defaults.format)
        args = usage_cli.parse_args([
            "--month", "2026-09", "--risk", "all", "--tier", "Pro+",
            "--sort", "cost", "--format", "json", "--profile", "profile",
            "--region", "us-east-1", "--no-color",
        ])
        self.assertEqual("2026-09", args.month)
        self.assertEqual("all", args.risk)
        self.assertEqual("Pro+", args.tier)
        self.assertEqual("cost", args.sort)
        self.assertEqual("json", args.format)
        self.assertTrue(args.no_color)


if __name__ == "__main__":
    unittest.main()
