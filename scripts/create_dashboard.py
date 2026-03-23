#!/usr/bin/env python3
"""创建综合 Dashboard，包含多个 Sheet，覆盖两个数据集的关键图表"""
import boto3
import yaml
import sys

config = yaml.safe_load(open('config.yaml'))
qs = boto3.client('quicksight', region_name=config['aws']['region'])
aid = config['aws']['account_id']
region = config['aws']['region']
user_arn = config['quicksight']['user_arn']

ACTIVITY_DS_ARN = f"arn:aws:quicksight:{region}:{aid}:dataset/kiro-user-activity-dataset"
CREDITS_DS_ARN = f"arn:aws:quicksight:{region}:{aid}:dataset/kiro-user-credits-dataset"
SUMMARY_DS_ARN = f"arn:aws:quicksight:{region}:{aid}:dataset/kiro-user-summary-dataset"

DASHBOARD_ID = 'kiro-comprehensive-dashboard'
DASHBOARD_NAME = 'Kiro 综合仪表板'

perms = [{
    'Principal': user_arn,
    'Actions': [
        'quicksight:DescribeDashboard',
        'quicksight:ListDashboardVersions',
        'quicksight:UpdateDashboardPermissions',
        'quicksight:QueryDashboard',
        'quicksight:UpdateDashboard',
        'quicksight:DeleteDashboard',
        'quicksight:DescribeDashboardPermissions',
        'quicksight:UpdateDashboardPublishedVersion'
    ]
}]


# ============================================
# 辅助函数
# ============================================
def kpi(vid, title, ds, col, agg='SUM'):
    if agg == 'DISTINCT_COUNT':
        measure = {'CategoricalMeasureField': {
            'FieldId': vid, 'Column': {'DataSetIdentifier': ds, 'ColumnName': col},
            'AggregationFunction': 'DISTINCT_COUNT'
        }}
    else:
        measure = {'NumericalMeasureField': {
            'FieldId': vid, 'Column': {'DataSetIdentifier': ds, 'ColumnName': col},
            'AggregationFunction': {'SimpleNumericalAggregation': agg}
        }}
    return {'KPIVisual': {
        'VisualId': vid,
        'Title': {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': title}},
        'ChartConfiguration': {'FieldWells': {'Values': [measure]}}
    }}


def line(vid, title, ds, date_col, value_cols):
    """value_cols: list of (field_id, column_name, agg)"""
    values = [{'NumericalMeasureField': {
        'FieldId': fid,
        'Column': {'DataSetIdentifier': ds, 'ColumnName': col},
        'AggregationFunction': {'SimpleNumericalAggregation': agg}
    }} for fid, col, agg in value_cols]
    return {'LineChartVisual': {
        'VisualId': vid,
        'Title': {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': title}},
        'ChartConfiguration': {'FieldWells': {'LineChartAggregatedFieldWells': {
            'Category': [{'DateDimensionField': {
                'FieldId': 'date', 'Column': {'DataSetIdentifier': ds, 'ColumnName': date_col},
                'DateGranularity': 'DAY'
            }}],
            'Values': values
        }}}
    }}


def bar(vid, title, ds, cat_col, value_cols, limit=None):
    """value_cols: list of (field_id, column_name, agg)"""
    values = []
    for fid, col, agg in value_cols:
        if agg == 'DISTINCT_COUNT':
            values.append({'CategoricalMeasureField': {
                'FieldId': fid,
                'Column': {'DataSetIdentifier': ds, 'ColumnName': col},
                'AggregationFunction': 'DISTINCT_COUNT'
            }})
        else:
            values.append({'NumericalMeasureField': {
                'FieldId': fid,
                'Column': {'DataSetIdentifier': ds, 'ColumnName': col},
                'AggregationFunction': {'SimpleNumericalAggregation': agg}
            }})
    cfg = {'FieldWells': {'BarChartAggregatedFieldWells': {
        'Category': [{'CategoricalDimensionField': {
            'FieldId': cat_col,
            'Column': {'DataSetIdentifier': ds, 'ColumnName': cat_col}
        }}],
        'Values': values
    }}}
    if limit:
        cfg['SortConfiguration'] = {'CategoryItemsLimit': {'ItemsLimit': limit}}
    return {'BarChartVisual': {
        'VisualId': vid,
        'Title': {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': title}},
        'ChartConfiguration': cfg
    }}


def table(vid, title, ds, group_cols, value_cols):
    """group_cols: list of col names, value_cols: list of (fid, col, agg)"""
    groups = [{'CategoricalDimensionField': {
        'FieldId': c, 'Column': {'DataSetIdentifier': ds, 'ColumnName': c}
    }} for c in group_cols]
    values = [{'NumericalMeasureField': {
        'FieldId': fid,
        'Column': {'DataSetIdentifier': ds, 'ColumnName': col},
        'AggregationFunction': {'SimpleNumericalAggregation': agg}
    }} for fid, col, agg in value_cols]
    return {'TableVisual': {
        'VisualId': vid,
        'Title': {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': title}},
        'ChartConfiguration': {'FieldWells': {'TableAggregatedFieldWells': {
            'GroupBy': groups, 'Values': values
        }}}
    }}


# ============================================
# Dashboard Definition
# ============================================
CR = 'credits'   # credits dataset identifier
AC = 'activity'  # activity dataset identifier
SM = 'summary'   # summary dataset identifier

definition = {
    'DataSetIdentifierDeclarations': [
        {'Identifier': CR, 'DataSetArn': CREDITS_DS_ARN},
        {'Identifier': AC, 'DataSetArn': ACTIVITY_DS_ARN},
        {'Identifier': SM, 'DataSetArn': SUMMARY_DS_ARN},
    ],
    'Sheets': [
        # Sheet 1: 概览 (credits dataset)
        {
            'SheetId': 'sheet-overview',
            'Name': '概览',
            'Visuals': [
                kpi('d-kpi-users', '活跃用户数', CR, 'userid', 'DISTINCT_COUNT'),
                kpi('d-kpi-credits', '总 Credit 消耗', CR, 'credits_used', 'SUM'),
                kpi('d-kpi-overage', '超额 Credit', CR, 'overage_credits_used', 'SUM'),
                kpi('d-kpi-messages', '总消息数', CR, 'total_messages', 'SUM'),
                line('d-line-credits', '每日 Credit 消耗趋势', CR, 'date',
                     [('cr_used', 'credits_used', 'SUM'),
                      ('cr_over', 'overage_credits_used', 'SUM')]),
                bar('d-bar-top-credits', 'Top 10 Credit 消耗用户', CR, 'username',
                    [('cr_sum', 'credits_used', 'SUM')], limit=10),
                bar('d-bar-tier', '各订阅层级用户数', CR, 'subscription_tier',
                    [('tier_users', 'userid', 'DISTINCT_COUNT')]),
            ]
        },
        # Sheet 2: 用户行为 (activity dataset)
        {
            'SheetId': 'sheet-behavior',
            'Name': '用户行为',
            'Visuals': [
                kpi('d-kpi-codelines', '总 AI 代码行数', AC, 'chat_aicodelines', 'SUM'),
                kpi('d-kpi-inline', '总 Inline 代码行数', AC, 'inline_aicodelines', 'SUM'),
                kpi('d-kpi-chat', '总 Chat 消息数', AC, 'chat_messagessent', 'SUM'),
                line('d-line-code', '每日 AI 代码生成趋势', AC, 'date',
                     [('chat_cl', 'chat_aicodelines', 'SUM'),
                      ('inline_cl', 'inline_aicodelines', 'SUM')]),
                line('d-line-accept', 'Inline 代码接受趋势', AC, 'date',
                     [('accepted', 'inline_acceptancecount', 'SUM'),
                      ('suggested', 'inline_suggestionscount', 'SUM')]),
                bar('d-bar-top-code', 'Top 10 代码生成用户', AC, 'username',
                    [('u_chat', 'chat_aicodelines', 'SUM'),
                     ('u_inline', 'inline_aicodelines', 'SUM')], limit=10),
            ]
        },
        # Sheet 3: 成本分析 (credits dataset)
        {
            'SheetId': 'sheet-cost',
            'Name': '成本分析',
            'Visuals': [
                line('d-line-overage', '每日超额趋势', CR, 'date',
                     [('ov_sum', 'overage_credits_used', 'SUM')]),
                # 每日每用户 Credit 消耗趋势（按用户分色）
                {'LineChartVisual': {
                    'VisualId': 'd-line-user-credits',
                    'Title': {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': '每日每用户 Credit 消耗趋势'}},
                    'ChartConfiguration': {'FieldWells': {'LineChartAggregatedFieldWells': {
                        'Category': [{'DateDimensionField': {
                            'FieldId': 'uc_date',
                            'Column': {'DataSetIdentifier': CR, 'ColumnName': 'date'},
                            'DateGranularity': 'DAY'
                        }}],
                        'Values': [{'NumericalMeasureField': {
                            'FieldId': 'uc_credits',
                            'Column': {'DataSetIdentifier': CR, 'ColumnName': 'credits_used'},
                            'AggregationFunction': {'SimpleNumericalAggregation': 'SUM'}
                        }}],
                        'Colors': [{'CategoricalDimensionField': {
                            'FieldId': 'uc_user',
                            'Column': {'DataSetIdentifier': CR, 'ColumnName': 'username'}
                        }}]
                    }}}
                }},
                bar('d-bar-tier-cost', '各层级平均 Credit 消耗', CR, 'subscription_tier',
                    [('avg_cr', 'credits_used', 'AVERAGE'),
                     ('avg_cap', 'overage_cap', 'AVERAGE')]),
                table('d-table-cost', '用户 Credit 使用明细', CR,
                      ['username', 'subscription_tier', 'client_type'],
                      [('t_credits', 'credits_used', 'SUM'),
                       ('t_overage', 'overage_credits_used', 'SUM'),
                       ('t_cap', 'overage_cap', 'MAX'),
                       ('t_msgs', 'total_messages', 'SUM')]),
            ]
        },
        # Sheet 4: 用户概况 (summary dataset)
        {
            'SheetId': 'sheet-user-summary',
            'Name': '用户概况',
            'Visuals': [
                kpi('d-kpi-total-users', '总用户数', SM, 'username', 'DISTINCT_COUNT'),
                kpi('d-kpi-active-users', '活跃用户数', SM, 'is_active', 'SUM'),
                # 每月用户 Credit 消耗柱状图（按用户分色）
                {'BarChartVisual': {
                    'VisualId': 'd-bar-monthly-credits',
                    'Title': {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': '每月用户 Credit 消耗'}},
                    'ChartConfiguration': {'FieldWells': {'BarChartAggregatedFieldWells': {
                        'Category': [{'CategoricalDimensionField': {
                            'FieldId': 'sm_month', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'month'}
                        }}],
                        'Values': [{'NumericalMeasureField': {
                            'FieldId': 'sm_credits', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'total_credits'},
                            'AggregationFunction': {'SimpleNumericalAggregation': 'SUM'}
                        }}],
                        'Colors': [{'CategoricalDimensionField': {
                            'FieldId': 'sm_user', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'username'}
                        }}]
                    }},
                    'Orientation': 'HORIZONTAL',
                    'SortConfiguration': {'CategorySort': [
                        {'FieldSort': {'FieldId': 'sm_credits', 'Direction': 'DESC'}}
                    ], 'ColorItemsLimit': {'ItemsLimit': 57, 'OtherCategories': 'EXCLUDE'}}}
                }},
                # 每月活跃天数柱状图（按用户分色）
                {'BarChartVisual': {
                    'VisualId': 'd-bar-monthly-days',
                    'Title': {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': '每月用户活跃天数'}},
                    'ChartConfiguration': {'FieldWells': {'BarChartAggregatedFieldWells': {
                        'Category': [{'CategoricalDimensionField': {
                            'FieldId': 'sd_month', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'month'}
                        }}],
                        'Values': [{'NumericalMeasureField': {
                            'FieldId': 'sd_days', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'active_days'},
                            'AggregationFunction': {'SimpleNumericalAggregation': 'SUM'}
                        }}],
                        'Colors': [{'CategoricalDimensionField': {
                            'FieldId': 'sd_user', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'username'}
                        }}]
                    }},
                    'Orientation': 'HORIZONTAL',
                    'SortConfiguration': {'CategorySort': [
                        {'FieldSort': {'FieldId': 'sd_days', 'Direction': 'DESC'}}
                    ], 'ColorItemsLimit': {'ItemsLimit': 57, 'OtherCategories': 'EXCLUDE'}}}
                }},
                # 用户月度明细表
                {'TableVisual': {
                    'VisualId': 'd-table-user-summary',
                    'Title': {'Visibility': 'VISIBLE', 'FormatText': {'PlainText': '用户月度概况'}},
                    'ChartConfiguration': {
                        'FieldWells': {'TableAggregatedFieldWells': {
                            'GroupBy': [
                                {'CategoricalDimensionField': {'FieldId': 's_month', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'month'}}},
                                {'CategoricalDimensionField': {'FieldId': 's_level', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'activity_level'}}},
                                {'CategoricalDimensionField': {'FieldId': 's_name', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'username'}}},
                                {'CategoricalDimensionField': {'FieldId': 's_tier', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'tier_history'}}},
                                {'CategoricalDimensionField': {'FieldId': 's_client', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'client_types'}}},
                            ],
                            'Values': [
                                {'NumericalMeasureField': {'FieldId': 's_credits', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'total_credits'}, 'AggregationFunction': {'SimpleNumericalAggregation': 'SUM'}}},
                                {'NumericalMeasureField': {'FieldId': 's_pct', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'usage_pct'}, 'AggregationFunction': {'SimpleNumericalAggregation': 'MAX'}}},
                                {'NumericalMeasureField': {'FieldId': 's_msgs', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'total_messages'}, 'AggregationFunction': {'SimpleNumericalAggregation': 'SUM'}}},
                                {'NumericalMeasureField': {'FieldId': 's_days', 'Column': {'DataSetIdentifier': SM, 'ColumnName': 'active_days'}, 'AggregationFunction': {'SimpleNumericalAggregation': 'SUM'}}},
                            ]
                        }},
                        'SortConfiguration': {'RowSort': [
                            {'FieldSort': {'FieldId': 's_month', 'Direction': 'DESC'}},
                            {'FieldSort': {'FieldId': 's_credits', 'Direction': 'DESC'}},
                        ]},
                    }
                }},
            ]
        },
    ]
}


# ============================================
# 创建或更新 Dashboard
# ============================================
print("创建综合仪表板...")

try:
    qs.create_dashboard(
        AwsAccountId=aid,
        DashboardId=DASHBOARD_ID,
        Name=DASHBOARD_NAME,
        Permissions=perms,
        Definition=definition
    )
    print(f"✓ Dashboard 创建成功: {DASHBOARD_NAME}")
except qs.exceptions.ResourceExistsException:
    print("  Dashboard 已存在，更新中...")
    qs.update_dashboard(
        AwsAccountId=aid,
        DashboardId=DASHBOARD_ID,
        Name=DASHBOARD_NAME,
        Definition=definition
    )
    print(f"✓ Dashboard 更新成功: {DASHBOARD_NAME}")

# 发布最新版本
import time
time.sleep(3)
try:
    versions = qs.list_dashboard_versions(AwsAccountId=aid, DashboardId=DASHBOARD_ID)
    latest = max(v['VersionNumber'] for v in versions['DashboardVersionSummaryList'])
    qs.update_dashboard_published_version(
        AwsAccountId=aid,
        DashboardId=DASHBOARD_ID,
        VersionNumber=latest
    )
    print(f"✓ 已发布版本 {latest}")
except Exception as e:
    print(f"  发布版本跳过: {e}")

# ============================================
# 创建或更新 Analysis（用于在控制台可视化编辑）
# ============================================
ANALYSIS_ID = 'kiro-comprehensive-analysis'
ANALYSIS_NAME = 'Kiro 综合分析'

analysis_perms = [{
    'Principal': user_arn,
    'Actions': [
        'quicksight:RestoreAnalysis', 'quicksight:UpdateAnalysisPermissions',
        'quicksight:DeleteAnalysis', 'quicksight:DescribeAnalysisPermissions',
        'quicksight:QueryAnalysis', 'quicksight:DescribeAnalysis',
        'quicksight:UpdateAnalysis'
    ]
}]

try:
    qs.create_analysis(
        AwsAccountId=aid, AnalysisId=ANALYSIS_ID, Name=ANALYSIS_NAME,
        Definition=definition, Permissions=analysis_perms
    )
    print(f"✓ Analysis 创建成功: {ANALYSIS_NAME}")
except qs.exceptions.ResourceExistsException:
    qs.update_analysis(
        AwsAccountId=aid, AnalysisId=ANALYSIS_ID, Name=ANALYSIS_NAME,
        Definition=definition
    )
    print(f"✓ Analysis 已更新: {ANALYSIS_NAME}")

print(f"\n✅ 综合仪表板部署完成！")
print(f"访问: https://{region}.quicksight.aws.amazon.com/sn/dashboards/{DASHBOARD_ID}")
