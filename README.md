# Kiro User Activity Analytics

Kiro 企业版用户活动数据分析平台。自动采集 S3 中的用户报告数据，通过 Athena 外部表构建数据湖，在 QuickSight (SPICE 模式) 中展示综合仪表板，帮助管理员了解团队的 Kiro 使用情况和 Credit 消耗。

## 系统架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Kiro Enterprise                                │
│                    (User Activity Report 功能)                          │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │ 每日自动投递 CSV
                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  S3 Bucket                                                              │
│  s3://<bucket>/<prefix>/AWSLogs/<account>/KiroLogs/           │
│  ├── by_user_analytic/   每日用户行为明细 (46 列)                         │
│  │   └── <region>/<year>/<month>/<day>/00/*.csv                         │
│  ├── user_report/        每日用户 Credit 汇总 (11 列)                    │
│  │   └── <region>/<year>/<month>/<day>/00/*.csv                         │
│  └── user-mapping/       用户名映射 (Lambda 生成)                        │
│      └── user_mapping.csv                                               │
└──────────┬──────────────────────────────────────────────────────────────┘
           │                                  │
           ▼                                  ▼
┌─────────────────────┐            ┌─────────────────────────┐
│  Glue API 建表       │            │  Lambda Function        │
│  (部署时一次性执行)   │            │  每天 UTC 3:00          │
│  ├─ by_user_analytic │            │  查询 Athena userid     │
│  └─ user_report      │            │  → Identity Center API  │
│                      │            │  → 生成 user_mapping.csv│
└────────┬─────────────┘            └────────┬────────────────┘
         │                                   │
         ▼                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Glue Data Catalog (kiro_analytics)                                     │
│  ├── by_user_analytic   行为明细表 (Glue API 建表，schema 固定)           │
│  ├── user_report        Credit 汇总表 (Glue API 建表，schema 固定)       │
│  └── user_mapping       用户名映射表 (脚本管理，schema 可扩展)            │
└──────────┬──────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  QuickSight (SPICE 模式)                                                │
│  ├── Data Source: Athena 连接                                           │
│  ├── Datasets x2 (SPICE，每日 UTC 04:00 自动刷新):                      │
│  │   ├── activity dataset (by_user_analytic LEFT JOIN user_mapping)     │
│  │   └── credits dataset  (user_report LEFT JOIN user_mapping)          │
│  ├── Analysis: Kiro 综合分析 (可在控制台编辑)                             │
│  └── Dashboard: Kiro 综合仪表板 (只读发布版)                              │
│      ├── Sheet 1: 概览                                                  │
│      ├── Sheet 2: 用户行为                                              │
│      └── Sheet 3: 成本分析                                              │
└─────────────────────────────────────────────────────────────────────────┘
```

## 仪表板内容

综合仪表板包含 3 个 Sheet：

| Sheet | 数据集 | 包含图表 |
|-------|--------|---------|
| 概览 | credits | 活跃用户数 KPI、Credit 消耗 KPI、超额 Credit KPI、总消息数 KPI、每日 Credit 趋势折线图、Top 10 用户柱状图、订阅层级分布 |
| 用户行为 | activity | AI 代码行数 KPI、Inline 代码行数 KPI、Chat 消息数 KPI、代码生成趋势折线图、Inline 接受趋势折线图、Top 10 代码用户柱状图 |
| 成本分析 | credits | 每日超额趋势折线图、各层级平均消耗柱状图、用户 Credit 使用明细表 |


## 前置条件

### 1. 开启 Kiro User Activity Report

在 AWS 管理控制台中开启 Kiro 的用户活动报告功能：

1. 登录 [AWS Console](https://console.aws.amazon.com/)
2. 进入 **Kiro** (原 Amazon Q Developer) 服务页面
3. 在左侧导航栏选择 **Settings** → **User activity report**
4. 点击 **Enable** 开启报告
5. 配置 S3 存储桶：
   - 选择一个已有的 S3 桶，或创建新桶
   - 记录桶名称（如 `kiro-user-reports-xxxxxxxx`）
   - 报告会自动投递到 `s3://<bucket>/<prefix>/AWSLogs/<account_id>/KiroLogs/` 路径下
   - 确认 S3 桶策略包含 Kiro 服务写入权限：
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Sid": "KiroLogsWrite",
           "Effect": "Allow",
           "Principal": {
             "Service": "q.amazonaws.com"
           },
           "Action": "s3:PutObject",
           "Resource": "arn:aws:s3:::<bucket-name>/<prefix>/*",
           "Condition": {
             "StringEquals": {
               "aws:SourceAccount": "<account-id>"
             },
             "ArnLike": {
               "aws:SourceArn": "arn:aws:codewhisperer:<region>:<account-id>:*"
             }
           }
         }
       ]
     }
     ```
6. 等待至少 1-2 天，确认 S3 中有数据生成

> **注意**: 报告有 1-2 天的延迟。开启后第二天才会看到第一份报告。

### 2. 其他前置条件

- **AWS CLI** 已安装并配置，当前用户有管理员权限
- **Python 3.9+** 已安装
- **QuickSight Enterprise** 已在当前 Region 启用
- **QuickSight S3 权限（重要！！）**）: 在 QuickSight Console → 右上角头像 → Manage Quick → Permissions → AWS resources → 勾选 Amazon S3 → 点击 Select S3 buckets，勾选报告所在的 S3 bucket，并启用 "Write permission for Athena Workgroup"
- **IAM Identity Center** 已配置（用于将 userid 映射为可读的用户名）
- **Lake Formation（重要！！）**: 当前用户需要是 Data Lake Admin（部署脚本会自动配置表权限）

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml`，填写你的环境信息（详见下方配置说明）。

### 3. 一键部署

```bash
# 如果你有多个AWS config, 需要指定具体的profile名称
# export AWS_PROFILE="Your-profile-name"
export AWS_PROFILE=${AWS_PROFILE:-default}

chmod +x deploy.sh
./deploy.sh
```

部署完成后，访问 QuickSight 控制台即可查看仪表板。

## 配置文件说明

`config.yaml` 包含所有部署所需的配置项：

```yaml
# AWS 基础配置
aws:
  region: us-east-1              # AWS Region（必须与 Kiro 和 QuickSight 在同一 Region）
  account_id: "123456789012"     # 你的 AWS 账户 ID（12 位数字，用引号包裹）

# S3 数据源配置
s3:
  bucket_name: "q-developer-reports-xxxxxxxx"  # Kiro User Activity Report 投递的 S3 桶名
  prefix: "amazon-q-developer/"                # S3 前缀（通常不需要修改）

# Glue 配置（通常不需要修改）
glue:
  database_name: "kiro_analytics"    # Glue 数据库名称

# IAM Identity Center 配置
identity_center:
  identity_store_id: "d-xxxxxxxxxx"  # Identity Store ID
                                      # 获取方式: AWS Console → IAM Identity Center → Settings
                                      # 或: aws sso-admin list-instances

# QuickSight 配置
quicksight:
  user_arn: "arn:aws:quicksight:us-east-1:123456789012:user/default/role_name/username"
    # QuickSight 用户 ARN，用于授权访问数据源、数据集和仪表板
    # 获取方式: aws quicksight list-users --aws-account-id <account_id> --namespace default
    # 如果通过 IAM 角色登录 QuickSight，格式为:
    #   arn:aws:quicksight:<region>:<account>:user/default/<role_name>/<username>
  data_source_name: "KiroUserActivity"       # QuickSight 数据源显示名称
  dataset_name: "KiroUserActivityDataset"    # QuickSight 数据集显示名称
```

### 如何获取关键配置值

| 配置项 | 获取方式 |
|--------|---------|
| `aws.account_id` | `aws sts get-caller-identity --query Account --output text` |
| `s3.bucket_name` | Kiro 控制台 → Settings → User activity report 中查看 |
| `identity_center.identity_store_id` | IAM Identity Center 控制台 → Settings → Identity store ID |
| `quicksight.user_arn` | `aws quicksight list-users --aws-account-id <ACCOUNT_ID> --namespace default` |

## 部署流程详解

`deploy.sh` 是端到端部署脚本，按顺序执行以下步骤：

| 步骤 | 说明 | 对应脚本/资源 |
|------|------|--------------|
| 1️⃣ | 部署 CloudFormation 基础设施 | `infrastructure/cloudformation.yaml` |
| 2️⃣ | 配置 Lake Formation 权限 | deploy.sh 内置 |
| 3️⃣ | 通过 Glue API 创建外部表 | deploy.sh 内置 |
| 4️⃣ | 验证 Athena 数据查询 | Athena |
| 5️⃣ | 同步用户名映射 | `scripts/sync_user_mapping.py` |
| 6️⃣ | 部署 QuickSight 数据源和数据集 (SPICE) | `scripts/create_datasets.py` |
| 7️⃣ | 发布综合仪表板和分析 | `scripts/create_dashboard_publish.py` |

### Lake Formation 权限

项目自动为以下 Principal 配置 Lake Formation 权限：

| Principal | 权限 | 用途 |
|-----------|------|------|
| 当前 IAM 用户/角色 | CREATE_TABLE, ALTER, SELECT, DESCRIBE | DDL 建表 + Athena 手动查询 |
| QuickSight Service Role | SELECT, DESCRIBE | QuickSight 读取数据 |
| QuickSight 用户 IAM 角色 | SELECT, DESCRIBE | QuickSight 用户访问 |
| Lambda Role | SELECT, DESCRIBE, ALTER, CREATE_TABLE | 用户映射同步 |
| IAMAllowedPrincipals | ALL | 兼容 IAM 模式访问 |


## 项目结构

```
kiro-user-activity-analytics/
├── config.yaml                      # 项目配置（包含账户信息，不提交 Git）
├── config.example.yaml              # 配置模板
├── deploy.sh                        # 端到端部署脚本
├── requirements.txt                 # Python 依赖
├── infrastructure/
│   └── cloudformation.yaml          # AWS 基础设施定义
│                                    #   - Glue Database
│                                    #   - Athena Workgroup
│                                    #   - Lambda 用户映射同步函数
│                                    #   - EventBridge 定时规则
└── scripts/
    ├── sync_user_mapping.py         # 同步 userid → 用户名映射
    ├── create_datasets.py           # 创建 QuickSight 数据源和数据集 (SPICE)
    └── create_dashboard_publish.py  # 创建并发布综合仪表板和分析
```

## 数据源说明

### by_user_analytic（行为明细）

每日每用户的详细使用数据，按 `client_type`（KIRO_CLI / KIRO_IDE）分别生成 CSV。

主要字段：
- `date` / `userid` — 日期和用户 ID
- `chat_*` — Chat 功能：AI 代码行数、消息数、交互数
- `inline_*` — Inline 补全：代码行数、建议数、接受数
- `codefix_*` — 代码修复：生成次数、接受次数
- `codereview_*` — 代码审查：发现数、成功次数
- `dev_*` — Dev Agent：生成次数、接受次数、生成行数
- `testgeneration_*` — 测试生成：次数、接受的测试数
- `inlinechat_*` — Inline Chat：总次数、接受次数
- `docgeneration_*` — 文档生成：次数、接受的文件数
- `transformation_*` — 代码转换：次数、生成行数

### user_report（Credit 汇总）

每日每用户的订阅和消费数据，同样按 `client_type` 分别生成。

| 字段 | 说明 |
|------|------|
| `date` | 报告日期 |
| `userid` | IAM Identity Center 用户 ID |
| `client_type` | 客户端类型（KIRO_CLI / KIRO_IDE） |
| `subscription_tier` | 订阅层级（PRO / PRO_PLUS） |
| `credits_used` | 当日 Credit 消耗量 |
| `overage_cap` | 超额上限 |
| `overage_credits_used` | 超额 Credit 消耗 |
| `overage_enabled` | 是否启用超额 |
| `total_messages` | 当日总消息数 |
| `chat_conversations` | 当日 Chat 会话数 |
| `profileid` | Kiro Profile ARN |

> **注意**: `user_report` 功能从 2026-02-10 开始提供数据。早期 PRO 层级的 credit 数值可能异常偏大，升级到 PRO_PLUS 后数据正常。报告有 1-2 天延迟。

## 用户名映射机制

S3 报告中的 `userid` 是 IAM Identity Center 的 UUID（如 `24681498-20e1-7057-3818-19d6b7a2f397`），不便于识别。项目通过以下机制自动映射为可读的用户名：

1. **Lambda 函数** (`kiro-user-mapping-sync`) 每天 UTC 3:00 自动运行
2. 从 Athena 查询所有不重复的 `userid`
3. 调用 IAM Identity Center `DescribeUser` API 获取 `DisplayName`
4. 生成映射 CSV 上传到 `s3://<bucket>/user-mapping/user_mapping.csv`
5. 创建/更新 Glue 外部表 `user_mapping`
6. QuickSight 数据集通过 `LEFT JOIN` 关联映射表，图表中直接显示用户名

手动触发同步：
```bash
# 本地运行
python3 scripts/sync_user_mapping.py

# 或通过 Lambda
aws lambda invoke --function-name kiro-user-mapping-sync /tmp/out.json && cat /tmp/out.json
```

## 常用操作

### 仅更新仪表板（不重建基础设施）

```bash
python3 scripts/create_datasets.py
python3 scripts/create_dashboard_publish.py
```

### 手动触发 SPICE 数据刷新

SPICE 数据集每日 UTC 04:00 自动刷新。如需手动刷新：

```bash
# 通过 QuickSight 控制台: Datasets → 选择数据集 → Refresh now
# 或通过 CLI:
aws quicksight create-ingestion \
    --aws-account-id <ACCOUNT_ID> \
    --data-set-id kiro-user-activity-dataset \
    --ingestion-id manual-$(date +%s)
```

### 完全重新部署

```bash
aws cloudformation delete-stack --stack-name kiro-analytics-stack --region us-east-1
aws cloudformation wait stack-delete-complete --stack-name kiro-analytics-stack --region us-east-1
./deploy.sh
```

### Athena 手动查询示例

```sql
-- 查看最近 7 天的 Credit 消耗
SELECT date, userid, client_type, CAST(credits_used AS decimal) as credits_used
FROM kiro_analytics.user_report
WHERE date >= date_format(date_add('day', -7, current_date), '%Y-%m-%d')
ORDER BY date DESC;

-- 查看每个用户的总代码生成量（OpenCSVSerde 列为 STRING，需要 CAST）
SELECT userid, 
       SUM(CAST(chat_aicodelines AS bigint)) as chat_code,
       SUM(CAST(inline_aicodelines AS bigint)) as inline_code
FROM kiro_analytics.by_user_analytic
GROUP BY userid;
```

## 故障排查

| 问题 | 解决方案 |
|------|---------|
| Athena 查询报 `AccessDeniedException` | 检查 Lake Formation 权限，重新运行 `sh deploy.sh --from-step 3`（建表后会自动重新授权） |
| Athena 查询 `SUM()` 报 `FUNCTION_NOT_FOUND` | OpenCSVSerde 所有列为 STRING，手动查询需要 CAST：`SUM(CAST(col AS bigint))`。QuickSight 数据集已通过 CastColumnTypeOperation 自动转换，不受影响 |
| Glue API 建表失败 | 确认 S3 路径正确，检查 S3 桶策略是否允许访问 |
| QuickSight 报 `SQL exception` / `TABLE_NOT_FOUND` | 1. 确认 Athena 表存在：`aws glue get-table --database-name kiro_analytics --name by_user_analytic`<br>2. 确认 QuickSight 已授权 S3（Manage QuickSight → Security & permissions → S3）<br>3. 确认当前用户是 Lake Formation Data Lake Admin<br>4. 重跑 `sh deploy.sh --from-step 3` |
| QuickSight 数据源 `CREATION_FAILED` | S3 权限问题。先在 QuickSight Console → Manage QuickSight → Security & permissions → S3 中授权 bucket，然后重跑 `sh deploy.sh --from-step 6`（脚本会自动删除失败的数据源并重建） |
| SPICE 刷新失败 | 检查 Athena 表是否可查询，确认 Lake Formation 表权限已授予 QuickSight Service Role。如果是删表重建导致权限丢失，重跑 `sh deploy.sh --from-step 3` |
| Dashboard 数值显示为 0 或无数据 | 可能是 Glue 表列顺序与 CSV header 不匹配。用 `SELECT * FROM kiro_analytics.user_report LIMIT 1` 验证列值是否合理，如不对需要修正表定义并重跑 `sh deploy.sh --from-step 3` |
| 用户名显示为 UUID | 运行 `python3 scripts/sync_user_mapping.py` 手动同步映射。如果 Identity Center 重建过用户目录，历史 userid 将无法解析 |
| 仪表板图表为空 | 1. 检查 Athena 表有数据：`SELECT COUNT(*) FROM kiro_analytics.user_report`<br>2. 检查 SPICE 导入状态：QuickSight Console → Datasets → 查看最近导入<br>3. 手动触发 SPICE 刷新 |
| S3 没有新数据 | 报告有 1-2 天延迟，确认 Kiro User Activity Report 已开启 |
| CloudFormation 部署报 `ROLLBACK_COMPLETE` | deploy.sh 会自动处理，删除旧 stack 后重建 |
| Lake Formation 权限丢失（删表重建后） | deploy.sh 步骤 3 建表后会自动重新授权所有 principal 的表级别权限，无需手动处理 |

## 成本估算

本方案使用的 AWS 服务均为按量付费或有免费额度，适合中小团队低成本运行。

### 各服务费用（us-east-1 区域）

| 服务 | 计费项 | 预估费用（50 用户/月） | 说明 |
|------|--------|----------------------|------|
| S3 | 存储 + 请求 | < $0.10 | Kiro 报告 CSV 文件很小，每用户每天 ~1KB |
| Glue Data Catalog | 表存储 | $0 | 前 100 万个对象免费 |
| Athena | 查询扫描量 | < $0.50 | 按扫描数据量计费（$5/TB），CSV 数据量极小；SPICE 模式下日常仅刷新时查询 |
| Lambda | 调用 + 执行时间 | $0 | 每天 1 次调用，远低于免费额度（100 万次/月） |
| EventBridge | 定时规则 | $0 | 免费 |
| Lake Formation | 权限管理 | $0 | 免费 |
| SPICE 额外容量 | 超出免费额度部分 | $0.38/GB/月 | 每个 Author 含 10GB 免费，本方案数据量远低于此 |

### QuickSight / Quick Suite 用户定价

AWS 提供两种订阅方式，根据账号所在区域和需求选择：

| 方案 | 角色 | 价格 | 说明 |
|------|------|------|------|
| **Quick Suite**（推荐） | Professional | $20/用户/月 | 包含 Quick Sight + Quick Research + Quick Flows，功能最全且比单独 Author 更便宜 |
| Quick Suite | Enterprise | $35/用户/月 | 在 Professional 基础上增加自动化工作流等高级功能 |
| **Quick Sight 仅 BI** | Author | $24/用户/月 | 创建/编辑仪表板，含 10GB SPICE |
| Quick Sight 仅 BI | Author Pro | $40/用户/月 | Author + AI 生成式分析（需额外 $250/月账户基础设施费） |
| Quick Sight 仅 BI | Reader | $3/用户/月 | 只读查看、筛选、下载 |
| Quick Sight 仅 BI | Reader Pro | $20/用户/月 | Reader + AI 摘要和场景分析 |

> Quick Suite 在 us-east-1 等区域可用。如果你的区域支持 Quick Suite，Professional $20/月比单独买 Quick Sight Author $24/月更划算。

### 典型场景月费估算

| 场景 | 用户配置 | Quick Suite 方案 | Quick Sight 仅 BI 方案 |
|------|---------|-----------------|----------------------|
| 个人/小团队（1 管理员） | 1 Author | $20 | $24 |
| 中型团队（1 管理员 + 5 只读） | 1 Author + 5 Reader | $20 + $0* | $24 + $15 = $39 |
| 大型团队（2 管理员 + 20 只读） | 2 Author + 20 Reader | $40 + $0* | $48 + $60 = $108 |

> \* Quick Suite Professional 用户可以查看仪表板，不需要额外的 Reader 费用。
>
> 费用主要来自用户订阅。其他服务（S3、Athena、Lambda、Glue）在本方案的数据规模下费用可忽略不计。
>
> 定价参考：[Quick Suite Pricing](https://aws.amazon.com/quick/pricing/)、[QuickSight BI-only Pricing](https://aws.amazon.com/quick/quicksight/pricing/)、[Athena Pricing](https://aws.amazon.com/athena/pricing/)

## License

This project is licensed under the [MIT License](LICENSE).
