# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [1.3.0] - 2026-03-23

### Added
- **表格序号列**：用户概况表和成本分析表均新增 `row_num` 排名序号，方便快速识别活跃用户数量
- **Athena 视图 `credit_summary`**：Credit 汇总视图，按消耗降序排名，含序号
- **QuickSight 数据集 `kiro-credit-summary-dataset`**：基于 credit_summary 视图，SPICE 模式，每日自动刷新
- **Lambda 代码自动同步**：deploy.sh 步骤 1 在 CloudFormation 部署后，自动从 `cloudformation.yaml` 提取 Lambda inline code 并通过 `update-function-code` 强制更新，解决 CloudFormation 不检测 inline code 变更的问题

### Changed
- **用户概况 Sheet 改进**：
  - 视图 `user_summary` 改为只查询当前自然月数据（不再包含历史月份）
  - KPI 改为"总用户数"和"活跃用户数"（通过 `is_active` 字段 SUM 实现）
  - 柱状图改为按用户分组降序排列（不再按月分色），一眼可见消耗/活跃排名
  - 明细表增加容量（capacity）、使用率（usage_pct）、活跃度（activity_level）字段
- **活跃度按订阅容量比例计算**：PRO=1000, PRO_PLUS=2000, POWER=10000，根据 usage_pct 划分 6 个等级
- **用户映射同步**：`sync_user_mapping.py` 和 Lambda 改为拉取 Identity Center 全部用户，不再仅拉取有使用记录的用户
- **成本分析表格**：改用 `credit_summary` 数据集，含排名序号
- **deploy.sh 步骤 6/7 拆分**：`--from-step 7` 现在可以单独更新 Dashboard

### Fixed
- README 中 3 处 `create_dashboard_publish.py` 修正为 `create_dashboard.py`

## [1.2.0] - 2026-03-23

### Added
- **用户概况 Sheet**：新增 Dashboard 第 4 个 Tab，按自然月展示用户使用情况
  - 每月用户 Credit 消耗柱状图（按用户分色）
  - 每月用户活跃天数柱状图（按用户分色）
  - 用户月度概况表：含层级变化追踪（如 `PRO → PRO_PLUS`）、客户端类型、活跃天数等
- **Athena 视图 `user_summary`**：按自然月聚合用户数据，支持层级变化追踪
- **QuickSight 数据集 `kiro-user-summary-dataset`**：基于 user_summary 视图，SPICE 模式，每日自动刷新
- deploy.sh 步骤 5.5 自动创建 user_summary 视图并授权 Lake Formation 权限

## [1.1.1] - 2026-03-23

### Fixed
- **Lambda 用户名映射再次出现 null**：修复 Lambda 函数两个遗漏的 bug
  - `get_name()` 缺少 `\r` 清理：Identity Center API 返回的 DisplayName 包含 `\r`，导致 CSV 中用户名带不可见字符，OpenCSVSerde 解析后 LEFT JOIN 匹配失败
  - `csv.writer` 未指定 `lineterminator='\n'`：默认使用 `\r\n`，进一步导致解析异常
  - 根本原因：上次部署时本地脚本（已修复）生成了干净的映射文件，但 Lambda 每天自动运行时用未修复的代码覆盖了好的文件
  - 影响文件：`infrastructure/cloudformation.yaml`

## [1.1.0] - 2026-03-13

### Fixed
- **用户名映射问题**：修复 Dashboard 中部分用户显示为 null 的问题
  - AWS 在 2026-03-10 改变了 `user_report` 表的 userid 格式，从纯 UUID 变为带 Identity Store ID 前缀（如 `d-90661af5ec.{uuid}`）
  - 原有的 `user_mapping` 表只包含纯 UUID，导致新格式无法 JOIN 匹配
  - 修复后为每个用户生成两种格式的映射记录（纯 UUID + 带前缀），确保新旧格式都能正确匹配
  - 影响文件：
    - `scripts/sync_user_mapping.py` - 本地同步脚本
    - `infrastructure/cloudformation.yaml` - Lambda 函数代码

### Changed
- 用户映射表现在包含双倍记录（每个用户 2 条：纯 UUID + 带前缀）
- Athena 查询时提取纯 UUID，但映射表支持两种格式以兼容 JOIN

## [1.0.0] - 2026-03-11

### Added
- 初始版本发布
- 完整的 Kiro User Activity Analytics 数据分析平台
- 支持 by_user_analytic（行为明细）和 user_report（Credit 汇总）两种数据源
- QuickSight 综合仪表板（概览、用户行为、成本分析）
- 自动化用户名映射（Lambda + EventBridge 定时同步）
- SPICE 模式数据集，每日自动刷新

### Fixed
- 修复 Identity Center API 返回的用户名中包含 `\r` 换行符的问题
- 修复 QuickSight 趋势图不显示的问题（date 字段类型转换）
- 添加数据过滤，排除 2026-02-10 之前的异常数据
