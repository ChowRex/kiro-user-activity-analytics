# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

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
