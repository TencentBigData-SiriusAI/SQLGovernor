-- ============================================================================
-- Case 03: SELECT * 无 LIMIT，全量拉取大表数据
-- ============================================================================
-- 【问题描述】
--   使用 SELECT * 且不带 LIMIT 和分区过滤，会拉取全表所有字段所有行。
--   对于 task 级别的表（每个 app 可能有数万条 task 记录），数据量极其庞大。
--
-- 【易犯场景】
--   1. 开发调试阶段想"看看数据长什么样"，写了 SELECT * 但忘加 LIMIT
--   2. 将临时查询 SQL 误提交到生产调度，未经审查
--   3. ETL 中间过程想"先拿全量再在代码里过滤"
--   4. BI 报表直接引用 SELECT * 的视图
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：
--   - SELECT * 建议指定需要的列
--   - 缺少分区过滤条件
--   - 缺少 LIMIT 限制
-- ============================================================================

-- Case 3a: SELECT * 无任何限定
-- task 表有 47 个字段，且数据量最大（一个 app 可有数万 task）
-- 无分区 + 无 LIMIT = 灾难性全表扫描
SELECT *
FROM spark_analytics.spark_task_metrics;


-- Case 3b: SELECT * 带了业务过滤但没带分区过滤
-- 看似有条件约束，但缺少分区条件仍然是全分区扫描
-- 且 SELECT * 拉取了 task 表全部 47 个字段，网络传输开销巨大
SELECT *
FROM spark_analytics.spark_task_metrics
WHERE status = -1
  AND gc_time > 60000;


-- Case 3c: SELECT * 带了分区过滤但无 LIMIT
-- 即使指定了单日分区，task 表单日数据量也可能是千万级
-- SELECT * 拉取全部字段无 LIMIT，仍属于暴力扫描
SELECT *
FROM spark_analytics.spark_task_metrics
WHERE dt = '20260308';
