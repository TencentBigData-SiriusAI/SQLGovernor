-- ============================================================================
-- Case 01: 缺失分区过滤条件
-- ============================================================================
-- 【问题描述】
--   数仓研发中最常见的暴力扫描错误：查询时完全没有带分区条件。
--   四张表均按 dt 分区，数据量随时间增长巨大。
--   不带分区条件会导致扫描全部历史分区，消耗大量计算资源和时间。
--
-- 【易犯场景】
--   1. 开发阶段临时查询数据验证逻辑，随手写了 WHERE 业务条件但忘加分区
--   2. 从非分区表迁移查询逻辑到分区表时，忘记补充分区过滤
--   3. 新人不了解表的分区设计，直接按业务字段过滤
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：查询未包含分区字段 dt 的过滤条件
-- ============================================================================

-- Case 1a: 单表查询缺失分区条件
-- 研发人员只关注 result != 0 的失败任务，但完全没限定日期范围
-- 这会扫描 app 表的所有历史分区
SELECT
    app_id,
    app_name,
    `user`,
    result,
    start_time,
    end_time
FROM spark_analytics.spark_app_metrics
WHERE result != 0
  AND platform = 'platform_a';


-- Case 1b: 聚合查询缺失分区条件
-- 统计每个用户的任务数和平均executor配置，没有时间范围限定
-- 实际数仓中这类统计SQL非常常见，研发人员容易忽略分区过滤
SELECT
    `user`,
    COUNT(*) AS total_apps,
    AVG(executor_num)  AS avg_executor_num,
    AVG(executor_memory) AS avg_executor_memory,
    SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END) AS success_count
FROM spark_analytics.spark_app_metrics
GROUP BY `user`
ORDER BY total_apps DESC
LIMIT 100;
