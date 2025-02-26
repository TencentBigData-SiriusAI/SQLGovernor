-- ============================================================================
-- Case 02: GROUP BY 遗漏非聚合列
-- ============================================================================
-- 【问题描述】
--   在使用 GROUP BY 时，SELECT 中所有非聚合列都必须出现在 GROUP BY 子句中。
--   遗漏非聚合列会导致：
--     1. Hive 严格模式下直接报错
--     2. SparkSQL 默认严格，直接抛 AnalysisException
--     3. 非严格模式下可能随机取值，产出不确定结果
--     4. 多列 GROUP BY 时容易漏掉一两个字段
--
-- 【易犯场景】
--   1. SELECT 列较多时，复制粘贴后忘了同步 GROUP BY
--   2. 后期迭代新增 SELECT 字段，忘了在 GROUP BY 中补充
--   3. 使用表达式作为 SELECT 列时，GROUP BY 中未写对应表达式
--   4. 多表 JOIN 后 GROUP BY 只写了部分表的字段
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - SELECT 中的非聚合列未在 GROUP BY 中声明
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 2a: 基础遗漏 —— SELECT 含多个非聚合列但 GROUP BY 漏写
-- 按用户和平台统计 app 指标，但 GROUP BY 中漏了 platform
-- ❌ 错误：platform 在 SELECT 中但未在 GROUP BY 中
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.platform,                                     -- ❌ 未在 GROUP BY 中
    COUNT(*)                                        AS app_count,
    SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END)  AS success_count,
    SUM(CASE WHEN a.result != 0 THEN 1 ELSE 0 END) AS fail_count,
    ROUND(
        SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 2
    )                                               AS success_rate,
    AVG(a.end_time - a.start_time)                  AS avg_duration_ms,
    MAX(a.executor_num)                             AS max_executor_num,
    MIN(a.start_time)                               AS earliest_start
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.`user`                                   -- ❌ 缺少 a.platform
ORDER BY app_count DESC;

-- ✅ 正确写法：
-- GROUP BY a.`user`, a.platform


-- ---------------------------------------------------------------------------
-- Case 2b: 表达式列遗漏 —— SELECT 中有 CASE 表达式但 GROUP BY 未包含
-- 按 app 规模分类统计，CASE 表达式出现在 SELECT 中但未在 GROUP BY 声明
-- ❌ 错误：CASE 表达式作为分组维度时，必须完整出现在 GROUP BY 中
-- ---------------------------------------------------------------------------
SELECT
    CASE
        WHEN a.executor_num >= 100 THEN '大规模(>=100)'
        WHEN a.executor_num >= 20  THEN '中规模(20-99)'
        WHEN a.executor_num >= 5   THEN '小规模(5-19)'
        ELSE '微型(<5)'
    END                                             AS app_scale,      -- ❌ 未在 GROUP BY
    a.platform,
    COUNT(*)                                        AS app_count,
    AVG(a.executor_memory)                          AS avg_memory,
    AVG(a.executor_cores)                           AS avg_cores,
    SUM(a.end_time - a.start_time)                  AS total_duration_ms,
    MAX(a.driver_memory)                            AS max_driver_mem
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.platform                                 -- ❌ 缺少 CASE 表达式
ORDER BY app_count DESC;

-- ✅ 正确写法：
-- GROUP BY
--     CASE WHEN a.executor_num >= 100 THEN '大规模(>=100)'
--          WHEN a.executor_num >= 20  THEN '中规模(20-99)'
--          WHEN a.executor_num >= 5   THEN '小规模(5-19)'
--          ELSE '微型(<5)'
--     END,
--     a.platform


-- ---------------------------------------------------------------------------
-- Case 2c: 多表 JOIN 后 GROUP BY 遗漏另一张表的字段
-- app 和 job 表 JOIN 后按维度分组，但 GROUP BY 中漏了 job 表的字段
-- ❌ 错误：j.status 在 SELECT 中但未在 GROUP BY 中
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.platform,
    j.status,                                       -- ❌ 未在 GROUP BY 中
    COUNT(DISTINCT j.job_id)                        AS job_count,
    COUNT(DISTINCT a.app_id)                        AS app_count,
    AVG(j.end_time - j.start_time)                  AS avg_job_duration,
    MAX(j.end_time - j.submit_time)                 AS max_total_time,
    SUM(CASE
        WHEN j.failed_reason IS NOT NULL
            AND j.failed_reason != ''
        THEN 1 ELSE 0
    END)                                            AS has_reason_count
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.`user`, a.platform                       -- ❌ 缺少 j.status
ORDER BY job_count DESC
LIMIT 200;

-- ✅ 正确写法：
-- GROUP BY a.`user`, a.platform, j.status


-- ---------------------------------------------------------------------------
-- Case 2d: 迭代新增字段后 GROUP BY 未同步更新
-- 原始查询只 GROUP BY app_id，后来新增了 stage 维度字段但忘了更新 GROUP BY
-- 模拟迭代开发中最常见的遗漏
-- ❌ 错误：s.status 是后来新增的 SELECT 字段，GROUP BY 没跟着更新
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.status,                                       -- ❌ 后续迭代新增，未同步到 GROUP BY
    COUNT(*)                                        AS stage_count,
    SUM(s.num_tasks)                                AS total_tasks,
    AVG(s.end_time - s.start_time)                  AS avg_stage_duration,
    MAX(s.num_tasks)                                AS max_tasks_per_stage,
    -- 以下是后续迭代新增的统计指标
    MIN(s.submit_time)                              AS earliest_submit,
    MAX(s.end_time)                                 AS latest_end
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.num_tasks > 0
GROUP BY s.app_id                                   -- ❌ 缺少 s.status
HAVING COUNT(*) > 3
ORDER BY total_tasks DESC
LIMIT 100;

-- ✅ 正确写法：
-- GROUP BY s.app_id, s.status
