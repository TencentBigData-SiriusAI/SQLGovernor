-- ============================================================================
-- Case 06: 复合键 JOIN 只写部分关联字段导致数据膨胀
-- ============================================================================
-- 【问题描述】
--   在数仓中，很多表的关联需要复合键（如 app_id + stage_id、
--   app_id + dt 等）。如果 JOIN 时只写了部分关联字段，
--   就会导致一行匹配多行（多对多 JOIN），结果集意外膨胀：
--     1. 不是完全笛卡尔积，但数据量可能膨胀数十倍
--     2. 下游聚合指标（SUM/COUNT）会偏大
--     3. DISTINCT 后数据看似正常但已经丢失了精确关联关系
--     4. 问题往往在上线后才被发现（指标突然翻倍）
--
-- 【易犯场景】
--   1. task 表关联 stage 表时只写了 stage_id，漏了 app_id
--   2. 跨天查询时只关联 app_id，漏了 dt
--   3. 多层级实体（app→job→stage→task）关联时漏掉中间层级 ID
--   4. 复制其他查询的 JOIN 条件但场景不同，需要的关联键不一样
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - JOIN 关联键不完整，可能导致多对多关联和数据膨胀
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 6a: 跨天查询只用 app_id 关联，漏了日期
-- 查询多天数据时只用 app_id 关联 app 和 job，
-- 同一 app_id 在不同天出现会导致交叉匹配
-- ❌ 错误：缺少 dt 关联
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.dt                 AS app_date,
    j.job_id,
    j.status                          AS job_status,
    j.dt                 AS job_date,
    (j.end_time - j.start_time)       AS job_duration_ms,
    j.failed_reason
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id                             -- ✅ 有 app_id
    -- ❌ 缺少 AND a.dt = j.dt
WHERE a.dt BETWEEN '20260301' AND '20260308'
  AND j.dt BETWEEN '20260301' AND '20260308'
  -- 跨天查询时，3月1日的 app 会与3月8日的 job 交叉匹配
  AND a.result != 0
ORDER BY j.end_time DESC
LIMIT 500;

-- ✅ 正确写法：
-- ON a.app_id = j.app_id
--    AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 6b: task 只用 stage_id 关联 stage，漏了 app_id
-- stage_id 可能在不同 app 中重复（如 stage_id = '0', '1', '2'），
-- 只用 stage_id 关联会产生跨 app 的错误匹配
-- ❌ 错误：stage_id 非全局唯一，必须加 app_id 联合关联
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    (s.end_time - s.start_time)       AS stage_duration,
    t.task_id,
    t.status                          AS task_status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time,
    CASE
        WHEN t.task_run_time > 0
        THEN ROUND(t.gc_time * 100.0 / t.task_run_time, 2)
        ELSE 0
    END                               AS gc_ratio_pct
FROM spark_analytics.spark_stage_metrics s
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.stage_id = t.stage_id                         -- ❌ 只用 stage_id
    AND s.dt = t.dt
    -- 缺少 AND s.app_id = t.app_id
WHERE s.dt = '20260308'
  AND s.num_tasks > 20
ORDER BY gc_ratio_pct DESC
LIMIT 200;

-- ✅ 正确写法：
-- ON s.app_id = t.app_id
--    AND s.stage_id = t.stage_id
--    AND s.dt = t.dt


-- ---------------------------------------------------------------------------
-- Case 6c: 聚合结果 JOIN 时关联键不充分
-- 两个聚合子查询分别按 app_id 和 app_id+stage_id 聚合，
-- JOIN 时只用 app_id 关联，导致一对多膨胀
-- ❌ 错误：粒度不一致的聚合结果 JOIN
-- ---------------------------------------------------------------------------
SELECT
    app_metrics.app_id,
    app_metrics.total_jobs,
    app_metrics.fail_rate,
    stage_metrics.stage_id,
    stage_metrics.stage_task_count,
    stage_metrics.avg_task_time,
    -- 由于一个 app 有多个 stage，下面的 total_jobs 会被重复计数
    stage_metrics.stage_task_count * app_metrics.total_jobs
                                      AS inflated_metric  -- ❌ 膨胀的指标
FROM (
    SELECT
        j.app_id,
        COUNT(*)                      AS total_jobs,
        ROUND(
            SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END) * 100.0
            / COUNT(*), 2
        )                             AS fail_rate
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
    GROUP BY j.app_id
) app_metrics
INNER JOIN (
    SELECT
        t.app_id,
        t.stage_id,
        COUNT(*)                      AS stage_task_count,
        AVG(t.task_run_time)          AS avg_task_time
    FROM spark_analytics.spark_task_metrics t
    WHERE t.dt = '20260308'
    GROUP BY t.app_id, t.stage_id
) stage_metrics
    ON app_metrics.app_id = stage_metrics.app_id       -- ❌ 粒度不同的 JOIN
    -- app_metrics 按 app_id 聚合（1行/app）
    -- stage_metrics 按 app_id + stage_id 聚合（多行/app）
    -- JOIN 后 app_metrics 的数据被重复
ORDER BY inflated_metric DESC
LIMIT 100;

-- ✅ 正确写法：
-- 方案1：将 stage_metrics 也聚合到 app_id 粒度
-- 方案2：在外层查询中避免对 app_metrics 的列做聚合
