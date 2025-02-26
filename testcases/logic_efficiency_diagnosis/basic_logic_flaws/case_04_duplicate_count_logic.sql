-- ============================================================================
-- Case 04: 去重逻辑缺失导致指标重复计算
-- ============================================================================
-- 【问题描述】
--   在 COUNT / SUM 等聚合中，如果数据存在一对多关系或重复行，不做去重
--   会导致指标被重复计算。典型情况：
--     1. COUNT(column) 未用 DISTINCT，一对多 JOIN 后值被膨胀
--     2. SUM 在多表 JOIN 后对非聚合表的字段求和，每行被重复累加
--     3. 数据源本身有重复（如 stage 重试产生多条记录）未去重
--     4. UNION ALL 合并了有重叠的数据集但未用 UNION 去重
--     5. 窗口函数与聚合混用时未注意粒度
--
-- 【易犯场景】
--   1. app JOIN job JOIN task，对 app 级别指标聚合但被 task 膨胀
--   2. stage 有 retry（stage_attempt_id 不同），COUNT(stage_id) 含重复
--   3. 日报表中 UNION ALL 两个源，其中有交集导致重复统计
--   4. 先 JOIN 再 COUNT，未注意 JOIN 带来的行膨胀
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - 聚合函数未使用 DISTINCT，JOIN 后可能存在重复计算
--   - 建议检查数据粒度，必要时添加 DISTINCT 或先去重再聚合
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 4a: COUNT 未使用 DISTINCT，一对多 JOIN 导致 job 数被 task 膨胀
-- 一个 job 有多个 task，JOIN 后 COUNT(j.job_id) 被膨胀
-- ❌ 错误：COUNT(j.job_id) 被 task 行数膨胀
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    COUNT(j.job_id)                                         AS job_count,     -- ❌ 被 task 膨胀，应 COUNT(DISTINCT j.job_id)
    COUNT(t.task_id)                                        AS task_count,
    SUM(t.task_run_time)                                    AS total_task_time,
    AVG(t.gc_time)                                          AS avg_gc_time
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON a.app_id = t.app_id
    AND a.dt = t.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name, a.`user`
ORDER BY job_count DESC
LIMIT 100;

-- ✅ 正确写法：使用 COUNT(DISTINCT ...)
-- COUNT(DISTINCT j.job_id)  AS job_count


-- ---------------------------------------------------------------------------
-- Case 4b: SUM 在 JOIN 膨胀后重复累加 job 级指标
-- job 的 duration 在 JOIN task 后被每个 task 行都算了一次
-- ❌ 错误：SUM(job_duration) 被 task 行数膨胀
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    SUM(j.end_time - j.start_time)                          AS total_job_duration, -- ❌ 每个 job 被重复加了 N 次（N=task数）
    COUNT(t.task_id)                                        AS task_count,
    AVG(t.task_run_time)                                    AS avg_task_runtime
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON j.app_id = t.app_id
    AND j.dt = t.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name
ORDER BY total_job_duration DESC;

-- ✅ 正确写法：先在子查询中聚合 job 指标，再 JOIN
-- WITH job_agg AS (
--     SELECT app_id, dt,
--            SUM(end_time - start_time) AS total_job_duration
--     FROM spark_analytics.spark_job_metrics
--     WHERE dt = '20260308'
--     GROUP BY app_id, dt
-- )
-- SELECT a.app_id, ja.total_job_duration, ...
-- FROM app a LEFT JOIN job_agg ja ON ...


-- ---------------------------------------------------------------------------
-- Case 4c: stage 重试（stage_attempt_id）未去重导致 stage 数量虚高
-- 同一个 stage_id 可能有多次 attempt，直接 COUNT(stage_id) 会重复
-- ❌ 错误：未按最新 attempt 去重，stage 被重复统计
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    COUNT(s.stage_id)                                       AS stage_count,     -- ❌ 含重试的 stage 被重复计数
    SUM(s.num_tasks)                                        AS total_tasks,     -- ❌ 重试 stage 的 task 数被重复累加
    SUM(s.end_time - s.start_time)                          AS total_stage_time -- ❌ 重试 stage 的时间也被重复累加
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name, a.`user`
ORDER BY stage_count DESC
LIMIT 100;

-- ✅ 正确写法：先去重取最新 attempt，再聚合
-- WITH latest_stage AS (
--     SELECT *, ROW_NUMBER() OVER (PARTITION BY app_id, stage_id ORDER BY stage_attempt_id DESC) AS rn
--     FROM spark_analytics.spark_stage_metrics
--     WHERE dt = '20260308'
-- )
-- SELECT ... FROM latest_stage WHERE rn = 1 ...


-- ---------------------------------------------------------------------------
-- Case 4d: UNION ALL 合并重叠数据集导致重复统计
-- 两个子查询分别查失败 app 和有失败 job 的 app，存在交集
-- ❌ 错误：两个子查询有重叠 app，UNION ALL 不去重
-- ---------------------------------------------------------------------------
SELECT
    app_id,
    app_name,
    `user`,
    fail_source,
    COUNT(*)                                                AS total_count
FROM (
    -- 子集1：result 非 0 的 app（app 级别失败）
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        'APP_FAIL' AS fail_source
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
      AND a.result != 0

    UNION ALL                                               -- ❌ 应该用 UNION 去重，或在外层去重

    -- 子集2：有失败 job 的 app（job 级别失败）
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        'JOB_FAIL' AS fail_source
    FROM spark_analytics.spark_app_metrics a
    INNER JOIN spark_analytics.spark_job_metrics j
        ON a.app_id = j.app_id
        AND a.dt = j.dt
    WHERE a.dt = '20260308'
      AND j.status = 'FAILED'
) combined
GROUP BY app_id, app_name, `user`, fail_source              -- ❌ 同一 app 可能出现在两个子集中
ORDER BY total_count DESC;

-- ✅ 正确写法：明确去重策略
-- 方案1：改用 UNION 自动去重
-- 方案2：在外层用 ROW_NUMBER 或 DISTINCT 去重
-- 方案3：合并为一个查询用 CASE WHEN 标记来源
