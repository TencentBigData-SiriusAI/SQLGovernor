-- ============================================================================
-- Case 10: CTE（WITH 子句）与复杂查询的语法错误
-- ============================================================================
-- 【问题描述】
--   CTE（Common Table Expression）是编写复杂数仓 SQL 的重要工具，
--   但其语法规则容易被忽视：
--     1. 多个 CTE 之间缺少逗号或多了逗号
--     2. CTE 中引用尚未定义的后续 CTE（前向引用）
--     3. 最后一个 CTE 后多余逗号
--     4. CTE 列名在外层引用时拼写错误
--     5. WITH AS 语法格式不完整
--
-- 【易犯场景】
--   1. 多个 CTE 之间用分号而非逗号分隔
--   2. CTE 定义的顺序与引用顺序不匹配
--   3. 最后一个 CTE 后面多了逗号导致解析失败
--   4. CTE 内定义的列别名在外层引用时拼写不一致
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - CTE / WITH 子句语法错误
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 10a: 多个 CTE 之间用分号分隔（应为逗号）
-- ❌ 错误：CTE 之间应用逗号分隔，不是分号
-- ---------------------------------------------------------------------------
WITH app_stats AS (
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        a.platform,
        a.result,
        a.executor_num,
        (a.end_time - a.start_time) AS app_duration
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
);                                    -- ❌ 应为逗号

job_stats AS (                        -- ❌ 已不在 WITH 上下文中
    SELECT
        j.app_id,
        j.job_id,
        j.status,
        (j.end_time - j.start_time) AS job_duration,
        j.failed_reason
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
)
SELECT
    a.app_id, a.app_name, j.job_id, j.job_duration
FROM app_stats a
LEFT JOIN job_stats j ON a.app_id = j.app_id
ORDER BY a.app_duration DESC LIMIT 100;

-- ✅ 正确写法：CTE 之间用逗号
-- WITH app_stats AS (...),
--      job_stats AS (...)
-- SELECT ...


-- ---------------------------------------------------------------------------
-- Case 10b: CTE 前向引用 —— 引用尚未定义的 CTE
-- task_agg 引用了 stage_agg，但 stage_agg 在后面才定义
-- ❌ 错误：CTE 只能引用在它之前定义的 CTE
-- ---------------------------------------------------------------------------
WITH task_agg AS (
    SELECT
        sa.app_id,
        sa.stage_id,
        COUNT(*)                     AS task_count,
        AVG(t.task_run_time)         AS avg_run_time,
        SUM(t.gc_time)               AS total_gc
    FROM stage_agg sa                 -- ❌ stage_agg 尚未定义
    INNER JOIN spark_analytics.spark_task_metrics t
        ON sa.app_id = t.app_id
        AND sa.stage_id = t.stage_id
        AND t.dt = '20260308'
    GROUP BY sa.app_id, sa.stage_id
),
stage_agg AS (
    SELECT
        s.app_id,
        s.stage_id,
        s.status         AS stage_status,
        s.num_tasks,
        (s.end_time - s.start_time) AS stage_duration
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.dt = '20260308'
)
SELECT * FROM task_agg ORDER BY avg_run_time DESC LIMIT 100;

-- ✅ 正确写法：调换顺序，先定义 stage_agg 再定义 task_agg


-- ---------------------------------------------------------------------------
-- Case 10c: 最后一个 CTE 后面多余逗号
-- ❌ 错误：最后一个 CTE 后面不应有逗号
-- ---------------------------------------------------------------------------
WITH failed_apps AS (
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        a.platform,
        a.result
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
      AND a.result != 0
),
failed_jobs AS (
    SELECT
        j.app_id,
        j.job_id,
        j.status,
        j.failed_reason,
        (j.end_time - j.start_time) AS job_duration
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
      AND j.status = 'FAILED'
),                                    -- ❌ 最后一个 CTE 后多余逗号
SELECT
    fa.app_id,
    fa.app_name,
    fj.job_id,
    fj.failed_reason,
    fj.job_duration
FROM failed_apps fa
INNER JOIN failed_jobs fj ON fa.app_id = fj.app_id
ORDER BY fj.job_duration DESC
LIMIT 50;

-- ✅ 正确写法：删掉最后一个 CTE 后的逗号
-- ),
-- SELECT ...  →  )
-- SELECT ...


-- ---------------------------------------------------------------------------
-- Case 10d: CTE 列别名在外层引用时拼写错误
-- CTE 中定义了 stage_dur，外层引用时写成了 stage_duration
-- ❌ 错误：列名不匹配，外层找不到该列
-- ---------------------------------------------------------------------------
WITH stage_metrics AS (
    SELECT
        s.app_id,
        s.stage_id,
        s.num_tasks,
        s.status,
        (s.end_time - s.start_time)  AS stage_dur,     -- 定义为 stage_dur
        s.submit_time,
        s.start_time,
        s.end_time
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.dt = '20260308'
      AND s.num_tasks > 0
)
SELECT
    sm.app_id,
    sm.stage_id,
    sm.num_tasks,
    sm.status,
    sm.stage_duration,                -- ❌ 拼写错误，CTE中定义的是 stage_dur
    sm.submit_time,
    sm.start_time,
    sm.end_time,
    ROUND(sm.stage_duration / GREATEST(sm.num_tasks, 1), 2)
                                      AS avg_task_time  -- ❌ 同样引用错误列名
FROM stage_metrics sm
WHERE sm.stage_duration > 60000       -- ❌ 同样引用错误列名
ORDER BY sm.stage_duration DESC       -- ❌ 同样引用错误列名
LIMIT 200;

-- ✅ 正确写法：使用 CTE 中定义的正确列名 stage_dur
-- sm.stage_dur
