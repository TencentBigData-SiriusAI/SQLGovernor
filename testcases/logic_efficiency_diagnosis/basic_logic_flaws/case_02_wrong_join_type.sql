-- ============================================================================
-- Case 02: JOIN 类型选择错误
-- ============================================================================
-- 【问题描述】
--   不同 JOIN 类型的语义差异巨大：
--     1. INNER JOIN：只保留两表都匹配的行，丢失不匹配的数据
--     2. LEFT JOIN：保留左表所有行，右表不匹配补 NULL
--     3. RIGHT JOIN：保留右表所有行，左表不匹配补 NULL
--     4. FULL OUTER JOIN：保留两表所有行
--   数仓场景中，选错 JOIN 类型是最常见的逻辑疏漏：
--     - 需要"全量 app"时用了 INNER JOIN，没有 job 的 app 被丢弃
--     - 需要"已匹配"时用了 LEFT JOIN，多出一堆 NULL 行干扰统计
--     - 多表 JOIN 链路中某一环用错导致数据量突然缩水或膨胀
--
-- 【易犯场景】
--   1. 产出全量 app 明细时，习惯性用 INNER JOIN 关联 job 表
--   2. 统计"有 job 的 app"时误用 LEFT JOIN，结果含无 job 的 app
--   3. 复制粘贴其他人的 SQL 时未根据自己的需求调整 JOIN 类型
--   4. 多层嵌套 JOIN 中，中间某一层 JOIN 类型与整体逻辑不一致
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - JOIN 类型可能与业务逻辑不匹配
--   - 建议根据是否需要保留未匹配行来选择 INNER/LEFT/FULL JOIN
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 2a: 需要全量 app 报表时误用 INNER JOIN，丢失无 job 的 app
-- 业务需求：统计所有 app 的运行情况，包括没有 job 的 app
-- ❌ 错误：INNER JOIN 导致没有 job 的 app 被丢弃
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    a.executor_num,
    a.executor_memory,
    (a.end_time - a.start_time)                             AS app_duration,
    COUNT(j.job_id)                                         AS job_count,
    SUM(CASE WHEN j.status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS success_jobs,
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END)   AS failed_jobs,
    AVG(j.end_time - j.start_time)                          AS avg_job_duration
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j             -- ❌ 应为 LEFT JOIN
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name, a.`user`, a.platform, a.result,
         a.executor_num, a.executor_memory, (a.end_time - a.start_time)
ORDER BY app_duration DESC;

-- ✅ 正确写法：使用 LEFT JOIN 保留所有 app
-- LEFT JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 2b: 需要"有任务的 stage"时误用 LEFT JOIN，导致无 task 的 stage 混入
-- 业务需求：找出有 task 的 stage 并分析 task 耗时分布
-- ❌ 错误：LEFT JOIN 保留了无 task 的 stage，AVG 被 NULL 干扰
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status                                                AS stage_status,
    s.num_tasks,
    (s.end_time - s.start_time)                             AS stage_duration,
    COUNT(t.task_id)                                        AS actual_task_count,
    AVG(t.task_run_time)                                    AS avg_task_runtime,
    MAX(t.task_run_time)                                    AS max_task_runtime,
    SUM(t.gc_time)                                          AS total_gc_time,
    SUM(t.executor_cpu_time)                                AS total_cpu_time
FROM spark_analytics.spark_stage_metrics s
LEFT JOIN spark_analytics.spark_task_metrics t             -- ❌ 应为 INNER JOIN
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE s.dt = '20260308'
  AND s.status = 'COMPLETE'
GROUP BY s.app_id, s.stage_id, s.status, s.num_tasks,
         (s.end_time - s.start_time)
HAVING COUNT(t.task_id) > 0                                 -- ❌ 补救措施但浪费资源，不如直接 INNER JOIN
ORDER BY avg_task_runtime DESC
LIMIT 200;

-- ✅ 正确写法：直接使用 INNER JOIN
-- INNER JOIN spark_analytics.spark_task_metrics t
--     ON s.app_id = t.app_id AND s.stage_id = t.stage_id AND ...


-- ---------------------------------------------------------------------------
-- Case 2c: 多表 JOIN 链中某一环误用导致数据缩水
-- 业务需求：从 app → job → stage → task 全链路追踪，保留所有 app
-- ❌ 错误：app-job 用了 LEFT JOIN，但 job-stage 用了 INNER JOIN
--   导致没有 stage 的 job 被丢弃，进而连带丢弃了对应的 app 行
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    j.job_id,
    j.status                                                AS job_status,
    s.stage_id,
    s.num_tasks,
    s.status                                                AS stage_status,
    (s.end_time - s.start_time)                             AS stage_duration
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j              -- ✅ 正确保留所有 app
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s           -- ❌ 应为 LEFT JOIN
    ON j.app_id = s.app_id                                  -- 没有 stage 的 job 被丢弃
    AND j.dt = s.dt               -- 连带没有 job 的 app 也被丢弃
WHERE a.dt = '20260308'
ORDER BY a.app_id, j.job_id, s.stage_id;

-- ✅ 正确写法：整条链路统一用 LEFT JOIN
-- LEFT JOIN spark_analytics.spark_stage_metrics s
--     ON j.app_id = s.app_id AND j.dt = s.dt


-- ---------------------------------------------------------------------------
-- Case 2d: 一对多 JOIN 导致聚合指标膨胀（不是 JOIN 类型而是粒度问题的延伸）
-- 业务需求：统计每个 app 的 job 数量和总耗时
-- ❌ 错误：先 JOIN task 再聚合，task 一对多使 job 被重复统计
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    COUNT(DISTINCT j.job_id)                                AS job_count,        -- ❌ 需要 DISTINCT 但开发者可能忘记
    COUNT(j.job_id)                                         AS job_rows,         -- ❌ 因 task 膨胀，比实际 job 数大
    SUM(j.end_time - j.start_time)                          AS total_job_time,   -- ❌ 每个 job 被重复加了 N 次（N = task 数）
    COUNT(t.task_id)                                        AS task_count,
    AVG(t.task_run_time)                                    AS avg_task_time
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_task_metrics t            -- ❌ task 一对多膨胀了 job 行
    ON a.app_id = t.app_id
    AND a.dt = t.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name, a.`user`
ORDER BY total_job_time DESC
LIMIT 100;

-- ✅ 正确写法：分层聚合，先在子查询中各自聚合再 JOIN
-- WITH job_stats AS (
--     SELECT app_id, dt, COUNT(*) AS job_count, SUM(end_time - start_time) AS total_job_time
--     FROM spark_analytics.spark_job_metrics WHERE dt = '20260308' GROUP BY app_id, dt
-- ),
-- task_stats AS (
--     SELECT app_id, dt, COUNT(*) AS task_count, AVG(task_run_time) AS avg_task_time
--     FROM spark_analytics.spark_task_metrics WHERE dt = '20260308' GROUP BY app_id, dt
-- )
-- SELECT a.*, js.job_count, js.total_job_time, ts.task_count, ts.avg_task_time
-- FROM app a LEFT JOIN job_stats js ON ... LEFT JOIN task_stats ts ON ...
