-- ============================================================================
-- Case 09: 聚合函数嵌套与混合粒度错误
-- ============================================================================
-- 【问题描述】
--   聚合函数（SUM/COUNT/AVG/MAX/MIN等）在使用中有严格的语法约束：
--     1. 聚合函数不能直接嵌套，如 MAX(COUNT(*)) 是非法的
--     2. SELECT 中混合聚合列和非聚合列（无 GROUP BY）
--     3. WHERE 中使用聚合函数（应该用 HAVING）
--     4. HAVING 中引用未聚合且不在 GROUP BY 中的列
--     5. 聚合函数中嵌套窗口函数（不允许）
--   这些约束背后的原因是 SQL 的执行粒度（行级 vs 组级）必须一致。
--
-- 【易犯场景】
--   1. 想求"最大的计数值"时直接写 MAX(COUNT(*))
--   2. SELECT 中既有明细列又有聚合列，忘了 GROUP BY
--   3. WHERE 和 HAVING 混淆，在 WHERE 中写 COUNT(*) > 10
--   4. 子查询中聚合再在外层聚合，但层次搞混
--   5. DISTINCT 和聚合函数的组合使用错误
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - 聚合函数使用不合法（嵌套/位置/粒度混合）
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 9a: 聚合函数直接嵌套 —— MAX(COUNT(*))
-- 想查每个 app 中 job 数最多的那个数量，直接嵌套写 MAX(COUNT(*))
-- ❌ 错误：聚合函数不能直接嵌套
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.platform,
    -- ❌ MAX 中嵌套了 COUNT，语法错误
    MAX(COUNT(*))                      AS max_job_count,
    AVG(COUNT(*))                      AS avg_job_count,  -- ❌ 同理
    MIN(SUM(j.end_time - j.start_time))
                                       AS min_total_duration  -- ❌ 同理
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.`user`, a.platform
ORDER BY max_job_count DESC;

-- ✅ 正确写法：用子查询分两层聚合
-- SELECT user, platform, MAX(job_count) FROM (
--     SELECT user, platform, app_id, COUNT(*) AS job_count FROM ... GROUP BY ...
-- ) sub GROUP BY user, platform


-- ---------------------------------------------------------------------------
-- Case 9b: WHERE 子句中使用聚合函数
-- 想过滤 job 数大于 5 的 app，但在 WHERE 而非 HAVING 中使用 COUNT
-- ❌ 错误：聚合函数不能出现在 WHERE 中，应用 HAVING
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    COUNT(*)                           AS job_count,
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END)
                                       AS fail_count,
    AVG(j.end_time - j.start_time)     AS avg_duration,
    MAX(j.end_time - j.submit_time)    AS max_total_time,
    MIN(j.submit_time)                 AS earliest_submit
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND COUNT(*) > 5                     -- ❌ WHERE 中不能用聚合函数
  AND AVG(j.end_time - j.start_time) > 30000  -- ❌ 同理
GROUP BY j.app_id
ORDER BY job_count DESC;

-- ✅ 正确写法：移到 HAVING 中
-- GROUP BY j.app_id
-- HAVING COUNT(*) > 5
--    AND AVG(j.end_time - j.start_time) > 30000


-- ---------------------------------------------------------------------------
-- Case 9c: SELECT 混合聚合与非聚合列（无 GROUP BY）
-- SELECT 中既有聚合函数又有普通列，但完全没有 GROUP BY
-- ❌ 错误：有聚合函数时，非聚合列必须在 GROUP BY 中
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,                          -- ❌ 非聚合列
    t.stage_id,                        -- ❌ 非聚合列
    t.task_id,                         -- ❌ 非聚合列
    t.status,                          -- ❌ 非聚合列
    COUNT(*)                           AS total_count,
    AVG(t.task_run_time)               AS avg_run_time,
    SUM(t.gc_time)                     AS total_gc_time,
    MAX(t.executor_cpu_time)           AS max_cpu_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
ORDER BY total_count DESC;
-- ❌ 完全没有 GROUP BY，但 SELECT 中混合了聚合和非聚合列

-- ✅ 正确写法：
-- GROUP BY t.app_id, t.stage_id, t.task_id, t.status


-- ---------------------------------------------------------------------------
-- Case 9d: HAVING 引用不在 GROUP BY 中的非聚合列
-- HAVING 中引用了不在 GROUP BY 中的列
-- ❌ 错误：HAVING 中的非聚合列必须在 GROUP BY 中
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    COUNT(*)                           AS stage_count,
    SUM(s.num_tasks)                   AS total_tasks,
    AVG(s.end_time - s.start_time)     AS avg_stage_duration,
    MAX(s.num_tasks)                   AS max_tasks
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
GROUP BY s.app_id
HAVING s.status = 'COMPLETE'           -- ❌ s.status 不在 GROUP BY 中
   AND COUNT(*) > 3
   AND s.num_tasks > 100               -- ❌ s.num_tasks 也不在 GROUP BY 中
ORDER BY total_tasks DESC
LIMIT 100;

-- ✅ 正确写法：
-- 方案1: 将 status 加入 GROUP BY
-- 方案2: 在 HAVING 中使用聚合: HAVING MAX(s.status) = 'COMPLETE'
-- 方案3: 在 WHERE 中提前过滤: WHERE s.status = 'COMPLETE'


-- ---------------------------------------------------------------------------
-- Case 9e: 聚合函数内嵌套窗口函数
-- 在 SUM 聚合中嵌套 ROW_NUMBER 窗口函数
-- ❌ 错误：聚合函数内不能嵌套窗口函数
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    -- ❌ 聚合函数中嵌套窗口函数，语法错误
    SUM(ROW_NUMBER() OVER(ORDER BY t.task_run_time))
                                       AS sum_of_ranks,
    COUNT(RANK() OVER(ORDER BY t.gc_time))
                                       AS count_of_ranks,  -- ❌ 同理
    MAX(t.task_run_time)               AS max_run_time,
    AVG(t.gc_time)                     AS avg_gc_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.status = 'SUCCESS'
GROUP BY t.app_id
ORDER BY max_run_time DESC
LIMIT 100;

-- ✅ 正确写法：先在子查询中计算窗口函数，外层再聚合
-- SELECT app_id, SUM(rn) FROM (
--     SELECT ..., ROW_NUMBER() OVER(...) AS rn FROM ...
-- ) sub GROUP BY app_id
