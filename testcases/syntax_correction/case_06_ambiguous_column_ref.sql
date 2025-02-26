-- ============================================================================
-- Case 06: 歧义列引用（Ambiguous Column Reference）
-- ============================================================================
-- 【问题描述】
--   多表 JOIN 时，如果两张或多张表存在同名字段（如 app_id、status、
--   start_time、end_time、dt 等），SELECT/WHERE/ORDER BY 中
--   引用这些字段时必须加表前缀（别名.列名），否则会报"歧义列引用"错误。
--   这四张表有大量同名字段：
--     - app_id: 四表均有
--     - start_time / end_time: app/job/stage 三表均有
--     - status: job/stage/task 三表均有
--     - dt: 四表均有
--     - stage_id: stage/task 两表均有
--
-- 【易犯场景】
--   1. 先写单表查询，后来改为多表 JOIN，忘记给所有列加前缀
--   2. 多表 JOIN 时使用 SELECT *，后续 WHERE 中不加前缀
--   3. 复制粘贴其他查询的 WHERE 条件片段，条件中无表前缀
--   4. 习惯用短列名，不知道多张表有同名字段
--   5. CTE 中定义的列名与外层表列名冲突
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - 列名歧义（Ambiguous reference），需指定表前缀
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 6a: 双表 JOIN 中 app_id / start_time / end_time 未加前缀
-- app 和 job 表都有 app_id / start_time / end_time，不加前缀引擎无法判断
-- ❌ 错误：三个字段未指定表前缀
-- ---------------------------------------------------------------------------
SELECT
    app_id,                           -- ❌ 歧义：app 和 job 表都有 app_id
    a.app_name,
    a.`user`,
    j.job_id,
    j.`action`,
    status,                           -- ❌ 歧义：job 表有 status
    start_time,                       -- ❌ 歧义：两表都有 start_time
    end_time,                         -- ❌ 歧义：两表都有 end_time
    j.failed_reason,
    (end_time - start_time)           AS duration_ms  -- ❌ 歧义
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE dt = '20260308'    -- ❌ 歧义：两表都有此列
  AND status = 'FAILED'               -- ❌ 歧义
ORDER BY start_time DESC              -- ❌ 歧义
LIMIT 100;

-- ✅ 正确写法：所有同名字段加表前缀
-- SELECT a.app_id, j.status, a.start_time, j.end_time ...
-- WHERE a.dt = '20260308'


-- ---------------------------------------------------------------------------
-- Case 6b: 三表 JOIN 中 status 字段歧义
-- job / stage / task 三表都有 status 字段，不指定前缀报歧义错误
-- ❌ 错误：status 在三张表中都存在
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    s.stage_id,
    t.task_id,
    status,                           -- ❌ 歧义：job/stage/task 都有 status
    s.num_tasks,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_job_metrics j
INNER JOIN spark_analytics.spark_stage_metrics s
    ON j.app_id = s.app_id
    AND j.dt = s.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE j.dt = '20260308'
  AND status = 'FAILED'              -- ❌ 歧义：哪张表的 status？
ORDER BY t.task_run_time DESC
LIMIT 200;

-- ✅ 正确写法：
-- SELECT j.status AS job_status, s.status AS stage_status, t.status AS task_status


-- ---------------------------------------------------------------------------
-- Case 6c: 四表 JOIN 中 dt 歧义
-- 四表全关联时，WHERE 中 dt 未指定前缀
-- ❌ 错误：四张表都有 dt
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    j.job_id,
    s.stage_id,
    s.num_tasks,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    ROUND(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1), 2)
                                      AS gc_ratio_pct
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE dt = '20260308'    -- ❌ 歧义：四张表都有此字段
  AND app_id IS NOT NULL              -- ❌ 歧义：四张表都有 app_id
ORDER BY gc_ratio_pct DESC
LIMIT 100;

-- ✅ 正确写法：
-- WHERE a.dt = '20260308'
--   AND a.app_id IS NOT NULL


-- ---------------------------------------------------------------------------
-- Case 6d: stage_id 在 stage 和 task 表中歧义
-- stage 和 task 表都有 stage_id，GROUP BY 和 SELECT 中未指定前缀
-- ❌ 错误：stage_id 歧义
-- ---------------------------------------------------------------------------
SELECT
    stage_id,                         -- ❌ 歧义：stage 和 task 表都有
    COUNT(*)                          AS task_count,
    AVG(t.task_run_time)              AS avg_run_time,
    MAX(t.gc_time)                    AS max_gc_time,
    SUM(t.executor_cpu_time)          AS total_cpu_time,
    s.num_tasks                       AS expected_tasks,
    s.status                          AS stage_status
FROM spark_analytics.spark_stage_metrics s
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE s.dt = '20260308'
GROUP BY stage_id,                    -- ❌ 歧义
         s.num_tasks, s.status
HAVING COUNT(*) != s.num_tasks
ORDER BY task_count DESC
LIMIT 100;

-- ✅ 正确写法：
-- SELECT s.stage_id
-- GROUP BY s.stage_id, s.num_tasks, s.status
