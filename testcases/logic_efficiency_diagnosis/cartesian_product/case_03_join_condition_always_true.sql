-- ============================================================================
-- Case 03: JOIN 条件恒为真（ON 1=1 / ON TRUE）导致笛卡尔积
-- ============================================================================
-- 【问题描述】
--   当 JOIN 的 ON 条件恒为真时（如 ON 1=1、ON TRUE），效果等同于
--   CROSS JOIN，产生笛卡尔积。这种写法常见于：
--     1. 动态 SQL 拼接时用 ON 1=1 作为占位符，后续条件用 AND 追加
--     2. 代码生成器/ORM 框架生成的模板 SQL
--     3. 调试时临时将 ON 条件改为 1=1 后忘记恢复
--     4. 误将 WHERE 条件放到了 ON 后面，ON 本身没有实际关联
--
-- 【易犯场景】
--   1. Java/Python 拼接 SQL 时用 "ON 1=1" + 动态条件拼接
--   2. 调试时注释掉真正的 ON 条件，用 1=1 临时替代
--   3. 将所有条件写在 WHERE 中，ON 写了 1=1 占位
--   4. 条件挪移后 ON 子句变为空壳
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - JOIN ON 条件恒为真，等效于 CROSS JOIN
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 3a: ON 1=1 占位符 —— 动态 SQL 拼接遗留
-- 开发人员用 ON 1=1 做动态 SQL 模板，条件本应 AND 追加但遗漏了
-- ❌ 错误：ON 1=1 恒为真，产生笛卡尔积
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.executor_num,
    j.job_id,
    j.`action`,
    j.status                          AS job_status,
    j.failed_reason,
    (j.end_time - j.start_time)       AS job_duration_ms,
    (j.start_time - j.submit_time)    AS queue_time_ms
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON 1 = 1                          -- ❌ 恒真条件，产生笛卡尔积
    -- 动态条件应该追加在这里：AND a.app_id = j.app_id
WHERE a.dt = '20260308'
  AND j.dt = '20260308'
  AND a.result != 0
ORDER BY job_duration_ms DESC
LIMIT 100;

-- ✅ 正确写法：
-- ON a.app_id = j.app_id
--    AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 3b: ON TRUE 显式恒真 + WHERE 中补关联条件
-- ON 条件为 TRUE，关联条件写在 WHERE 中，语义上仍先做笛卡尔积
-- ❌ 错误：ON TRUE 等效 CROSS JOIN
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    t.task_id,
    t.status                          AS task_status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time,
    ROUND(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1), 2)
                                      AS gc_ratio_pct
FROM spark_analytics.spark_stage_metrics s
INNER JOIN spark_analytics.spark_task_metrics t
    ON TRUE                           -- ❌ 恒真条件
WHERE s.dt = '20260308'
  AND t.dt = '20260308'
  AND s.app_id = t.app_id                              -- 关联放在 WHERE 中
  AND s.stage_id = t.stage_id
  AND t.task_run_time > 10000
ORDER BY gc_ratio_pct DESC
LIMIT 200;

-- ✅ 正确写法：将关联条件移到 ON 子句
-- ON s.app_id = t.app_id
--    AND s.stage_id = t.stage_id
--    AND s.dt = t.dt


-- ---------------------------------------------------------------------------
-- Case 3c: ON 中只有非关联的过滤条件，无实际关联
-- ON 子句写了条件但都是单表过滤，不是跨表关联
-- ❌ 错误：ON 中的条件没有关联两张表，实质上仍是笛卡尔积
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    j.job_id,
    j.status,
    j.submit_time,
    j.start_time,
    j.end_time,
    j.failed_reason
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.dt = '20260308'                  -- ❌ 单表过滤非关联
    AND j.dt = '20260308'                 -- ❌ 单表过滤非关联
    AND a.result != 0                                  -- ❌ 单表过滤非关联
    -- ❌ 没有 a.xxx = j.xxx 的跨表关联条件
WHERE j.status = 'FAILED'
ORDER BY j.submit_time DESC
LIMIT 100;

-- ✅ 正确写法：ON 中必须有跨表关联条件
-- ON a.app_id = j.app_id
--    AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 3d: 多表 JOIN 中某个 JOIN 的 ON 恒为真
-- 四表 JOIN 中，app-job 和 stage-task 有正确关联，
-- 但 job-stage 之间的 ON 是恒真条件
-- ❌ 错误：第二个 JOIN 的 ON 恒为真
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.status                          AS job_status,
    s.stage_id,
    s.num_tasks,
    t.task_id,
    t.task_run_time,
    t.gc_time
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id                             -- ✅ 正确关联
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON 1 = 1                                           -- ❌ 恒真，笛卡尔积
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id                             -- ✅ 正确关联
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE a.dt = '20260308'
ORDER BY t.task_run_time DESC
LIMIT 100;

-- ✅ 正确写法：
-- INNER JOIN stage s
--     ON a.app_id = s.app_id
--     AND a.dt = s.dt
