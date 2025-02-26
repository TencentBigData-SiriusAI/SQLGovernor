-- ============================================================================
-- Case 04: JOIN 缺少关键关联字段导致数据膨胀
-- ============================================================================
-- 【问题描述】
--   多表 JOIN 时只写了部分关联字段，或关联字段写错（关联到非唯一键上），
--   虽不是纯笛卡尔积，但产生了"部分笛卡尔积"——即一行匹配多行，
--   导致结果集远超预期：
--     1. app 与 job 只用 dt 关联，缺少 app_id
--     2. stage 与 task 只用 app_id 关联，缺少 stage_id
--     3. 关联字段不是唯一键，一对多变成多对多
--
-- 【易犯场景】
--   1. 分区字段 dt 当作关联键，但它不是唯一标识
--   2. 多级实体间只用顶层 ID 关联，漏掉了层级 ID
--   3. 复制 JOIN 条件时漏掉了复合键中的某个字段
--   4. 不了解表的主键/关联关系，凭直觉写关联条件
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - JOIN 关联条件不充分，可能导致数据膨胀（部分笛卡尔积）
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 4a: 只用分区字段关联 —— 同日所有 app × 所有 job
-- app 和 job 只通过 dt 关联，同一天的所有 app 会与所有 job
-- 产生笛卡尔积。假设当天有 1000 个 app 和 5000 个 job，结果 = 500万行
-- ❌ 错误：缺少 app_id 关联
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
    (j.end_time - j.start_time)       AS job_duration_ms
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.dt = j.dt           -- ❌ 只用分区字段关联
    -- 缺少 AND a.app_id = j.app_id
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY job_duration_ms DESC
LIMIT 100;

-- ✅ 正确写法：
-- ON a.app_id = j.app_id
--    AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 4b: stage-task 关联缺少 stage_id —— 同 app 下所有 stage × task
-- stage 和 task 只用 app_id 关联但漏了 stage_id，
-- 同一 app 下的所有 stage 会与所有 task 产生笛卡尔积
-- ❌ 错误：缺少 stage_id 关联
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
    ROUND(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1), 2)
                                      AS gc_ratio_pct
FROM spark_analytics.spark_stage_metrics s
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id                             -- ✅ 有 app_id
    AND s.dt = t.dt
    -- ❌ 缺少 AND s.stage_id = t.stage_id
WHERE s.dt = '20260308'
  AND s.num_tasks > 50
ORDER BY gc_ratio_pct DESC
LIMIT 200;

-- ✅ 正确写法：
-- ON s.app_id = t.app_id
--    AND s.stage_id = t.stage_id
--    AND s.dt = t.dt


-- ---------------------------------------------------------------------------
-- Case 4c: 四表链式 JOIN 中间环节关联不充分
-- app→job 关联正确，job→stage 只用日期关联，stage→task 关联正确
-- 中间环节的不充分关联导致整体结果膨胀
-- ❌ 错误：job-stage 之间缺少 app_id 关联
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    j.job_id,
    j.status                          AS job_status,
    s.stage_id,
    s.num_tasks,
    t.task_id,
    t.task_run_time,
    t.gc_time
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id                             -- ✅ 正确
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON j.dt = s.dt           -- ❌ 只用日期，缺 app_id
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id                             -- ✅ 正确
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY t.task_run_time DESC
LIMIT 500;

-- ✅ 正确写法：
-- INNER JOIN stage s
--     ON j.app_id = s.app_id
--     AND j.dt = s.dt
