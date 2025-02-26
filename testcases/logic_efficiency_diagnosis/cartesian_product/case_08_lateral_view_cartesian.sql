-- ============================================================================
-- Case 08: LATERAL VIEW EXPLODE 后与其他表缺少关联导致笛卡尔积
-- ============================================================================
-- 【问题描述】
--   在 Hive/SparkSQL 中使用 LATERAL VIEW EXPLODE 展开数组/Map 字段后，
--   如果与其他表 JOIN 时关联条件不充分，会导致笛卡尔积：
--     1. job 表的 stage_ids 字段（逗号分隔的字符串）EXPLODE 后产生多行
--     2. EXPLODE 后的行与 stage 表 JOIN 时如果缺少关联条件，会膨胀
--     3. 多层 LATERAL VIEW 嵌套时更容易出问题
--     4. EXPLODE 结果的列名容易写错导致关联失效
--
-- 【易犯场景】
--   1. job.stage_ids EXPLODE 后与 stage 表关联时只用了 app_id
--   2. EXPLODE 后的列名拼写错误导致 ON 条件不匹配
--   3. 多个 LATERAL VIEW 与多个表 JOIN 时关联关系混乱
--   4. EXPLODE 后忘了与展开来源的表保持关联
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - LATERAL VIEW 展开后与其他表 JOIN 关联不充分
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 8a: EXPLODE stage_ids 后与 stage 表关联缺少 stage_id
-- job 表的 stage_ids 展开后，与 stage 表 JOIN 时只用 app_id
-- 导致展开的每个 stage_id 都与该 app 下的所有 stage 交叉匹配
-- ❌ 错误：缺少 exploded_stage_id = s.stage_id 的关联
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status                          AS job_status,
    j.stage_ids,
    single_stage_id,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    (s.end_time - s.start_time)       AS stage_duration_ms
FROM spark_analytics.spark_job_metrics j
LATERAL VIEW EXPLODE(SPLIT(j.stage_ids, ',')) stage_tbl
    AS single_stage_id
INNER JOIN spark_analytics.spark_stage_metrics s
    ON j.app_id = s.app_id                             -- ✅ 有 app_id
    AND j.dt = s.dt
    -- ❌ 缺少 AND single_stage_id = s.stage_id
    -- 展开的每个 stage_id 与该 app 下所有 stage 交叉匹配
WHERE j.dt = '20260308'
  AND j.status = 'FAILED'
ORDER BY stage_duration_ms DESC
LIMIT 200;

-- ✅ 正确写法：
-- ON j.app_id = s.app_id
--    AND single_stage_id = s.stage_id
--    AND j.dt = s.dt


-- ---------------------------------------------------------------------------
-- Case 8b: EXPLODE 后与完全无关的表 JOIN
-- EXPLODE 展开 stage_ids 后，与 task 表 JOIN 但没有合理关联
-- ❌ 错误：展开后的结果与 task 表之间无有效关联
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    single_stage_id,
    t.task_id,
    t.status                          AS task_status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_job_metrics j
LATERAL VIEW EXPLODE(SPLIT(j.stage_ids, ',')) stage_tbl
    AS single_stage_id
INNER JOIN spark_analytics.spark_task_metrics t
    ON j.dt = t.dt           -- ❌ 只有日期关联
    -- 缺少 AND j.app_id = t.app_id
    -- 缺少 AND single_stage_id = t.stage_id
WHERE j.dt = '20260308'
ORDER BY t.task_run_time DESC
LIMIT 500;

-- ✅ 正确写法：
-- ON j.app_id = t.app_id
--    AND single_stage_id = t.stage_id
--    AND j.dt = t.dt


-- ---------------------------------------------------------------------------
-- Case 8c: EXPLODE 后与 app 表和 stage 表三方 JOIN 缺关联
-- 三表关联中 EXPLODE 后的列未参与关联
-- ❌ 错误：展开的 stage_id 未用于关联 stage 表
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    j.job_id,
    j.status                          AS job_status,
    single_stage_id                   AS exploded_sid,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    (s.end_time - s.start_time)       AS stage_duration_ms
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt          -- ✅ app-job 正确
LATERAL VIEW EXPLODE(SPLIT(j.stage_ids, ',')) stage_tbl
    AS single_stage_id
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
    -- ❌ 缺少 AND single_stage_id = s.stage_id
    -- 展开的 stage_id 没有参与关联，app 下所有 stage 都会匹配
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY stage_duration_ms DESC
LIMIT 200;

-- ✅ 正确写法：
-- ON a.app_id = s.app_id
--    AND single_stage_id = s.stage_id
--    AND a.dt = s.dt
