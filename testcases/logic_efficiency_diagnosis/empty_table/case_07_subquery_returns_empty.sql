-- ============================================================================
-- Case 07: 子查询返回空集导致外层结果异常
-- ============================================================================
-- 【问题描述】
--   IN/NOT IN/EXISTS/NOT EXISTS 子查询返回空集时的行为差异：
--     1. WHERE col IN (空集) → 条件永远为 FALSE，外层结果为空
--     2. WHERE col NOT IN (空集) → 条件永远为 TRUE，返回全部数据
--     3. WHERE EXISTS (空集) → FALSE，外层结果为空
--     4. WHERE NOT EXISTS (空集) → TRUE，返回全部数据
--   这些差异在子查询源表为空时表现尤为突出。
--
-- 【易犯场景】
--   1. IN 子查询的源表/分区为空，外层过滤掉所有数据
--   2. NOT IN 子查询为空时返回全部数据，与预期的"排除某些值"不同
--   3. 关联子查询中的表为空，EXISTS 总返回 FALSE
--   4. 标量子查询返回空集（NULL），外层比较结果异常
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - 子查询可能返回空集，影响外层查询的过滤逻辑
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 7a: IN 子查询返回空集 —— 外层结果为空
-- 从空分区查失败的 app_id 列表，作为 IN 条件过滤 job 表
-- 子查询为空 → IN (空集) → 永远 FALSE → job 查询返回0行
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.action,
    j.status,
    j.submit_time,
    j.start_time,
    j.end_time,
    j.failed_reason,
    (j.end_time - j.start_time)               AS job_duration_ms
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20270308'
  -- ❌ 子查询查空分区，返回空集 → IN (空集) → 永远 FALSE
  AND j.app_id IN (
      SELECT app_id
      FROM spark_analytics.spark_app_metrics
      WHERE dt = '20270310'      -- ❌ 未来分区，为空
        AND `result` != 0
  )
ORDER BY j.submit_time DESC;


-- ---------------------------------------------------------------------------
-- Case 7b: NOT IN 子查询返回空集 —— 外层返回全部数据
-- 本意是排除昨天已处理的 stage，但昨天分区为空
-- NOT IN (空集) → 永远 TRUE → 返回所有 stage（未做任何排除）
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.stage_attempt_id,
    s.num_tasks,
    s.status,
    s.submit_time,
    s.start_time,
    s.end_time,
    (s.end_time - s.start_time)               AS stage_duration_ms,
    CASE
        WHEN s.num_tasks > 0
        THEN ROUND((s.end_time - s.start_time) * 1.0 / s.num_tasks, 2)
        ELSE 0
    END                                        AS avg_task_duration
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20270308'
  -- ❌ 本意：排除昨天已处理的 stage
  -- 但昨天分区为空 → NOT IN (空集) → 不排除任何行 → 返回全部
  AND s.stage_id NOT IN (
      SELECT stage_id
      FROM spark_analytics.spark_stage_metrics
      WHERE dt = '20270310'      -- ❌ 空分区
        AND status = 'SUCCEEDED'
  )
  AND s.status = 'FAILED'
ORDER BY s.num_tasks DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- Case 7c: 标量子查询返回 NULL（源表为空）
-- 子查询 SELECT MAX() FROM 空表 返回 NULL
-- 外层 WHERE col > NULL → 结果永远为 UNKNOWN → 过滤掉所有行
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time,
    t.peak_memory,
    t.shuffle_read_bytes,
    t.shuffle_write_bytes,
    ROUND(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1), 2)
                                               AS gc_pct
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20270308'
  -- ❌ 标量子查询从空分区取 MAX → 返回 NULL
  -- task_run_time > NULL → UNKNOWN → 过滤掉所有行
  AND t.task_run_time > (
      SELECT MAX(task_run_time)
      FROM spark_analytics.spark_task_metrics
      WHERE dt = '20270310'      -- ❌ 空分区 → MAX 返回 NULL
  )
  -- ❌ 同理，AVG 也返回 NULL
  AND t.gc_time > (
      SELECT AVG(gc_time)
      FROM spark_analytics.spark_task_metrics
      WHERE dt = '20270310'
  )
ORDER BY t.task_run_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 7d: EXISTS 子查询源表为空 —— 外层结果为空
-- 本意是查找有 task 数据的 stage，但 task 表分区为空
-- EXISTS 对每一行都返回 FALSE，外层结果为空
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.num_tasks,
    s.status,
    s.start_time,
    s.end_time,
    (s.end_time - s.start_time)               AS stage_duration_ms
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20270308'
  -- ❌ task 表空分区 → EXISTS 永远 FALSE → 外层0行结果
  AND EXISTS (
      SELECT 1
      FROM spark_analytics.spark_task_metrics t
      WHERE t.app_id = s.app_id
        AND t.stage_id = s.stage_id
        AND t.dt = '20270310'    -- ❌ 空分区
        AND t.status = 'SUCCESS'
  )
ORDER BY s.num_tasks DESC;
