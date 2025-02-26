-- ============================================================================
-- Case 07: IN 列表类型不匹配导致隐式转换
-- ============================================================================
-- 【问题描述】
--   IN 子句中列表值的类型与目标字段不一致时，引擎会对目标字段或列表值做
--   隐式转换。常见问题：
--     1. STRING 字段 IN (1, 2, 3) —— 字段被转为数值，非数字值变 NULL
--     2. BIGINT 字段 IN ('1', '2', '3') —— 列表值被转为数值
--     3. IN 列表中混合不同类型值 —— 类型推导不确定
--     4. 子查询 IN 中返回列类型与外层不一致
--
-- 【易犯场景】
--   1. 从代码中拼接 IN 列表时，未统一引号（有些加引号有些不加）
--   2. 状态码字段类型与业务人员理解不一致（STRING 误当 INT 用）
--   3. 子查询 SELECT 的列经过运算后类型发生变化
--   4. 从配置表读取的值类型与目标表字段不同
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - IN 列表中值的类型与目标字段类型不匹配
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 7a: STRING 字段 IN (数值列表)
-- stage_id 是 STRING 类型，但 IN 列表用了整数
-- 引擎将 stage_id 转为数值比较，非数字 stage_id 变 NULL 被排除
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
    t.result_size,
    t.shuffle_read_bytes,
    t.shuffle_write_bytes,
    -- 性能分析指标
    ROUND(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1), 2)
                                                  AS gc_pct,
    ROUND(t.executor_cpu_time * 100.0 / GREATEST(t.task_run_time * 1000000, 1), 2)
                                                  AS cpu_util_pct,
    ROUND(t.shuffle_write_bytes * 1.0 / GREATEST(t.shuffle_read_bytes, 1), 4)
                                                  AS shuffle_amplification
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.stage_id IN (0, 1, 2, 3, 4, 5)           -- ❌ stage_id 是 STRING，IN 列表为 INT
  AND t.status IN ('SUCCESS', 'FAILED')            -- ✅ 正确：STRING IN (STRING)
ORDER BY t.task_run_time DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- Case 7b: BIGINT 字段 IN (字符串列表) + 混合类型 IN
-- result 是 BIGINT，IN 列表用了字符串
-- 同一查询中还有 executor_num IN 混合类型列表
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.spark_version,
    a.executor_num,
    a.executor_memory,
    a.executor_cores,
    a.driver_memory,
    a.driver_cores,
    a.rss_enabled,
    a.start_time,
    a.end_time,
    (a.end_time - a.start_time)               AS duration_ms,
    a.job_event_num,
    a.stage_event_num,
    a.task_event_num
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result IN ('0', '1', '137', '143')    -- ❌ result 是 BIGINT，IN 列表为 STRING
  AND a.executor_num IN (4, '8', 16, '32')    -- ❌ 混合 INT 和 STRING
  AND a.rss_enabled IN ('1', '0')             -- ❌ rss_enabled 是 BIGINT，用 STRING 列表
  AND a.platform IN ('platform_a', 'platform_b')           -- ✅ 正确：STRING IN (STRING)
ORDER BY a.start_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 7c: 子查询 IN 返回列类型不匹配
-- 外层 WHERE job_id IN (子查询)，子查询经过聚合/运算后返回类型变化
-- 这是最隐蔽的 IN 类型不匹配场景
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
    (j.start_time - j.submit_time)            AS queue_time_ms,
    (j.end_time - j.start_time)               AS exec_time_ms,
    (j.end_time - j.submit_time)              AS total_time_ms
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  -- ❌ job_id 是 STRING，子查询 CAST 返回 BIGINT
  AND j.job_id IN (
      SELECT CAST(job_id AS BIGINT)
      FROM spark_analytics.spark_job_metrics
      WHERE dt = '20260307'
        AND status = 'FAILED'
  )
  -- ❌ stage_ids 是 STRING，子查询返回 COUNT (BIGINT)
  AND j.stage_ids IN (
      SELECT CAST(COUNT(DISTINCT stage_id) AS STRING)
      FROM spark_analytics.spark_stage_metrics
      WHERE dt = '20260308'
      GROUP BY app_id
      HAVING COUNT(DISTINCT stage_id) > 5
  )
ORDER BY j.submit_time DESC
LIMIT 50;
