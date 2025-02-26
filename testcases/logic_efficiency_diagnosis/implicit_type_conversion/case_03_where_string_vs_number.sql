-- ============================================================================
-- Case 03: WHERE 条件中字符串与数值混用
-- ============================================================================
-- 【问题描述】
--   WHERE 条件中将 STRING 类型字段与数值常量比较，或者将 BIGINT 类型字段
--   与字符串常量比较，引擎会对一侧做隐式转换。常见后果包括：
--     1. STRING 字段被 CAST 为 DOUBLE，'abc' 变 NULL 导致过滤失败
--     2. 数值语义与字典序不同：STRING '9' > STRING '10' 但 INT 9 < INT 10
--     3. 前导零丢失：STRING '007' 转为 INT 7，不再匹配 '007'
--     4. 执行计划无法下推谓词到存储层
--
-- 【易犯场景】
--   1. status 字段为 STRING（如 'FAILED'/'SUCCESS'），但用 0/1 去比较
--   2. result 字段为 BIGINT，但用 '0' 字符串去比较
--   3. 从 Excel/CSV 导入的配置文件中字段类型混乱
--   4. 多人协作时对字段类型认知不一致
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - WHERE 条件中字段与常量类型不匹配，发生隐式类型转换
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 3a: BIGINT 字段 result 与字符串常量比较
-- result 是 BIGINT 类型（0=成功，非0=错误码），用字符串 '0' 比较
-- 引擎会将 '0' 转为数值，虽然结果可能正确，但影响谓词下推和优化
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
    a.start_time,
    a.end_time,
    (a.end_time - a.start_time)               AS duration_ms,
    a.job_event_num,
    a.stage_event_num,
    a.task_event_num
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result = '0'                          -- ❌ result 是 BIGINT，用 STRING '0' 比较
  AND a.executor_num > '4'                    -- ❌ executor_num 是 BIGINT，用 STRING '4' 比较
  AND a.driver_memory > '2048'                -- ❌ driver_memory 是 BIGINT，用 STRING 比较
ORDER BY duration_ms DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 3b: STRING 字段 status 与数值比较
-- job 表的 status 是 STRING 类型（如 'SUCCEEDED'/'FAILED'），
-- 研发人员误以为是数值编码，用 0/1 来比较
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
    -- 总耗时
    (j.end_time - j.submit_time)              AS total_time_ms
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.status != 0                           -- ❌ status 是 STRING，与 INT 0 比较
  AND j.submit_time > '1709827200000'         -- ❌ submit_time 是 BIGINT，与 STRING 比较
ORDER BY j.submit_time DESC;


-- ---------------------------------------------------------------------------
-- Case 3c: 混合比较 —— 同一查询中多个字段类型错配
-- 综合场景：task 表中 STRING 和 BIGINT 字段都存在类型错配
-- 实际开发中一个查询内多处类型混用更难排查
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.task_id,
    t.stage_id,
    t.status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time,
    t.peak_memory,
    t.result_size,
    t.shuffle_read_bytes,
    t.shuffle_write_bytes,
    t.input_bytes,
    t.output_bytes,
    -- GC 耗时占比
    ROUND(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1), 2)
                                               AS gc_pct,
    -- 内存使用率指标
    ROUND(t.peak_memory * 100.0 / GREATEST(t.result_size, 1), 2)
                                               AS mem_usage_pct
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.status = 0                            -- ❌ status 是 STRING，与 INT 比较
  AND t.task_run_time > '5000'                -- ❌ task_run_time 是 BIGINT，与 STRING 比较
  AND t.gc_time > '1000'                      -- ❌ gc_time 是 BIGINT，与 STRING 比较
  AND t.stage_id = 1                          -- ❌ stage_id 是 STRING，与 INT 比较
  AND t.executor_cpu_time != '0'              -- ❌ BIGINT 与 STRING 比较
ORDER BY t.task_run_time DESC
LIMIT 500;
