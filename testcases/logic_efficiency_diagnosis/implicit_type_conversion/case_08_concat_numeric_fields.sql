-- ============================================================================
-- Case 08: 字符串拼接/运算中数值字段的隐式转换
-- ============================================================================
-- 【问题描述】
--   CONCAT、||（字符串拼接）、LIKE、REGEXP 等字符串函数/运算符要求参数为
--   STRING 类型。当传入 BIGINT/DOUBLE 类型字段时，引擎会隐式转换为 STRING。
--   反过来，算术运算（+、-、*、/）要求数值类型，传入 STRING 也会触发隐式转换。
--   潜在问题：
--     1. BIGINT -> STRING 后丢失类型信息，后续比较变为字典序
--     2. CONCAT 中 NULL 参数使整个结果为 NULL
--     3. 算术运算中 STRING -> DOUBLE 可能精度丢失
--     4. LIKE 匹配 BIGINT 字段时，数值先转字符串再匹配
--
-- 【易犯场景】
--   1. 拼接日志信息时混入数值字段但忘记 CAST
--   2. 构建复合 key 时直接 CONCAT 数值和字符串字段
--   3. 对 BIGINT 字段用 LIKE 做模式匹配（如 start_time LIKE '1709%'）
--   4. 字符串字段直接参与算术运算
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - 字符串函数/算术运算中参数类型不匹配，发生隐式转换
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 8a: CONCAT 中混入 BIGINT 字段
-- 构建复合 key 和描述信息时，直接将数值字段传入 CONCAT
-- 数值字段隐式转为 STRING，结果看似正确但存在类型转换开销
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    -- ❌ CONCAT 中 result (BIGINT) 隐式转 STRING
    CONCAT(a.app_id, '_', a.result)               AS app_result_key,
    -- ❌ 多个 BIGINT 字段隐式转 STRING
    CONCAT(
        a.`user`, '|',
        a.platform, '|',
        a.executor_num, 'cores_',                 -- ❌ executor_num: BIGINT
        a.executor_memory, 'mb_',                 -- ❌ executor_memory: BIGINT
        a.driver_memory, 'mb'                     -- ❌ driver_memory: BIGINT
    )                                              AS resource_desc,
    -- ❌ 使用 CONCAT 构造 JOIN key，混合 STRING 和 BIGINT
    CONCAT(a.app_id, '#', a.start_time)           AS app_time_key,
    a.app_name,
    a.result,
    a.start_time,
    a.end_time,
    -- ❌ CONCAT_WS 中同样存在隐式转换
    CONCAT_WS(',',
        a.app_id,                                  -- STRING
        a.app_name,                                -- STRING
        a.executor_num,                            -- ❌ BIGINT
        a.executor_memory,                         -- ❌ BIGINT
        a.result                                   -- ❌ BIGINT
    )                                              AS csv_row
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY a.start_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 8b: LIKE 匹配 BIGINT 字段
-- 对 start_time (BIGINT, 毫秒时间戳) 使用 LIKE 做前缀匹配
-- BIGINT 被隐式转为 STRING 后再做模式匹配，性能极差
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.start_time,
    a.end_time,
    a.result,
    a.platform,
    a.executor_num,
    a.executor_memory,
    (a.end_time - a.start_time)               AS duration_ms,
    -- ❌ 对 BIGINT 做字符串拼接比较
    CASE
        WHEN CONCAT(a.start_time, '') LIKE '1709%' THEN '2024-03'
        WHEN CONCAT(a.start_time, '') LIKE '1741%' THEN '2025-03'
        ELSE '其他时间段'
    END                                        AS time_period
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  -- ❌ BIGINT 字段直接 LIKE，隐式转 STRING
  AND a.start_time LIKE '1741%'
  -- ❌ BIGINT 字段做 REGEXP
  AND a.executor_memory REGEXP '^[0-9]{4}$'
ORDER BY a.start_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 8c: STRING 字段参与算术运算
-- task_event_num (STRING) 直接参与加减乘除运算
-- 以及 stage_id (STRING) 做数值运算
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.job_event_num,
    a.stage_event_num,
    a.task_event_num,
    -- ❌ task_event_num 是 STRING，直接参与算术运算触发隐式转换
    a.task_event_num + 0                      AS task_events_as_num,
    a.task_event_num * 1.0                    AS task_events_as_double,
    -- ❌ STRING 字段做除法
    a.task_event_num / GREATEST(a.stage_event_num, 1)
                                               AS tasks_per_stage,
    -- ❌ STRING 与 BIGINT 混合运算
    a.task_event_num - a.job_event_num        AS task_job_diff,
    -- ❌ 复合运算中 STRING 参与多步计算
    ROUND(
        (a.task_event_num * 100.0) /
        GREATEST(a.stage_event_num * a.job_event_num, 1),
        2
    )                                          AS complexity_ratio,
    a.platform,
    a.spark_version
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  -- ❌ STRING 字段做大小比较（算术语义）
  AND a.task_event_num > 100
ORDER BY task_events_as_num DESC
LIMIT 200;
