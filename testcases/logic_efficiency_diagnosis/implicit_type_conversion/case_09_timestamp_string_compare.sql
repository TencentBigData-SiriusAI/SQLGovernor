-- ============================================================================
-- Case 09: 时间戳(BIGINT)与日期字符串比较导致隐式转换
-- ============================================================================
-- 【问题描述】
--   四张表中的 start_time、end_time、submit_time、timestamp 等字段均为
--   BIGINT 类型（毫秒时间戳），但研发人员经常习惯性地用日期字符串做比较。
--   这会导致引擎将一侧做隐式转换：
--     1. BIGINT 时间戳与 '2026-03-08' 比较时，字符串被转为数值（可能失败）
--     2. 或 BIGINT 被转为 STRING 后做字典序比较（结果完全错误）
--     3. 使用 DATE 函数包装 BIGINT 字段时产生中间隐式转换
--     4. 跨时区比较时隐式转换行为不一致
--
-- 【易犯场景】
--   1. 研发人员习惯写 WHERE start_time > '2026-03-08' 类似的条件
--   2. 不清楚时间戳是秒还是毫秒，直接与日期字符串比较
--   3. 使用 FROM_UNIXTIME 但忘了毫秒需除以 1000
--   4. DATEDIFF 等日期函数接收 BIGINT 参数时的隐式转换
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - BIGINT 时间戳字段与 STRING 日期做比较，存在隐式类型转换
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 9a: BIGINT 时间戳与日期字符串直接比较
-- start_time/end_time 是毫秒时间戳(BIGINT)，用日期字符串做范围过滤
-- 引擎尝试将 '2026-03-08' 转为 BIGINT，结果不可预测
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.start_time,
    a.end_time,
    a.`timestamp`,
    -- 正确做法的对比：手动转换为可读时间
    FROM_UNIXTIME(a.start_time / 1000, 'yyyy-MM-dd HH:mm:ss')
                                                  AS start_time_str,
    FROM_UNIXTIME(a.end_time / 1000, 'yyyy-MM-dd HH:mm:ss')
                                                  AS end_time_str,
    (a.end_time - a.start_time)               AS duration_ms,
    a.executor_num,
    a.executor_memory
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  -- ❌ BIGINT 时间戳与日期字符串比较，引擎做隐式转换
  AND a.start_time > '2026-03-08 00:00:00'
  AND a.end_time < '2026-03-09 00:00:00'
  -- ❌ timestamp (BIGINT) 与字符串比较
  AND a.`timestamp` >= '2026-03-08'
ORDER BY a.start_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 9b: 日期函数中 BIGINT 参数的隐式转换
-- DATEDIFF、DATE_FORMAT、TO_DATE 等函数期望 DATE/STRING 输入
-- 传入 BIGINT 时间戳会触发隐式转换，结果可能完全错误
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.submit_time,
    j.start_time,
    j.end_time,
    -- ❌ DATEDIFF 期望 DATE 类型，传入 BIGINT 触发隐式转换
    DATEDIFF(j.end_time, j.start_time)        AS days_diff,
    -- ❌ DATE_FORMAT 期望 DATE/TIMESTAMP，传入 BIGINT
    DATE_FORMAT(j.submit_time, 'yyyy-MM-dd')  AS submit_date_str,
    -- ❌ TO_DATE 传入 BIGINT
    TO_DATE(j.start_time)                     AS start_date,
    -- 正确做法：先除以 1000 转为秒级时间戳，再用 FROM_UNIXTIME
    FROM_UNIXTIME(j.submit_time / 1000, 'yyyy-MM-dd HH:mm:ss')
                                               AS submit_time_correct,
    -- ❌ YEAR/MONTH/DAY 函数传入 BIGINT
    YEAR(j.submit_time)                        AS submit_year,
    MONTH(j.start_time)                        AS start_month,
    DAY(j.end_time)                            AS end_day,
    -- ❌ BIGINT 时间戳之间用 DATEDIFF 而非直接相减
    DATEDIFF(j.end_time, j.submit_time)       AS total_days
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.status = 'FAILED'
  -- ❌ BIGINT 与 DATE 字面量比较
  AND j.submit_time > DATE '2026-03-08'
ORDER BY j.submit_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 9c: 跨表 JOIN 中时间字段类型混用
-- 用 FROM_UNIXTIME 将一侧转为 STRING，另一侧保持 BIGINT
-- JOIN 条件中两侧类型不一致触发隐式转换
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.start_time                              AS app_start_time,
    j.job_id,
    j.submit_time                             AS job_submit_time,
    j.start_time                              AS job_start_time,
    s.stage_id,
    s.submit_time                             AS stage_submit_time,
    -- 计算各层级的排队时间
    (j.start_time - j.submit_time)            AS job_queue_ms,
    (s.start_time - s.submit_time)            AS stage_queue_ms,
    -- ❌ 用 FROM_UNIXTIME 结果（STRING）与 BIGINT 做运算
    FROM_UNIXTIME(a.start_time / 1000) - j.submit_time
                                               AS mixed_time_diff
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
    -- ❌ 一侧 FROM_UNIXTIME (返回 STRING) 与另一侧 BIGINT 比较
    AND FROM_UNIXTIME(a.start_time / 1000) <= j.submit_time
INNER JOIN spark_analytics.spark_stage_metrics s
    ON j.app_id = s.app_id
    AND j.dt = s.dt
    -- ❌ 字符串日期与 BIGINT 时间戳比较
    AND '2026-03-08' <= s.submit_time
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY a.start_time DESC
LIMIT 50;
