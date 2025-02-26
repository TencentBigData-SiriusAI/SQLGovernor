-- ============================================================================
-- Case 06: 边界值差一错误（Off-by-One）
-- ============================================================================
-- 【问题描述】
--   边界值差一是编程中最经典的逻辑疏漏，在 SQL 中表现为：
--     1. > 和 >= 混用：少/多算了一个边界值
--     2. BETWEEN 包含两端：误以为左闭右开
--     3. 分区日期范围差一天：少一天或多一天的数据
--     4. 时间戳比较时毫秒/秒单位混淆导致范围错误
--     5. LIMIT/OFFSET 计算错误导致漏行或重复
--   在数仓场景中，边界值错误会导致指标微小偏差，很难被发现。
--
-- 【易犯场景】
--   1. 统计某日数据用 >= 当日 00:00 AND < 次日 00:00，写成 <= 当日 23:59:59
--   2. BETWEEN 两端都包含，但开发者认为右端不包含
--   3. 分区日期用 >= '20260301' AND <= '20260307'，少了最后一天
--   4. 毫秒级时间戳和秒级时间戳混用，阈值差 1000 倍
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - 边界条件可能存在差一错误
--   - 建议确认比较运算符和 BETWEEN 的边界语义
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 6a: > 和 >= 混用，漏掉了恰好等于阈值的记录
-- 业务需求：统计运行时长大于等于 10 分钟的 app
-- ❌ 错误：用了 > 600000 而非 >= 600000，漏掉恰好 10 分钟的 app
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    (a.end_time - a.start_time)                             AS duration_ms,
    ROUND((a.end_time - a.start_time) / 1000.0 / 60, 2)    AS duration_min,
    a.executor_num,
    a.executor_memory
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND (a.end_time - a.start_time) > 600000                  -- ❌ 应为 >= 600000，漏掉恰好 10 分钟的
  AND a.result = 0
ORDER BY duration_ms DESC
LIMIT 100;

-- ✅ 正确写法：
-- AND (a.end_time - a.start_time) >= 600000


-- ---------------------------------------------------------------------------
-- Case 6b: BETWEEN 包含两端导致多算了一天数据
-- 业务需求：统计 3月1日至3月7日（7天）的 app 数量
-- ❌ 错误：BETWEEN 包含两端，'20260301' 到 '20260308' 实际是 8 天
-- ---------------------------------------------------------------------------
SELECT
    a.dt,
    COUNT(*)                                                AS app_count,
    SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END)          AS success_count,
    SUM(CASE WHEN a.result != 0 THEN 1 ELSE 0 END)         AS fail_count,
    AVG(a.end_time - a.start_time)                          AS avg_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt BETWEEN '20260301' AND '20260308'   -- ❌ 含两端，实际 8 天而非 7 天
GROUP BY a.dt
ORDER BY a.dt;

-- ✅ 正确写法：BETWEEN 两端都包含，右端应为 '20260307'
-- WHERE a.dt BETWEEN '20260301' AND '20260307'
-- 或使用半开区间：WHERE a.dt >= '20260301' AND a.dt < '20260308'


-- ---------------------------------------------------------------------------
-- Case 6c: 时间戳单位混淆（毫秒 vs 秒），阈值差 1000 倍
-- 业务需求：找出排队时间超过 5 分钟的 job
-- start_time 和 submit_time 是毫秒级时间戳，但阈值写成了秒
-- ❌ 错误：300 是秒，但字段是毫秒，应该用 300000
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.submit_time,
    j.start_time,
    (j.start_time - j.submit_time)                          AS queue_time_ms,
    (j.end_time - j.start_time)                             AS run_time_ms,
    j.failed_reason
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND (j.start_time - j.submit_time) > 300                  -- ❌ 字段为毫秒，阈值应为 300000
ORDER BY queue_time_ms DESC
LIMIT 100;

-- ✅ 正确写法：统一使用毫秒
-- AND (j.start_time - j.submit_time) > 300000


-- ---------------------------------------------------------------------------
-- Case 6d: 分区日期范围差一，少统计了一天
-- 业务需求：统计最近 7 天（含今天 20260308）的趋势
-- ❌ 错误：起始日期算错，20260308 - 7 天应从 20260302 开始，而非 20260301
-- ---------------------------------------------------------------------------
SELECT
    a.dt,
    a.platform,
    COUNT(DISTINCT a.app_id)                                AS app_count,
    COUNT(DISTINCT a.`user`)                                AS user_count,
    AVG(a.executor_num)                                     AS avg_executors,
    AVG(a.executor_memory)                                  AS avg_memory
FROM spark_analytics.spark_app_metrics a
WHERE a.dt >= '20260301'                       -- ❌ 实际是 8 天而非 7 天（0301~0308）
  AND a.dt <= '20260308'
GROUP BY a.dt, a.platform
ORDER BY a.dt, a.platform;

-- ✅ 正确写法：最近 7 天含 0308 应从 0302 开始
-- WHERE a.dt >= '20260302' AND a.dt <= '20260308'


-- ---------------------------------------------------------------------------
-- Case 6e: task 耗时分桶边界不连续，造成数据遗漏
-- 业务需求：将 task 按运行时间分桶统计
-- ❌ 错误：分桶边界有间隙，恰好等于边界的 task 可能被漏掉
-- ---------------------------------------------------------------------------
SELECT
    CASE
        WHEN t.task_run_time < 1000 THEN '0-1s'
        WHEN t.task_run_time > 1000 AND t.task_run_time < 10000 THEN '1-10s'     -- ❌ task_run_time = 1000 被漏掉
        WHEN t.task_run_time > 10000 AND t.task_run_time < 60000 THEN '10-60s'   -- ❌ task_run_time = 10000 被漏掉
        WHEN t.task_run_time > 60000 AND t.task_run_time < 300000 THEN '1-5min'  -- ❌ task_run_time = 60000 被漏掉
        WHEN t.task_run_time > 300000 THEN '5min+'                                -- ❌ task_run_time = 300000 被漏掉
    END                                                     AS time_bucket,
    COUNT(*)                                                AS task_count,
    AVG(t.gc_time)                                          AS avg_gc_time,
    AVG(t.executor_cpu_time)                                AS avg_cpu_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
GROUP BY
    CASE
        WHEN t.task_run_time < 1000 THEN '0-1s'
        WHEN t.task_run_time > 1000 AND t.task_run_time < 10000 THEN '1-10s'
        WHEN t.task_run_time > 10000 AND t.task_run_time < 60000 THEN '10-60s'
        WHEN t.task_run_time > 60000 AND t.task_run_time < 300000 THEN '1-5min'
        WHEN t.task_run_time > 300000 THEN '5min+'
    END
ORDER BY task_count DESC;

-- ✅ 正确写法：用 >= 确保边界连续无间隙
-- WHEN t.task_run_time < 1000 THEN '0-1s'
-- WHEN t.task_run_time >= 1000 AND t.task_run_time < 10000 THEN '1-10s'
-- WHEN t.task_run_time >= 10000 AND t.task_run_time < 60000 THEN '10-60s'
-- WHEN t.task_run_time >= 60000 AND t.task_run_time < 300000 THEN '1-5min'
-- WHEN t.task_run_time >= 300000 THEN '5min+'
