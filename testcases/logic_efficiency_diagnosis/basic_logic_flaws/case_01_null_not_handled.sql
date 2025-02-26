-- ============================================================================
-- Case 01: NULL 值未正确处理
-- ============================================================================
-- 【问题描述】
--   NULL 在 SQL 中代表"未知值"，参与比较和运算时有特殊行为：
--     1. NULL 与任何值比较结果为 NULL（非 TRUE 也非 FALSE），WHERE 中被过滤掉
--     2. NULL 参与算术运算（+、-、*、/）结果仍为 NULL
--     3. COUNT(*) 统计所有行，但 COUNT(column) 会跳过 NULL
--     4. NOT IN 子查询若含 NULL 值，整个过滤条件可能返回空集
--     5. DISTINCT、GROUP BY 中 NULL 值被视为同一组
--   在数仓研发中，上游数据源经常出现字段为 NULL 的情况（如 end_time 任务
--   未结束、failed_reason 成功任务无失败原因），不处理会导致静默丢数或指标失真。
--
-- 【易犯场景】
--   1. 未结束的任务 end_time 为 NULL，直接用 end_time - start_time 算时长
--   2. 用 != / <> 过滤时丢失了 NULL 行
--   3. NOT IN 子查询中包含 NULL，导致主查询返回空
--   4. LEFT JOIN 后对右表字段做非空比较，丢失未关联的行
--   5. COALESCE / NVL / IF 等处理函数未使用
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - 存在可能为 NULL 的列参与比较或运算但未做空值处理
--   - 建议使用 COALESCE / NVL / IS NULL / IS NOT NULL 保护
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 1a: end_time 为 NULL 导致时长计算结果为 NULL，聚合指标偏低
-- 未结束的 app 其 end_time 可能为 NULL，直接计算 duration 会得到 NULL，
-- AVG 会跳过这些行，导致平均时长计算偏低
-- ❌ 错误：未处理 end_time 为 NULL 的情况
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    a.`user`,
    COUNT(*)                                                AS app_count,
    AVG(a.end_time - a.start_time)                          AS avg_duration_ms,  -- ❌ NULL 行被 AVG 跳过
    MAX(a.end_time - a.start_time)                          AS max_duration_ms,  -- ❌ NULL 行被 MAX 跳过
    SUM(a.end_time - a.start_time)                          AS total_duration_ms -- ❌ NULL 参与 SUM 被忽略
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.platform, a.`user`
ORDER BY avg_duration_ms DESC
LIMIT 100;

-- ✅ 正确写法：用 COALESCE 处理 NULL 值，或先过滤掉未结束的 app
-- AVG(COALESCE(a.end_time, UNIX_TIMESTAMP() * 1000) - a.start_time) AS avg_duration_ms
-- 或者：WHERE a.end_time IS NOT NULL


-- ---------------------------------------------------------------------------
-- Case 1b: 用 != 过滤时丢失 NULL 行
-- failed_reason 字段在任务成功时为 NULL，用 != 'OOM' 过滤时
-- NULL 行（成功任务）也会被丢掉，结果只剩非 OOM 的失败任务
-- ❌ 错误：!= 无法匹配 NULL，丢失了成功的任务
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.failed_reason,
    j.submit_time,
    j.start_time,
    j.end_time,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.failed_reason != 'OutOfMemoryError'                 -- ❌ NULL 行被丢弃
ORDER BY j.submit_time;

-- ✅ 正确写法：显式处理 NULL
-- AND (j.failed_reason != 'OutOfMemoryError' OR j.failed_reason IS NULL)
-- 或：AND COALESCE(j.failed_reason, '') != 'OutOfMemoryError'


-- ---------------------------------------------------------------------------
-- Case 1c: NOT IN 子查询包含 NULL 值，导致主查询返回空集
-- 如果子查询的 app_id 中有 NULL，NOT IN 的结果全部变为 UNKNOWN
-- ❌ 错误：NOT IN 子查询结果含 NULL 导致空结果
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    a.executor_num,
    a.executor_memory,
    a.start_time,
    a.end_time
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id NOT IN (                                     -- ❌ 如果子查询有 NULL，整个条件失效
        SELECT t.app_id                                     -- ❌ app_id 可能为 NULL
        FROM spark_analytics.spark_task_metrics t
        WHERE t.dt = '20260308'
          AND t.status = 'FAILED'
      )
ORDER BY a.start_time;

-- ✅ 正确写法：改用 NOT EXISTS 或在子查询中过滤 NULL
-- AND NOT EXISTS (
--     SELECT 1 FROM spark_analytics.spark_task_metrics t
--     WHERE t.app_id = a.app_id AND t.dt = '20260308' AND t.status = 'FAILED'
-- )
-- 或：AND a.app_id NOT IN (SELECT t.app_id FROM ... WHERE t.app_id IS NOT NULL AND ...)


-- ---------------------------------------------------------------------------
-- Case 1d: NULL 参与字符串拼接导致整条结果变 NULL
-- 拼接 app_name 和 user 时，如果 app_name 为 NULL，CONCAT 结果为 NULL
-- ❌ 错误：CONCAT 中有 NULL 参数导致整个结果为 NULL
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    CONCAT(a.app_name, '_', a.`user`, '_', a.platform)     AS app_label,   -- ❌ 任一字段为 NULL 结果即为 NULL
    a.result,
    a.executor_num,
    a.executor_memory,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY duration_ms DESC
LIMIT 50;

-- ✅ 正确写法：使用 CONCAT_WS 或对每个字段 COALESCE
-- CONCAT_WS('_', COALESCE(a.app_name, 'UNKNOWN'), a.`user`, COALESCE(a.platform, 'UNKNOWN'))


-- ---------------------------------------------------------------------------
-- Case 1e: LEFT JOIN 后对右表字段做 NULL 不安全的运算
-- LEFT JOIN 后右表未匹配的行字段全为 NULL，直接计算会丢失或失真
-- ❌ 错误：LEFT JOIN 后对 j.end_time 做运算，未匹配行变 NULL
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    COUNT(j.job_id)                                         AS job_count,
    AVG(j.end_time - j.start_time)                          AS avg_job_duration, -- ❌ 未匹配行全为 NULL
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END)   AS failed_jobs,
    SUM(j.end_time - j.start_time)                          AS total_job_time    -- ❌ NULL 传播
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name, a.`user`, a.result
HAVING AVG(j.end_time - j.start_time) > 60000              -- ❌ 无 job 的 app 其 AVG 为 NULL，被 HAVING 过滤
ORDER BY avg_job_duration DESC;

-- ✅ 正确写法：对右表字段加 COALESCE 保护
-- AVG(COALESCE(j.end_time, 0) - COALESCE(j.start_time, 0)) AS avg_job_duration
-- HAVING COALESCE(AVG(j.end_time - j.start_time), 0) > 60000
