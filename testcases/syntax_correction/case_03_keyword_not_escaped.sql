-- ============================================================================
-- Case 03: SQL 关键字/保留字作为列名未用反引号转义
-- ============================================================================
-- 【问题描述】
--   当表中的字段名恰好是 SQL 保留字（如 user、action、status、result、
--   timestamp 等）时，在 SQL 中引用这些字段必须用反引号（``）转义。
--   否则解析器会将其视为关键字而非列名，导致语法解析失败。
--   常见保留字冲突字段：
--     - app 表：user, result, timestamp
--     - job 表：action, status
--     - stage 表：status
--     - task 表：status
--
-- 【易犯场景】
--   1. user 是最高频被误用的保留字，因为太常用而忘记它是关键字
--   2. action 在部分引擎中是保留字
--   3. status 在某些 SQL 方言中是保留字
--   4. result、timestamp 在标准 SQL 中是保留字
--   5. 从其他引擎迁移过来的 SQL，原引擎不需要转义
--   6. ORM/代码生成器生成的 SQL 未做保留字检测
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - 列名与 SQL 保留字冲突，需使用反引号转义
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 3a: user 未转义 —— 最常见的保留字冲突
-- user 是 SQL 保留字，直接使用 a.user 而非 a.`user` 会报语法错误
-- ❌ 错误：user 必须用反引号转义
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.user,                           -- ❌ user 是保留字，应为 a.`user`
    a.result,
    a.platform,
    a.executor_num,
    a.executor_memory,
    a.executor_cores,
    a.start_time,
    a.end_time,
    ROUND((a.end_time - a.start_time) / 1000, 2) AS duration_sec,
    a.driver_memory,
    a.driver_cores,
    a.rss_enabled
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.user = 'data_team'            -- ❌ WHERE 中也需要转义
  AND a.result != 0
ORDER BY a.end_time DESC
LIMIT 100;

-- ✅ 正确写法：
-- a.`user` = 'data_team'


-- ---------------------------------------------------------------------------
-- Case 3b: 多字段保留字同时未转义
-- 查询中同时引用 user, result, timestamp 多个保留字字段，全部未转义
-- ❌ 错误：多个保留字字段全部未加反引号
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.user,                           -- ❌ 保留字
    a.result,                         -- ❌ 部分引擎中是保留字
    a.timestamp,                      -- ❌ 保留字
    a.platform,
    a.executor_num
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.user IN ('user_a', 'user_b', 'user_c')    -- ❌ 未转义
  AND a.result = 0
  AND a.timestamp > 1709856000000     -- ❌ 未转义
ORDER BY a.timestamp DESC;            -- ❌ 未转义

-- ✅ 正确写法：
-- a.`user`, a.`result`, a.`timestamp`


-- ---------------------------------------------------------------------------
-- Case 3c: GROUP BY / ORDER BY 中保留字未转义
-- 按 user 分组统计时，GROUP BY 和 ORDER BY 中也需要转义
-- ❌ 错误：GROUP BY 和 HAVING 中的 user 未转义
-- ---------------------------------------------------------------------------
SELECT
    a.user,                           -- ❌ 未转义
    a.platform,
    COUNT(*)                          AS app_count,
    SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END)  AS success_cnt,
    AVG(a.executor_num)               AS avg_executors,
    AVG(a.executor_memory)            AS avg_memory,
    MAX(a.end_time - a.start_time)    AS max_duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.user, a.platform           -- ❌ GROUP BY 中未转义
ORDER BY a.user;                      -- ❌ ORDER BY 中未转义

-- ✅ 正确写法：
-- GROUP BY a.`user`, a.platform
-- ORDER BY a.`user`


-- ---------------------------------------------------------------------------
-- Case 3d: JOIN 条件中保留字未转义 + 多表场景
-- app 表和 job 表 JOIN 时，ON 条件和 SELECT 中多个保留字字段未转义
-- ❌ 错误：跨多表的保留字字段全部未转义
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.user,                           -- ❌ 未转义
    j.job_id,
    j.action,                         -- ❌ 部分引擎中是保留字
    j.status,                         -- ❌ 部分引擎中是保留字
    j.submit_time,
    j.start_time,
    j.end_time,
    j.failed_reason,
    (j.end_time - j.start_time)       AS job_duration_ms
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
  AND a.user = 'pipeline_prod'        -- ❌ 未转义
  AND j.status = 'FAILED'             -- ❌ 部分引擎需转义
ORDER BY a.user, j.action;            -- ❌ 未转义

-- ✅ 正确写法：
-- a.`user`, j.`action`, j.`status`


-- ---------------------------------------------------------------------------
-- Case 3e: 子查询 + CTE 中保留字传播未转义
-- CTE 定义中使用了保留字列名，后续引用时也需转义
-- ❌ 错误：CTE 中定义时及引用时保留字均未转义
-- ---------------------------------------------------------------------------
WITH user_stats AS (
    SELECT
        a.user,                       -- ❌ 未转义
        COUNT(*)                      AS total_apps,
        SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END) AS success_apps,
        AVG(a.executor_num)           AS avg_executors
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
    GROUP BY a.user                   -- ❌ 未转义
)
SELECT
    us.user,                          -- ❌ 引用 CTE 列时也需转义
    us.total_apps,
    us.success_apps,
    ROUND(us.success_apps * 100.0 / us.total_apps, 2) AS success_rate,
    us.avg_executors
FROM user_stats us
WHERE us.total_apps > 10
ORDER BY us.user;                     -- ❌ 未转义

-- ✅ 正确写法：所有 user 引用处都用 `user`
