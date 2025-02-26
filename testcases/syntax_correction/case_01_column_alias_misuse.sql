-- ============================================================================
-- Case 01: 列别名（Alias）在 WHERE / GROUP BY / HAVING / JOIN ON 中误用
-- ============================================================================
-- 【问题描述】
--   在 SQL 中，SELECT 子句定义的列别名（alias）在逻辑执行顺序上晚于
--   WHERE / GROUP BY / HAVING / JOIN ON 子句。因此不能在这些子句中直接
--   引用 SELECT 中定义的别名。这是数仓研发中最高频的语法错误之一，因为：
--     1. 别名在视觉上已经定义，直觉上"应该可用"
--     2. 部分数据库（如 MySQL）对此较宽容，迁移到 Hive/Spark SQL 后报错
--     3. 在复杂多层嵌套查询中更容易混淆作用域
--     4. GROUP BY 在某些引擎中支持别名，但 WHERE 绝对不支持
--
-- 【易犯场景】
--   1. 从 MySQL 迁移到 Hive/SparkSQL，原有 SQL 中 WHERE 引用别名
--   2. 先写完 SELECT 再补 WHERE，复制别名而非原始表达式
--   3. HAVING 与 WHERE 混淆，在 WHERE 中使用聚合别名
--   4. JOIN ON 条件中引用外层 SELECT 别名
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - 别名在 WHERE / HAVING / JOIN ON 中不可引用
--   - 建议使用原始列名或表达式替代
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 1a: WHERE 子句中引用 SELECT 别名
-- 研发人员计算了 app 运行时长的别名 duration_sec，然后在 WHERE 中直接过滤
-- ❌ 错误：WHERE 子句在 SELECT 之前执行，无法识别 duration_sec 别名
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.executor_num,
    a.executor_memory,
    a.executor_cores,
    a.start_time,
    a.end_time,
    ROUND((a.end_time - a.start_time) / 1000, 2) AS duration_sec,     -- 定义别名
    a.driver_memory,
    a.driver_cores
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0
  AND duration_sec > 600              -- ❌ WHERE 中引用 SELECT 别名，语法错误
ORDER BY duration_sec DESC
LIMIT 100;

-- ✅ 正确写法：在 WHERE 中重复原始表达式
-- WHERE ROUND((a.end_time - a.start_time) / 1000, 2) > 600


-- ---------------------------------------------------------------------------
-- Case 1b: HAVING 子句中引用非聚合别名
-- 研发人员按 user 统计 app 数量，在 HAVING 中引用了 SELECT 的计算列别名
-- ❌ 错误：avg_duration 是 SELECT 中的别名，HAVING 无法识别
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.platform,
    COUNT(*)                                        AS app_count,
    AVG(a.end_time - a.start_time)                  AS avg_duration,   -- 别名
    MAX(a.executor_num)                             AS max_executors,
    SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END)  AS success_count,
    SUM(CASE WHEN a.result != 0 THEN 1 ELSE 0 END) AS fail_count
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.`user`, a.platform
HAVING avg_duration > 300000          -- ❌ HAVING 中引用 SELECT 别名
   AND app_count > 5                  -- ❌ 同上，app_count 也是别名
ORDER BY app_count DESC;

-- ✅ 正确写法：在 HAVING 中重复聚合表达式
-- HAVING AVG(a.end_time - a.start_time) > 300000
--    AND COUNT(*) > 5


-- ---------------------------------------------------------------------------
-- Case 1c: JOIN ON 条件中引用 SELECT 别名
-- 先对 job 表计算 job_duration，然后在 JOIN 条件中引用该别名
-- ❌ 错误：JOIN ON 子句无法访问 SELECT 中定义的别名
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.status                                        AS job_status,
    j.submit_time,
    j.start_time                                    AS job_start,
    j.end_time                                      AS job_end,
    (j.end_time - j.start_time)                     AS job_duration,   -- 别名
    (j.start_time - j.submit_time)                  AS queue_time,
    j.failed_reason
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
    AND job_duration > 60000          -- ❌ JOIN ON 中引用 SELECT 别名，语法错误
WHERE a.dt = '20260308'
  AND j.status = 'FAILED'
ORDER BY job_duration DESC
LIMIT 50;

-- ✅ 正确写法：在 JOIN ON 中使用原始表达式
-- AND (j.end_time - j.start_time) > 60000


-- ---------------------------------------------------------------------------
-- Case 1d: 嵌套子查询中外层别名在内层被引用
-- 外层查询定义了 app_type 别名，内层关联子查询试图引用外层别名
-- ❌ 错误：子查询不能引用外层 SELECT 的别名（只能引用外层表的列）
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    CASE
        WHEN a.executor_num > 100 THEN 'large'
        WHEN a.executor_num > 10 THEN 'medium'
        ELSE 'small'
    END                                             AS app_type,       -- 别名
    a.executor_num,
    a.executor_memory,
    (
        SELECT COUNT(*)
        FROM spark_analytics.spark_job_metrics j
        WHERE j.app_id = a.app_id
          AND j.dt = a.dt
          AND app_type = 'large'      -- ❌ 引用外层 SELECT 别名，语法错误
    )                                               AS job_count
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY a.executor_num DESC
LIMIT 100;

-- ✅ 正确写法：在子查询中重复 CASE 表达式或将外层包一层子查询
-- AND (CASE WHEN a.executor_num > 100 THEN 'large' ... END) = 'large'
