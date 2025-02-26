-- ============================================================================
-- Case 07: 窗口函数（Window Function）语法错误
-- ============================================================================
-- 【问题描述】
--   窗口函数是数仓 SQL 中高频使用但语法复杂的特性，常见错误包括：
--     1. 缺少 OVER 子句或 OVER() 内容为空时的误用
--     2. PARTITION BY 后列名拼写错误或引用错误
--     3. ORDER BY 遗漏导致 ROW_NUMBER 结果不确定
--     4. ROWS/RANGE 窗口框架语法错误
--     5. 在 WHERE 子句中直接使用窗口函数（不允许）
--     6. 窗口函数嵌套（不允许）
--
-- 【易犯场景】
--   1. 写 ROW_NUMBER() 忘了 OVER(...)
--   2. RANK/DENSE_RANK 在 OVER 中漏写 ORDER BY，结果无意义
--   3. 想在 WHERE 中过滤窗口函数结果，但窗口函数不能在 WHERE 中使用
--   4. LAG/LEAD 参数个数写错
--   5. ROWS BETWEEN 语法格式写反或关键字拼错
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - 窗口函数语法不合法
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 7a: ROW_NUMBER() 缺少 OVER 子句
-- 研发人员想对 task 按运行时间排序，但忘了 OVER(...)
-- ❌ 错误：ROW_NUMBER() 必须跟 OVER(PARTITION BY ... ORDER BY ...)
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time,
    ROW_NUMBER()                      AS rn          -- ❌ 缺少 OVER(...)
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.status = 'SUCCESS'
  AND t.task_run_time > 0
ORDER BY t.task_run_time DESC
LIMIT 100;

-- ✅ 正确写法：
-- ROW_NUMBER() OVER(PARTITION BY t.app_id ORDER BY t.task_run_time DESC) AS rn


-- ---------------------------------------------------------------------------
-- Case 7b: RANK() 的 OVER 子句中缺少 ORDER BY
-- 没有 ORDER BY 的 RANK() 没有实际意义，部分引擎会报错
-- ❌ 错误：RANK() OVER() 必须包含 ORDER BY
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.num_tasks,
    s.start_time,
    s.end_time,
    (s.end_time - s.start_time)       AS stage_duration,
    RANK() OVER(PARTITION BY s.app_id) AS stage_rank  -- ❌ OVER 中缺 ORDER BY
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.num_tasks > 0
ORDER BY s.app_id, stage_duration DESC
LIMIT 200;

-- ✅ 正确写法：
-- RANK() OVER(PARTITION BY s.app_id ORDER BY (s.end_time - s.start_time) DESC)


-- ---------------------------------------------------------------------------
-- Case 7c: WHERE 子句中直接使用窗口函数
-- 想过滤出每个 app 中排名前3的 job，但在 WHERE 中直接写窗口函数
-- ❌ 错误：窗口函数不能出现在 WHERE 子句中，需包一层子查询
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.submit_time,
    j.start_time,
    j.end_time,
    (j.end_time - j.start_time)       AS job_duration,
    j.failed_reason,
    ROW_NUMBER() OVER(
        PARTITION BY j.app_id
        ORDER BY (j.end_time - j.start_time) DESC
    )                                  AS rn
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND ROW_NUMBER() OVER(              -- ❌ WHERE 中不能使用窗口函数
        PARTITION BY j.app_id
        ORDER BY (j.end_time - j.start_time) DESC
      ) <= 3
ORDER BY j.app_id, job_duration DESC;

-- ✅ 正确写法：包一层子查询
-- SELECT * FROM (
--     SELECT ..., ROW_NUMBER() OVER(...) AS rn FROM ...
-- ) sub WHERE sub.rn <= 3


-- ---------------------------------------------------------------------------
-- Case 7d: LAG/LEAD 函数参数错误 + 窗口框架语法错误
-- LAG 函数参数拼写错误，ROWS BETWEEN 语法关键字写反
-- ❌ 错误：LAG 参数格式不对 + ROWS BETWEEN 写法错误
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.start_time,
    a.end_time,
    (a.end_time - a.start_time)       AS duration_ms,
    -- LAG 参数应为 (列, 偏移量, 默认值)
    LAG(a.end_time - a.start_time)
        OVER(PARTITION BY a.`user`
             ORDER BY a.start_time
        )                              AS prev_duration,  -- ✅ 这个写法OK
    -- ❌ 尝试用 ROWS BETWEEN 但关键字拼错
    SUM(a.executor_num)
        OVER(PARTITION BY a.`user`
             ORDER BY a.start_time
             ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING  -- ✅ 正确
        )                              AS rolling_executors,
    AVG(a.executor_memory)
        OVER(PARTITION BY a.`user`
             ORDER BY a.start_time
             ROWS BETWEEN UNBOUNDED PRECEDING TO CURRENT ROW  -- ❌ TO 应为 AND
        )                              AS cumul_avg_memory
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
ORDER BY a.`user`, a.start_time
LIMIT 500;

-- ✅ 正确写法：
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW


-- ---------------------------------------------------------------------------
-- Case 7e: 窗口函数嵌套使用（不允许）
-- 尝试在一个窗口函数内嵌套另一个窗口函数
-- ❌ 错误：窗口函数不可嵌套
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    -- ❌ SUM 窗口函数内嵌套了 ROW_NUMBER 窗口函数
    SUM(
        ROW_NUMBER() OVER(PARTITION BY t.stage_id ORDER BY t.task_run_time)
    ) OVER(PARTITION BY t.app_id)     AS nested_window_error
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.status = 'SUCCESS'
ORDER BY t.app_id, t.task_run_time DESC
LIMIT 100;

-- ✅ 正确写法：先在子查询中计算内层窗口函数，再在外层使用
-- SELECT ..., SUM(rn) OVER(...) FROM (
--     SELECT ..., ROW_NUMBER() OVER(...) AS rn FROM ...
-- ) sub
