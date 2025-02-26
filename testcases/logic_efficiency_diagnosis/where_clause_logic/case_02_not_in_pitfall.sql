-- ============================================================================
-- Case 02: NOT IN 陷阱
-- ============================================================================
-- 【问题描述】
--   NOT IN 在 SQL 中有多个隐藏陷阱：
--     1. 子查询结果含 NULL 时，NOT IN 整体返回空集
--     2. 与 IN 的 NULL 行为不对称：IN 遇 NULL 只是不确定，NOT IN 全部失效
--     3. 大列表 NOT IN 性能极差，可能导致全表扫描
--     4. NOT IN 与 NOT EXISTS 语义在有 NULL 时不同
--     5. 空子查询时 NOT IN 返回所有行，而开发者可能期望返回空
--
-- 【易犯场景】
--   1. 子查询返回的列可能为 NULL（如未关联的 LEFT JOIN 结果）
--   2. 从业务字典表查数据做排除，但字典表有脏数据含 NULL
--   3. 手写 IN 列表时混入了 NULL
--   4. 把 NOT EXISTS 改写为 NOT IN 但没考虑 NULL 影响
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - NOT IN 子查询可能包含 NULL 值，导致结果集为空
--   - 建议改用 NOT EXISTS 或在子查询中过滤 NULL
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 2a: NOT IN 子查询结果含 NULL，整个查询返回空集
-- 业务需求：找出没有失败 task 的 app
-- ❌ 错误：task 表的 app_id 可能为 NULL，NOT IN 返回空集
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id NOT IN (                                     -- ❌ 如果子查询有 NULL，返回空集
        SELECT t.app_id                                     -- app_id 可能为 NULL
        FROM spark_analytics.spark_task_metrics t
        WHERE t.dt = '20260308'
          AND t.status = 'FAILED'
      )
ORDER BY duration_ms DESC;

-- ✅ 正确写法：改用 NOT EXISTS
-- AND NOT EXISTS (
--     SELECT 1 FROM spark_analytics.spark_task_metrics t
--     WHERE t.app_id = a.app_id AND t.dt = '20260308' AND t.status = 'FAILED'
-- )


-- ---------------------------------------------------------------------------
-- Case 2b: NOT IN 手写列表混入 NULL
-- 业务需求：排除特定几个 status 的 job
-- ❌ 错误：NULL 混入 NOT IN 列表，所有行都被过滤
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.status NOT IN ('FAILED', 'KILLED', NULL)            -- ❌ NULL 导致 NOT IN 返回空集
ORDER BY job_duration DESC;

-- ✅ 正确写法：移除 NULL，额外处理 NULL 情况
-- AND j.status NOT IN ('FAILED', 'KILLED')
-- AND j.status IS NOT NULL


-- ---------------------------------------------------------------------------
-- Case 2c: NOT IN 与嵌套子查询组合，子查询可能返回 NULL
-- 业务需求：找出没有关联 stage 的 app
-- ❌ 错误：LEFT JOIN 子查询中 stage 的 app_id 可能为 NULL
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id NOT IN (                                     -- ❌ 子查询结果含 NULL
        SELECT s.app_id
        FROM spark_analytics.spark_stage_metrics s
        LEFT JOIN spark_analytics.spark_task_metrics t     -- LEFT JOIN 可能产生 NULL
            ON s.app_id = t.app_id
            AND s.stage_id = t.stage_id
            AND s.dt = t.dt
        WHERE s.dt = '20260308'
          AND t.gc_time > 30000                             -- LEFT JOIN 后过滤右表，NULL 行被过滤
      );

-- ✅ 正确写法：子查询中显式过滤 NULL
-- SELECT s.app_id FROM ... WHERE s.app_id IS NOT NULL AND ...
-- 或改用 NOT EXISTS


-- ---------------------------------------------------------------------------
-- Case 2d: IN 与 NOT IN 对空子查询的行为差异
-- 业务需求：找出不属于"大内存 app"的 job
-- 如果子查询为空（比如今天没有大内存 app），NOT IN 返回全部
-- ❌ 错误：开发者期望空子查询时返回空，但实际返回全部
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.app_id NOT IN (
        SELECT a.app_id
        FROM spark_analytics.spark_app_metrics a
        WHERE a.dt = '20260308'
          AND a.executor_memory > 999999                    -- ❌ 条件极端，子查询可能为空
      )                                                     -- ❌ 空子查询 → NOT IN 返回全部，非预期
ORDER BY job_duration DESC
LIMIT 100;

-- ✅ 正确写法：根据业务语义选择
-- 若期望空子查询返回空集，改用 INNER JOIN：
-- INNER JOIN (SELECT app_id FROM app WHERE mem > 999999) big_app
--     ON j.app_id != big_app.app_id   -- 注意这也不对，应该用 NOT EXISTS


-- ---------------------------------------------------------------------------
-- Case 2e: 多列 NOT IN 的语义陷阱
-- 业务需求：排除特定 (app_id, stage_id) 组合的 task
-- ❌ 错误：SQL 不支持多列 NOT IN 元组语法（部分引擎支持但行为不一致）
-- ---------------------------------------------------------------------------
SELECT
    t.task_id,
    t.app_id,
    t.stage_id,
    t.status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND (t.app_id, t.stage_id) NOT IN (                      -- ❌ 多列 NOT IN，NULL 问题更严重
        SELECT s.app_id, s.stage_id
        FROM spark_analytics.spark_stage_metrics s
        WHERE s.dt = '20260308'
          AND s.status = 'FAILED'
      )
ORDER BY t.task_run_time DESC
LIMIT 200;

-- ✅ 正确写法：改用 NOT EXISTS 更安全
-- AND NOT EXISTS (
--     SELECT 1 FROM stage s
--     WHERE s.app_id = t.app_id AND s.stage_id = t.stage_id
--       AND s.dt = '20260308' AND s.status = 'FAILED'
-- )
