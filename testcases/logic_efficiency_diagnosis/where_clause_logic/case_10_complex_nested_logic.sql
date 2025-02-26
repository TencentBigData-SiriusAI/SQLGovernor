-- ============================================================================
-- Case 10: 复杂嵌套条件逻辑错误
-- ============================================================================
-- 【问题描述】
--   当 WHERE 条件包含多层嵌套的 AND/OR/NOT/EXISTS/IN 组合时，
--   逻辑复杂度急剧上升，极易出现语义偏差：
--     1. 多层括号嵌套导致作用域混乱
--     2. NOT 与 EXISTS/IN 组合的德摩根转换错误
--     3. CASE WHEN 嵌入 WHERE 导致可读性极差且 ELSE 缺失
--     4. 多个 OR 分支条件不完整
--
-- 【易犯场景】
--   1. 需求变更导致 WHERE 反复修改，逻辑越来越复杂
--   2. 多人协作开发，各自添加条件但未整体审视
--   3. 用 NOT 取反复杂条件时德摩根定律应用错误
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - WHERE 条件嵌套过深，逻辑可能有误
--   - 建议拆分为 CTE 或简化条件表达式
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 10a: 多层括号嵌套导致作用域混乱
-- 业务需求：查找（失败且超时）或（大内存且多executor）的 app，限定 spark
-- ❌ 错误：platform 条件因 AND 优先级只约束第二个 OR 分支
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    a.executor_num,
    a.executor_memory,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND (
        (a.result != 0 AND (a.end_time - a.start_time) > 600000)
        OR
        (a.executor_memory > 8192 AND a.executor_num > 50)
        AND a.platform = 'spark'                            -- ❌ 只约束第二个分支
      )
ORDER BY duration_ms DESC
LIMIT 100;

-- ✅ 正确写法：
-- AND a.platform = 'spark'
-- AND ((a.result != 0 AND ...) OR (a.executor_memory > 8192 AND ...))


-- ---------------------------------------------------------------------------
-- Case 10b: NOT EXISTS 与 OR 组合时德摩根错误
-- 业务需求：找出"没有失败 job 且没有长耗时 stage"的 app
-- ❌ 错误：NOT(A OR B) 应为 NOT A AND NOT B，但写成了 OR
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND (
        NOT EXISTS (
            SELECT 1 FROM spark_analytics.spark_job_metrics j
            WHERE j.app_id = a.app_id AND j.dt = '20260308'
              AND j.status = 'FAILED'
        )
        OR                                                  -- ❌ 应该是 AND
        NOT EXISTS (
            SELECT 1 FROM spark_analytics.spark_stage_metrics s
            WHERE s.app_id = a.app_id AND s.dt = '20260308'
              AND (s.end_time - s.start_time) > 600000
        )
      )
ORDER BY a.app_id;

-- ✅ 正确写法：两个 NOT EXISTS 用 AND 连接
-- NOT EXISTS (...) AND NOT EXISTS (...)


-- ---------------------------------------------------------------------------
-- Case 10c: CASE WHEN 嵌入 WHERE 缺少 ELSE，未匹配的行被过滤
-- 业务需求：根据 app 类型动态过滤不同阈值
-- ❌ 错误：CASE WHEN 缺少 ELSE，不满足任何 WHEN 的行返回 NULL
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.executor_num,
    a.executor_memory,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND CASE
        WHEN a.executor_memory > 8192 THEN (a.end_time - a.start_time) > 300000
        WHEN a.executor_memory > 4096 THEN (a.end_time - a.start_time) > 600000
        -- ❌ 缺少 ELSE 分支，executor_memory <= 4096 的 app 返回 NULL 被过滤
      END
ORDER BY duration_ms DESC;

-- ✅ 正确写法：添加 ELSE 分支
-- ELSE (a.end_time - a.start_time) > 900000
-- END


-- ---------------------------------------------------------------------------
-- Case 10d: 多 OR 分支中某些分支缺少关键过滤
-- 业务需求：查找异常 task（GC过高 或 CPU过高 或 运行超时）
-- ❌ 错误：第三个 OR 分支漏了分区过滤
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_task_metrics t
WHERE (
        (t.dt = '20260308' AND t.gc_time > 60000)
        OR
        (t.dt = '20260308' AND t.executor_cpu_time > 300000)
        OR
        (t.task_run_time > 1800000)                         -- ❌ 缺少分区过滤，扫全表
      )
  AND t.status = 'SUCCESS'
ORDER BY t.task_run_time DESC
LIMIT 200;

-- ✅ 正确写法：分区过滤提到外层
-- WHERE t.dt = '20260308'
--   AND (t.gc_time > 60000 OR t.executor_cpu_time > 300000 OR t.task_run_time > 1800000)


-- ---------------------------------------------------------------------------
-- Case 10e: 复杂 CTE + WHERE 条件引用错误
-- 业务需求：用 CTE 分层查询后在最终 WHERE 中组合过滤
-- ❌ 错误：最终 WHERE 引用了 CTE 中不存在的列别名
-- ---------------------------------------------------------------------------
WITH failed_apps AS (
    SELECT a.app_id, a.app_name, a.`user`, a.result,
           (a.end_time - a.start_time) AS app_duration
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308' AND a.result != 0
),
job_stats AS (
    SELECT j.app_id, COUNT(*) AS job_count,
           SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) AS fail_count
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
    GROUP BY j.app_id
)
SELECT
    fa.app_id, fa.app_name, fa.`user`,
    fa.app_duration, js.job_count, js.fail_count
FROM failed_apps fa
LEFT JOIN job_stats js ON fa.app_id = js.app_id
WHERE fa.app_duration > 600000
  AND js.fail_rate > 0.5                                    -- ❌ fail_rate 不存在于 job_stats CTE 中
  AND fa.platform = 'spark'                                 -- ❌ platform 不存在于 failed_apps CTE 中
ORDER BY fa.app_duration DESC;

-- ✅ 正确写法：引用 CTE 中实际存在的列
-- AND (js.fail_count * 1.0 / js.job_count) > 0.5
-- 在 CTE 中添加 platform 字段或在 CTE 内部过滤
