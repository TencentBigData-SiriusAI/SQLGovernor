-- ============================================================================
-- Case 02: 显式 CROSS JOIN 误用导致结果集膨胀
-- ============================================================================
-- 【问题描述】
--   CROSS JOIN 是 SQL 中唯一不需要 ON 条件的 JOIN 类型，它会产生两表
--   的笛卡尔积。在数仓开发中，CROSS JOIN 极少有合理场景，绝大多数
--   使用都是错误的：
--     1. 误将 CROSS JOIN 当作 INNER JOIN 使用
--     2. 本意是 JOIN 但复制代码时关键字写错
--     3. 想做维度展开但未意识到数据量会爆炸性增长
--     4. IDE 自动补全选错了 JOIN 类型
--
-- 【易犯场景】
--   1. 在 SQL 编辑器中自动补全选择了 CROSS JOIN
--   2. 从网上复制示例代码时未修改 JOIN 类型
--   3. 对 CROSS JOIN 语义理解不清，以为等同于 INNER JOIN
--   4. 写了 CROSS JOIN 后打算在 WHERE 中加条件，但忘了
--   5. 用于生成日期维度表与事实表关联时量级判断失误
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - 使用了 CROSS JOIN，将产生笛卡尔积
--   - 建议确认是否应使用 INNER JOIN / LEFT JOIN
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 2a: CROSS JOIN 代替 INNER JOIN
-- 本意是查 app 对应的 job，误用了 CROSS JOIN
-- ❌ 错误：CROSS JOIN 产生笛卡尔积，每个 app 会关联所有 job
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.executor_num,
    a.executor_memory,
    j.job_id,
    j.`action`,
    j.status                          AS job_status,
    j.submit_time,
    j.start_time                      AS job_start,
    j.end_time                        AS job_end,
    (j.end_time - j.start_time)       AS job_duration_ms,
    j.failed_reason
FROM spark_analytics.spark_app_metrics a
CROSS JOIN spark_analytics.spark_job_metrics j       -- ❌ 应为 INNER JOIN
WHERE a.dt = '20260308'
  AND j.dt = '20260308'
  AND a.result != 0
  AND j.status = 'FAILED'
ORDER BY j.submit_time DESC
LIMIT 100;

-- ✅ 正确写法：
-- INNER JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id
--     AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 2b: CROSS JOIN 后在 WHERE 中加了关联条件（低效写法）
-- 虽然 WHERE 中补了 app_id 关联，但执行计划仍可能先做笛卡尔积再过滤
-- ❌ 错误：即使 WHERE 有关联条件，CROSS JOIN 语义上仍是笛卡尔积
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    s.stage_id,
    s.stage_attempt_id,
    s.num_tasks,
    s.status                          AS stage_status,
    (s.end_time - s.start_time)       AS stage_duration_ms,
    CASE
        WHEN s.num_tasks > 0
        THEN ROUND((s.end_time - s.start_time) / s.num_tasks, 2)
        ELSE 0
    END                               AS avg_task_duration
FROM spark_analytics.spark_app_metrics a
CROSS JOIN spark_analytics.spark_stage_metrics s     -- ❌ 应为 INNER JOIN
WHERE a.dt = '20260308'
  AND s.dt = '20260308'
  AND a.app_id = s.app_id                              -- 关联条件在 WHERE 中
ORDER BY stage_duration_ms DESC
LIMIT 200;

-- ✅ 正确写法：改为 INNER JOIN ... ON
-- INNER JOIN spark_analytics.spark_stage_metrics s
--     ON a.app_id = s.app_id
--     AND a.dt = s.dt


-- ---------------------------------------------------------------------------
-- Case 2c: 链式 CROSS JOIN —— 三表依次 CROSS JOIN
-- 三张表依次 CROSS JOIN，结果集指数膨胀
-- ❌ 错误：三重 CROSS JOIN，10000 × 50000 × 200000 = 超大结果集
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.status                          AS job_status,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    (j.end_time - j.start_time)       AS job_duration,
    (s.end_time - s.start_time)       AS stage_duration
FROM spark_analytics.spark_app_metrics a
CROSS JOIN spark_analytics.spark_job_metrics j       -- ❌ 第一个 CROSS JOIN
CROSS JOIN spark_analytics.spark_stage_metrics s     -- ❌ 第二个 CROSS JOIN
WHERE a.dt = '20260308'
  AND j.dt = '20260308'
  AND s.dt = '20260308'
  AND a.result != 0
LIMIT 500;

-- ✅ 正确写法：
-- FROM a
-- INNER JOIN j ON a.app_id = j.app_id AND a.dt = j.dt
-- INNER JOIN s ON a.app_id = s.app_id AND a.dt = s.dt
