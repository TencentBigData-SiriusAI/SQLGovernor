-- ============================================================================
-- Case 10: 关联子查询逻辑错误
-- ============================================================================
-- 【问题描述】
--   关联子查询（Correlated Subquery）引用了外层查询的列，用于行级过滤
--   或计算。常见的逻辑错误包括：
--     1. 关联条件不足：子查询只关联了部分键，返回的结果集超出预期
--     2. 关联方向反转：内外层的关联条件写反了
--     3. 子查询返回多行但用在了需要标量的位置
--     4. EXISTS/NOT EXISTS 的关联条件遗漏导致逻辑失效
--     5. IN 子查询与关联子查询的语义混淆
--
-- 【易犯场景】
--   1. 关联子查询只关联了 app_id，但需要同时关联分区字段
--   2. 把外层表的列和内层表的列搞反
--   3. 标量子查询返回了多行导致运行时报错
--   4. EXISTS 子查询中忘记写关联条件，变成"是否有任意数据"
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - 关联子查询的关联条件可能不完整
--   - 建议检查内外层的关联键是否充分
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 10a: EXISTS 子查询漏写关联条件，变成"是否有任意失败 job"
-- 业务需求：找出有失败 job 的 app
-- ❌ 错误：EXISTS 内缺少 j.app_id = a.app_id，只要 job 表有任意失败记录
--   就返回所有 app
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    (a.end_time - a.start_time)                             AS app_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND EXISTS (
        SELECT 1
        FROM spark_analytics.spark_job_metrics j
        WHERE j.dt = '20260308'
          AND j.status = 'FAILED'
          -- ❌ 缺少 AND j.app_id = a.app_id 关联条件
          -- 只要 job 表有任何一条 FAILED 记录，所有 app 都会返回
      )
ORDER BY app_duration DESC;

-- ✅ 正确写法：添加关联条件
-- WHERE j.app_id = a.app_id AND j.dt = '20260308' AND j.status = 'FAILED'


-- ---------------------------------------------------------------------------
-- Case 10b: 标量子查询返回多行导致运行时错误
-- 业务需求：查每个 app 最长 job 的耗时
-- ❌ 错误：子查询可能返回多行（多个 job 同时最大），非标量
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    (a.end_time - a.start_time)                             AS app_duration,
    (
        SELECT (j.end_time - j.start_time)                  -- ❌ 可能返回多行
        FROM spark_analytics.spark_job_metrics j
        WHERE j.app_id = a.app_id
          AND j.dt = a.dt
        ORDER BY (j.end_time - j.start_time) DESC
        LIMIT 1                                             -- 虽然加了 LIMIT 1，但部分引擎不支持子查询中 LIMIT
    )                                                       AS max_job_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
ORDER BY app_duration DESC
LIMIT 100;

-- ✅ 正确写法：用聚合函数保证标量
-- (SELECT MAX(j.end_time - j.start_time)
--  FROM spark_analytics.spark_job_metrics j
--  WHERE j.app_id = a.app_id AND j.dt = a.dt)


-- ---------------------------------------------------------------------------
-- Case 10c: 关联条件不足，子查询跨分区匹配
-- 业务需求：统计每个 app 当天的 task 总 GC 时间
-- ❌ 错误：子查询只关联了 app_id，未关联分区，匹配了所有日期的 task
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    (a.end_time - a.start_time)                             AS app_duration,
    (
        SELECT SUM(t.gc_time)
        FROM spark_analytics.spark_task_metrics t
        WHERE t.app_id = a.app_id
          -- ❌ 缺少 AND t.dt = a.dt
          -- 子查询匹配了所有日期的 task，gc_time 被累加了历史数据
    )                                                       AS total_gc_time
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
ORDER BY total_gc_time DESC
LIMIT 100;

-- ✅ 正确写法：关联条件包含分区字段
-- WHERE t.app_id = a.app_id AND t.dt = a.dt


-- ---------------------------------------------------------------------------
-- Case 10d: 关联方向反转，内外层列写反
-- 业务需求：找出有长耗时 stage 的 job
-- ❌ 错误：关联条件写反了，s.app_id = s.app_id（自己关联自己）
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND EXISTS (
        SELECT 1
        FROM spark_analytics.spark_stage_metrics s
        WHERE s.app_id = s.app_id                           -- ❌ 写反了！应该是 s.app_id = j.app_id
          AND s.dt = '20260308'
          AND (s.end_time - s.start_time) > 600000
      )
ORDER BY job_duration DESC;

-- ✅ 正确写法：关联条件引用外层表
-- WHERE s.app_id = j.app_id AND s.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 10e: IN 子查询 vs EXISTS 语义混淆
-- 业务需求：找出有失败 task 的 stage
-- ❌ 错误：IN 子查询选了 task_id，但用 stage_id IN (...)，列不匹配
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.num_tasks,
    (s.end_time - s.start_time)                             AS stage_duration
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.stage_id IN (                                       -- ❌ 用 stage_id 去匹配 task_id
        SELECT t.task_id                                    -- ❌ 应该是 t.stage_id
        FROM spark_analytics.spark_task_metrics t
        WHERE t.dt = '20260308'
          AND t.status = 'FAILED'
      )
ORDER BY stage_duration DESC;

-- ✅ 正确写法：IN 子查询返回正确的列
-- AND s.stage_id IN (SELECT t.stage_id FROM ... WHERE t.status = 'FAILED')
-- 或改用 EXISTS：
-- AND EXISTS (SELECT 1 FROM task t WHERE t.stage_id = s.stage_id AND t.app_id = s.app_id AND ...)
