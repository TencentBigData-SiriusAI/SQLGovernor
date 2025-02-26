-- ============================================================================
-- Case 03: 分区过滤遗漏或不一致
-- ============================================================================
-- 【问题描述】
--   SparkSQL 表通常以 dt 作为分区字段，分区过滤是必须的：
--     1. 不加分区过滤会全表扫描，消耗巨大资源且速度极慢
--     2. 多表 JOIN 时各表的分区条件不一致，导致跨天数据混入
--     3. 子查询中漏加分区过滤，虽然外层有但子查询会扫全量
--     4. CTE 中的表忘记加分区条件
--     5. UNION ALL 的某个分支漏加分区条件
--   在数仓研发中，分区过滤遗漏是最高频且危害最大的逻辑疏漏之一。
--
-- 【易犯场景】
--   1. 复制粘贴时漏拷了 WHERE 条件中的分区过滤
--   2. 子查询或 CTE 认为"外层有分区过滤就够了"
--   3. 多表 JOIN 时一张表加了分区过滤，另一张漏了
--   4. INSERT OVERWRITE 时目标分区和源数据分区不一致
--   5. 动态分区场景下误将分区字段放在 SELECT 而非 WHERE 中
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - 表缺少分区过滤条件，可能导致全表扫描
--   - 多表分区条件不一致，可能混入非目标日期数据
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 3a: 主表有分区过滤，但 JOIN 的子表漏加分区条件
-- app 表过滤了 20260308，但 job 表未加分区条件，扫描 job 全表
-- ❌ 错误：job 表缺少 dt 过滤
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    j.job_id,
    j.status                                                AS job_status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    -- ❌ 缺少 AND a.dt = j.dt
WHERE a.dt = '20260308'                        -- ✅ app 表有分区过滤
  -- ❌ 缺少 AND j.dt = '20260308'             -- job 表全表扫描
  AND j.status = 'FAILED'
ORDER BY job_duration DESC
LIMIT 100;

-- ✅ 正确写法：所有表都加分区过滤
-- WHERE a.dt = '20260308'
--   AND j.dt = '20260308'


-- ---------------------------------------------------------------------------
-- Case 3b: CTE 中的表忘记加分区过滤
-- 主查询在最终 SELECT 中过滤了分区，但 CTE 定义时未加分区条件
-- ❌ 错误：CTE 中 stage 和 task 表未加分区过滤，会扫描全表
-- ---------------------------------------------------------------------------
WITH stage_stats AS (
    SELECT
        app_id,
        stage_id,
        status,
        num_tasks,
        (end_time - start_time)                             AS stage_duration
    FROM spark_analytics.spark_stage_metrics
    -- ❌ 缺少 WHERE dt = '20260308'，扫描全表
    WHERE status = 'COMPLETE'
),
task_stats AS (
    SELECT
        app_id,
        stage_id,
        COUNT(*)                                            AS task_count,
        AVG(task_run_time)                                  AS avg_runtime,
        SUM(gc_time)                                        AS total_gc
    FROM spark_analytics.spark_task_metrics
    -- ❌ 缺少 WHERE dt = '20260308'，扫描全表
    GROUP BY app_id, stage_id
)
SELECT
    ss.app_id,
    ss.stage_id,
    ss.stage_duration,
    ts.task_count,
    ts.avg_runtime,
    ts.total_gc
FROM stage_stats ss
INNER JOIN task_stats ts
    ON ss.app_id = ts.app_id
    AND ss.stage_id = ts.stage_id
ORDER BY ss.stage_duration DESC
LIMIT 200;

-- ✅ 正确写法：CTE 中也必须加分区过滤
-- FROM spark_analytics.spark_stage_metrics
-- WHERE dt = '20260308' AND status = 'COMPLETE'


-- ---------------------------------------------------------------------------
-- Case 3c: UNION ALL 各分支分区条件不一致
-- 两个分支分别查 app 和 job，但 job 分支的分区日期写错了
-- ❌ 错误：第二个分支分区日期多写了一天，混入了非目标日期数据
-- ---------------------------------------------------------------------------
SELECT
    'APP' AS source_type,
    a.app_id                                                AS entity_id,
    a.`user`,
    a.platform,
    a.result                                                AS status_code,
    (a.end_time - a.start_time)                             AS duration_ms,
    a.dt
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'                        -- ✅ 正确日期
  AND a.result != 0

UNION ALL

SELECT
    'JOB' AS source_type,
    j.job_id                                                AS entity_id,
    ''                                                      AS `user`,
    ''                                                      AS platform,
    CAST(0 AS BIGINT)                                       AS status_code,
    (j.end_time - j.start_time)                             AS duration_ms,
    j.dt
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260309'                        -- ❌ 日期写成了 0309，混入了次日数据
  AND j.status = 'FAILED';

-- ✅ 正确写法：统一分区日期
-- WHERE j.dt = '20260308'


-- ---------------------------------------------------------------------------
-- Case 3d: 关联子查询中漏加分区条件，子查询全表扫描
-- 外层查询有分区过滤，但 EXISTS 子查询中没有加分区过滤
-- ❌ 错误：子查询会扫描 task 表全量数据
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    a.executor_num,
    a.executor_memory,
    (a.end_time - a.start_time)                             AS app_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0
  AND EXISTS (
        SELECT 1
        FROM spark_analytics.spark_task_metrics t
        WHERE t.app_id = a.app_id
          -- ❌ 缺少 AND t.dt = '20260308'
          AND t.gc_time > 60000                             -- gc 时间超过 60s
      )
ORDER BY app_duration DESC
LIMIT 100;

-- ✅ 正确写法：子查询中也加分区过滤
-- WHERE t.app_id = a.app_id
--   AND t.dt = '20260308'
--   AND t.gc_time > 60000


-- ---------------------------------------------------------------------------
-- Case 3e: 多表 JOIN 分区条件分散在不同位置，某表遗漏
-- 四表 JOIN 场景，app/job/stage 都有分区条件，但 task 遗漏
-- ❌ 错误：task 表缺少分区过滤
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.status                                                AS job_status,
    s.stage_id,
    s.num_tasks,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    -- ❌ 缺少 AND s.dt = t.dt
WHERE a.dt = '20260308'
  AND j.status = 'FAILED'
  -- ❌ task 表完全没有分区过滤（ON 和 WHERE 中都没有）
ORDER BY t.task_run_time DESC
LIMIT 500;

-- ✅ 正确写法：
-- ON s.app_id = t.app_id AND s.stage_id = t.stage_id AND s.dt = t.dt
-- 或在 WHERE 中加：AND t.dt = '20260308'
