-- ============================================================================
-- Case 04: 子查询（Derived Table）缺少别名
-- ============================================================================
-- 【问题描述】
--   在 FROM 子句中使用子查询（派生表）时，必须为其指定别名（alias）。
--   如果遗漏别名，SQL 解析器无法为子查询结果集命名，直接报语法错误。
--   这一规则在几乎所有 SQL 引擎中都是强制的（Hive/SparkSQL/Presto等）。
--
-- 【易犯场景】
--   1. 快速编写子查询时忘记在末尾添加别名
--   2. 复杂嵌套子查询中，某一层漏掉了别名
--   3. 从其他地方复制 SQL 片段拼接时，漏掉了别名部分
--   4. JOIN 中两个子查询，其中一个忘记起别名
--   5. 在子查询后紧跟 WHERE/ON 时，视觉上容易忽略别名
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - 子查询缺少别名（alias），需在子查询后添加别名
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 4a: FROM 后单个子查询缺少别名
-- 对 app 表做过滤后作为子查询，但忘了加别名
-- ❌ 错误：子查询 (...) 后必须跟别名
-- ---------------------------------------------------------------------------
SELECT
    app_id,
    app_name,
    `user`,
    duration_sec,
    executor_num,
    executor_memory
FROM (
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        a.executor_num,
        a.executor_memory,
        a.executor_cores,
        a.platform,
        a.result,
        ROUND((a.end_time - a.start_time) / 1000, 2) AS duration_sec
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
      AND a.result != 0
)                                     -- ❌ 子查询缺少别名，应为 ) sub 或 ) t
WHERE duration_sec > 300
ORDER BY duration_sec DESC
LIMIT 100;

-- ✅ 正确写法：
-- ) sub
-- WHERE sub.duration_sec > 300


-- ---------------------------------------------------------------------------
-- Case 4b: JOIN 中一侧子查询缺少别名
-- 左表是子查询有别名，右表子查询忘了加别名
-- ❌ 错误：右侧子查询缺少别名，JOIN 无法引用其列
-- ---------------------------------------------------------------------------
SELECT
    app_info.app_id,
    app_info.app_name,
    app_info.`user`,
    job_stats.job_count,
    job_stats.fail_count,
    job_stats.avg_job_duration
FROM (
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        a.platform,
        a.result,
        a.start_time,
        a.end_time
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
      AND a.result != 0
) app_info                            -- ✅ 左表子查询有别名
INNER JOIN (
    SELECT
        j.app_id,
        COUNT(*)                                    AS job_count,
        SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END)
                                                    AS fail_count,
        AVG(j.end_time - j.start_time)              AS avg_job_duration,
        MAX(j.end_time - j.submit_time)             AS max_total_time
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
    GROUP BY j.app_id
)                                     -- ❌ 右表子查询缺少别名
    ON app_info.app_id = job_stats.app_id            -- job_stats 未定义
ORDER BY job_stats.fail_count DESC
LIMIT 50;

-- ✅ 正确写法：
-- ) job_stats


-- ---------------------------------------------------------------------------
-- Case 4c: 多层嵌套子查询中间层缺少别名
-- 三层嵌套查询，中间层的子查询漏掉了别名
-- ❌ 错误：中间层子查询缺少别名
-- ---------------------------------------------------------------------------
SELECT
    final.app_id,
    final.total_tasks,
    final.avg_gc_ratio
FROM (
    SELECT
        mid.app_id,
        mid.total_tasks,
        mid.avg_gc_ratio
    FROM (
        SELECT
            t.app_id,
            COUNT(*)                                AS total_tasks,
            AVG(
                CASE
                    WHEN t.task_run_time > 0
                    THEN ROUND(t.gc_time * 100.0 / t.task_run_time, 2)
                    ELSE 0
                END
            )                                       AS avg_gc_ratio,
            SUM(t.task_run_time)                    AS total_run_time,
            SUM(t.gc_time)                          AS total_gc_time,
            MAX(t.executor_cpu_time)                AS max_cpu_time
        FROM spark_analytics.spark_task_metrics t
        WHERE t.dt = '20260308'
          AND t.status = 'SUCCESS'
        GROUP BY t.app_id
        HAVING COUNT(*) > 50
    )                                 -- ❌ 中间层子查询缺少别名，应为 ) mid
    WHERE avg_gc_ratio > 20
) final                               -- ✅ 最外层有别名
ORDER BY final.avg_gc_ratio DESC
LIMIT 100;

-- ✅ 正确写法：
-- ) mid


-- ---------------------------------------------------------------------------
-- Case 4d: LATERAL VIEW 搭配子查询时缺少别名
-- 对 stage_ids 做 explode 的子查询场景，子查询缺少别名
-- ❌ 错误：FROM 后子查询缺少别名
-- ---------------------------------------------------------------------------
SELECT
    job_detail.app_id,
    job_detail.job_id,
    single_stage_id,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    (s.end_time - s.start_time)       AS stage_duration_ms
FROM (
    SELECT
        j.app_id,
        j.job_id,
        j.stage_ids,
        j.status                      AS job_status,
        j.submit_time,
        j.start_time,
        j.end_time,
        j.failed_reason
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
      AND j.status IN ('SUCCEEDED', 'FAILED')
)                                     -- ❌ 缺少别名
LATERAL VIEW EXPLODE(SPLIT(job_detail.stage_ids, ',')) stage_tbl
    AS single_stage_id
LEFT JOIN spark_analytics.spark_stage_metrics s
    ON job_detail.app_id = s.app_id
    AND single_stage_id = s.stage_id
    AND s.dt = '20260308'
ORDER BY stage_duration_ms DESC
LIMIT 200;

-- ✅ 正确写法：
-- ) job_detail
