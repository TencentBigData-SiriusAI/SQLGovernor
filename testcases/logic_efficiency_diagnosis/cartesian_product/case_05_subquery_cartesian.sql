-- ============================================================================
-- Case 05: 子查询结果集之间缺少关联导致笛卡尔积
-- ============================================================================
-- 【问题描述】
--   当 FROM 中使用多个子查询时，如果子查询结果集之间没有正确关联，
--   同样会产生笛卡尔积。由于子查询已经聚合或过滤过数据，开发者
--   可能误以为数据量很小不会有问题，但实际上：
--     1. 聚合后的行数可能仍然很大
--     2. 子查询的列名可能与预期不同，关联条件失效
--     3. 子查询间的关联容易在复杂 SQL 中被遗漏
--
-- 【易犯场景】
--   1. 分别写了两个统计子查询，想合到一起但忘了关联
--   2. 子查询各自统计不同维度，关联维度不一致
--   3. 子查询定义了别名但外层引用时拼写错误
--   4. 子查询的 GROUP BY 维度不同，关联键不对齐
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - 子查询间缺少关联条件
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 5a: 两个聚合子查询无关联 —— app 统计 × job 统计
-- 分别统计每个用户的 app 和 job 指标，但两个子查询没有关联
-- ❌ 错误：两个子查询之间缺少 ON 关联
-- ---------------------------------------------------------------------------
SELECT
    app_summary.`user`,
    app_summary.app_count,
    app_summary.success_rate,
    app_summary.avg_duration,
    job_summary.total_jobs,
    job_summary.fail_count,
    job_summary.avg_job_duration
FROM (
    SELECT
        a.`user`,
        COUNT(*)                                    AS app_count,
        ROUND(
            SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END) * 100.0
            / COUNT(*), 2
        )                                           AS success_rate,
        AVG(a.end_time - a.start_time)              AS avg_duration
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
    GROUP BY a.`user`
) app_summary
INNER JOIN (
    SELECT
        j.app_id,
        COUNT(*)                                    AS total_jobs,
        SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END)
                                                    AS fail_count,
        AVG(j.end_time - j.start_time)              AS avg_job_duration
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
    GROUP BY j.app_id
) job_summary
    ON 1 = 1                          -- ❌ 恒真关联，app 按 user 聚合，job 按 app_id 聚合
    -- 两个子查询的维度不同（user vs app_id），无法正确关联
ORDER BY app_summary.app_count DESC
LIMIT 100;

-- ✅ 正确写法：统一维度或通过 app 表桥接
-- 方案1：两个子查询都按 user 聚合
-- 方案2：先关联 app 表获取 user，再做聚合


-- ---------------------------------------------------------------------------
-- Case 5b: 标量子查询与主查询笛卡尔积
-- 标量子查询返回多行时，与主查询产生笛卡尔积
-- ❌ 错误：子查询返回多行，不能作为标量值使用
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.executor_num,
    (a.end_time - a.start_time)       AS app_duration,
    (
        SELECT AVG(j.end_time - j.start_time)
        FROM spark_analytics.spark_job_metrics j
        WHERE j.dt = '20260308'
        -- ❌ 缺少 j.app_id = a.app_id 的关联
        -- 返回全局平均值而非当前 app 的平均值
    )                                  AS avg_job_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY app_duration DESC
LIMIT 100;

-- ✅ 正确写法：
-- WHERE j.app_id = a.app_id
--   AND j.dt = a.dt


-- ---------------------------------------------------------------------------
-- Case 5c: 子查询与普通表之间缺少关联
-- 子查询统计 stage 信息，与 task 表 JOIN 时缺少关联条件
-- ❌ 错误：子查询结果与 task 表之间没有正确关联
-- ---------------------------------------------------------------------------
SELECT
    stage_info.app_id,
    stage_info.stage_id,
    stage_info.stage_duration,
    stage_info.num_tasks              AS expected_tasks,
    t.task_id,
    t.status                          AS task_status,
    t.task_run_time,
    t.gc_time
FROM (
    SELECT
        s.app_id,
        s.stage_id,
        s.num_tasks,
        s.status,
        (s.end_time - s.start_time)   AS stage_duration,
        s.dt
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.dt = '20260308'
      AND s.num_tasks > 100
) stage_info
INNER JOIN spark_analytics.spark_task_metrics t
    ON stage_info.dt = t.dt  -- ❌ 只用日期关联
    -- 缺少 AND stage_info.app_id = t.app_id
    -- 缺少 AND stage_info.stage_id = t.stage_id
WHERE t.status = 'SUCCESS'
ORDER BY t.task_run_time DESC
LIMIT 500;

-- ✅ 正确写法：
-- ON stage_info.app_id = t.app_id
--    AND stage_info.stage_id = t.stage_id
--    AND stage_info.dt = t.dt
