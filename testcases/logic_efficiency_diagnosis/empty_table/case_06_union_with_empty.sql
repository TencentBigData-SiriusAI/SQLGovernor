-- ============================================================================
-- Case 06: UNION ALL 中包含空表分支
-- ============================================================================
-- 【问题描述】
--   多分支 UNION ALL 时，如果某些分支查询的表/分区为空：
--     1. 空分支不贡献任何行，但 SQL 仍成功执行
--     2. 最终结果只包含非空分支的数据，可能造成数据不完整
--     3. 下游基于 UNION 结果做统计时，缺少某些来源的数据
--     4. 如果所有分支都为空，整个 UNION 结果为空
--
-- 【易犯场景】
--   1. 合并多天数据时，某些天的分区为空（节假日、数据延迟）
--   2. 合并多来源数据时，某个来源系统故障导致数据为空
--   3. 按业务类型分支查询，某个类型无数据但不影响整体执行
--   4. 增量合并场景中，增量表为空但全量部分正常
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - UNION ALL 中某些分支可能返回空集，结果数据可能不完整
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 6a: 合并多天数据，某些天分区为空
-- 合并最近5天的 app 数据做趋势分析
-- 其中某些天可能无数据（周末、节假日、采集故障）
-- ---------------------------------------------------------------------------
SELECT
    dt,
    COUNT(*)                                      AS app_count,
    AVG(executor_num)                             AS avg_executors,
    AVG(executor_memory)                          AS avg_memory,
    SUM(CASE WHEN `result` = 0 THEN 1 ELSE 0 END)  AS success_count,
    ROUND(SUM(CASE WHEN `result` = 0 THEN 1 ELSE 0 END) * 100.0
        / GREATEST(COUNT(*), 1), 2)               AS success_rate
FROM (
    -- Day 1 ✅ 有数据
    SELECT * FROM spark_analytics.spark_app_metrics
    WHERE dt = '20270306'

    UNION ALL

    -- Day 2 ✅ 有数据
    SELECT * FROM spark_analytics.spark_app_metrics
    WHERE dt = '20270307'

    UNION ALL

    -- Day 3 ✅ 有数据
    SELECT * FROM spark_analytics.spark_app_metrics
    WHERE dt = '20270308'

    UNION ALL

    -- ❌ Day 4 未来日期，分区不存在，空分支
    SELECT * FROM spark_analytics.spark_app_metrics
    WHERE dt = '20270309'

    UNION ALL

    -- ❌ Day 5 未来日期，分区不存在，空分支
    SELECT * FROM spark_analytics.spark_app_metrics
    WHERE dt = '20270310'
) all_days
GROUP BY dt
-- ❌ 空分区日期不会出现在结果中，趋势分析缺少断点，图表不连续
ORDER BY dt;


-- ---------------------------------------------------------------------------
-- Case 6b: 合并多来源表，某张表为空
-- 合并 app、job、stage 的异常记录做统一告警
-- 如果某张表当天无异常数据，该分支为空
-- ---------------------------------------------------------------------------
SELECT
    source_type,
    record_id,
    app_id,
    error_info,
    event_time,
    dt
FROM (
    -- 分支1: app 异常
    SELECT
        'APP'                                     AS source_type,
        app_id                                    AS record_id,
        app_id,
        CAST(`result` AS STRING)                    AS error_info,
        start_time                                AS event_time,
        dt
    FROM spark_analytics.spark_app_metrics
    WHERE dt = '20270308'
      AND `result` != 0

    UNION ALL

    -- 分支2: job 异常
    SELECT
        'JOB'                                     AS source_type,
        job_id                                    AS record_id,
        app_id,
        failed_reason                             AS error_info,
        submit_time                               AS event_time,
        dt
    FROM spark_analytics.spark_job_metrics
    WHERE dt = '20270308'
      AND status = 'FAILED'

    UNION ALL

    -- ❌ 分支3: stage 异常 —— 如果当天无 FAILED stage，此分支为空
    -- 下游统计来源分布时会缺少 STAGE 类型
    SELECT
        'STAGE'                                   AS source_type,
        stage_id                                  AS record_id,
        app_id,
        status                                    AS error_info,
        submit_time                               AS event_time,
        dt
    FROM spark_analytics.spark_stage_metrics
    WHERE dt = '20270308'
      AND status = 'FAILED'
      AND num_tasks > 1000                        -- ❌ 条件过严，可能无数据满足
) all_errors
ORDER BY event_time DESC
LIMIT 500;
