-- ============================================================================
-- Case 04: UNION ALL 各分支列类型不一致导致隐式转换
-- ============================================================================
-- 【问题描述】
--   UNION ALL 要求各分支的列数相同，但对类型的处理是通过隐式转换来统一的。
--   当各分支对应位置的列类型不同时（如 STRING vs BIGINT），引擎会选择一个
--   "更宽"的类型做转换。这会导致：
--     1. 数值精度丢失（BIGINT -> DOUBLE -> STRING）
--     2. 字符串语义变化（'007' -> 7 -> '7'）
--     3. 下游消费 UNION 结果时类型不符合预期
--     4. 影响后续聚合/排序的正确性
--
-- 【易犯场景】
--   1. 多张表结构类似但字段类型不完全统一，直接 UNION
--   2. 用 SELECT 常量行做 dummy 数据插入时类型不匹配
--   3. 不同子查询返回的聚合值类型不同（COUNT 是 BIGINT，AVG 是 DOUBLE）
--   4. 字段别名相同但实际类型不同的视图做 UNION
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - UNION ALL 各分支对应列类型不一致，存在隐式类型转换
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 4a: app 表与 job 表 UNION，对应列类型不统一
-- app.result (BIGINT) 与 job.status (STRING) 放在同一列位置
-- app.executor_num (BIGINT) 与 job.job_id (STRING) 放在同一列位置
-- ---------------------------------------------------------------------------
SELECT
    source_table,
    record_id,
    app_id,
    status_or_result,
    time_value,
    partition_date
FROM (
    -- 分支1：来自 app 表
    SELECT
        'app'                                     AS source_table,
        app_id                                    AS record_id,     -- STRING
        app_id,                                                     -- STRING
        result                                    AS status_or_result,  -- ❌ BIGINT
        start_time                                AS time_value,    -- BIGINT
        dt                           AS partition_date -- STRING
    FROM spark_analytics.spark_app_metrics
    WHERE dt = '20260308'
      AND result != 0

    UNION ALL

    -- 分支2：来自 job 表
    SELECT
        'job'                                     AS source_table,
        job_id                                    AS record_id,     -- STRING
        app_id,                                                     -- STRING
        status                                    AS status_or_result,  -- ❌ STRING，与上面 BIGINT 不匹配
        submit_time                               AS time_value,    -- BIGINT
        dt                           AS partition_date -- STRING
    FROM spark_analytics.spark_job_metrics
    WHERE dt = '20260308'
      AND status = 'FAILED'

    UNION ALL

    -- 分支3：来自 stage 表
    SELECT
        'stage'                                   AS source_table,
        stage_id                                  AS record_id,     -- STRING
        app_id,                                                     -- STRING
        num_tasks                                 AS status_or_result,  -- ❌ BIGINT，又与第二分支 STRING 不匹配
        start_time                                AS time_value,    -- BIGINT
        dt                           AS partition_date -- STRING
    FROM spark_analytics.spark_stage_metrics
    WHERE dt = '20260308'
      AND status = 'FAILED'
) all_records
ORDER BY time_value DESC
LIMIT 500;


-- ---------------------------------------------------------------------------
-- Case 4b: 聚合值类型不一致的 UNION
-- 不同聚合函数返回不同类型：COUNT->BIGINT, AVG->DOUBLE, MAX(STRING)->STRING
-- 放在同一列位置导致隐式转换链
-- ---------------------------------------------------------------------------
SELECT
    metric_name,
    metric_source,
    metric_value,
    metric_detail,
    calc_date
FROM (
    -- 分支1：app 级别统计 —— COUNT 返回 BIGINT
    SELECT
        'app_count'                               AS metric_name,
        'app'                                     AS metric_source,
        COUNT(*)                                  AS metric_value,    -- BIGINT
        MAX(app_name)                             AS metric_detail,   -- STRING
        dt                           AS calc_date
    FROM spark_analytics.spark_app_metrics
    WHERE dt = '20260308'
    GROUP BY dt

    UNION ALL

    -- 分支2：stage 级别统计 —— AVG 返回 DOUBLE
    SELECT
        'avg_tasks'                               AS metric_name,
        'stage'                                   AS metric_source,
        AVG(num_tasks)                            AS metric_value,    -- ❌ DOUBLE 与上面 BIGINT 不匹配
        MAX(status)                               AS metric_detail,   -- STRING
        dt                           AS calc_date
    FROM spark_analytics.spark_stage_metrics
    WHERE dt = '20260308'
    GROUP BY dt

    UNION ALL

    -- 分支3：task 级别统计 —— 字符串拼接返回 STRING
    SELECT
        'task_summary'                            AS metric_name,
        'task'                                    AS metric_source,
        -- ❌ CONCAT 返回 STRING，与前两个分支的 BIGINT/DOUBLE 完全不同类型
        CONCAT(COUNT(*), '_tasks')                AS metric_value,
        MAX(status)                               AS metric_detail,   -- STRING
        dt                           AS calc_date
    FROM spark_analytics.spark_task_metrics
    WHERE dt = '20260308'
    GROUP BY dt
) metrics
ORDER BY metric_name;
