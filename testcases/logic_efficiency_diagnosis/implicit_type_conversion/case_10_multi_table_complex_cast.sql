-- ============================================================================
-- Case 10: 复杂多表场景中的综合隐式转换
-- ============================================================================
-- 【问题描述】
--   在真实数仓开发中，往往一条 SQL 涉及多张表 JOIN、嵌套子查询、CTE、
--   窗口函数和复杂聚合，其中可能同时存在多种隐式转换。这种综合场景下
--   的隐式转换更加隐蔽、更难排查，且各种转换可能相互叠加放大：
--     1. JOIN key 类型不匹配 + 分区列整数比较 同时出现
--     2. CTE 中间结果类型变化传递到后续引用
--     3. 窗口函数 PARTITION BY / ORDER BY 中的类型不一致
--     4. 多层子查询中逐层类型退化
--
-- 【易犯场景】
--   1. 从多个来源拼接的复杂 ETL SQL，各段代码由不同人编写
--   2. 长期迭代的报表 SQL，不同时期添加的逻辑类型风格不一致
--   3. 从生产环境 copy-paste 拼接的 ad-hoc 查询
--   4. 自动生成的 SQL（如 BI 工具/代码生成器）类型处理粗糙
--
-- 【预期诊断结果】
--   应触发多条"隐式转换"告警，覆盖以下类型：
--   - 分区列类型不匹配
--   - JOIN key 类型不一致
--   - CASE WHEN 分支类型混合
--   - 聚合函数参数类型错误
--   - 算术运算中 STRING 参与
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 10a: CTE + 多表 JOIN + 窗口函数中的综合隐式转换
-- 模拟一个典型的 Spark 任务性能分析报表 SQL
-- 每个环节都埋入了不同类型的隐式转换
-- ---------------------------------------------------------------------------
WITH app_base AS (
    -- CTE 1: 获取 app 基础信息
    SELECT
        app_id,
        app_name,
        `user`,
        result,
        platform,
        executor_num,
        executor_memory,
        start_time,
        end_time,
        task_event_num,                           -- STRING 类型，后续会参与数值运算
        dt,
        -- ❌ CASE 分支类型不一致
        CASE
            WHEN result = 0 THEN '成功'
            WHEN result = 137 THEN result         -- ❌ BIGINT 与 STRING 混合
            ELSE CONCAT('错误码:', result)         -- result BIGINT 隐式转 STRING
        END AS result_label,
        -- ❌ ROW_NUMBER 的 PARTITION BY 中类型不一致
        ROW_NUMBER() OVER (
            PARTITION BY `user`, platform
            ORDER BY end_time - start_time DESC
        ) AS rn
    FROM spark_analytics.spark_app_metrics
    WHERE dt = 20260308              -- ❌ 分区列与整数比较
),

job_stats AS (
    -- CTE 2: 聚合 job 级别统计
    SELECT
        app_id,
        COUNT(*)                                  AS job_count,
        SUM(CASE
            WHEN status = 'SUCCEEDED' THEN 1
            ELSE status                           -- ❌ STRING 参与 SUM，隐式转换
        END)                                       AS success_indicator,
        MAX(end_time - start_time)                AS max_job_duration,
        -- ❌ job_id (STRING) 做数值聚合
        AVG(job_id)                               AS avg_job_id,
        dt
    FROM spark_analytics.spark_job_metrics
    WHERE dt = '20260308'
    GROUP BY app_id, dt
),

stage_task_agg AS (
    -- CTE 3: stage + task 关联聚合
    SELECT
        s.app_id,
        s.stage_id,
        s.num_tasks,
        COUNT(t.task_id)                          AS actual_task_count,
        AVG(t.task_run_time)                      AS avg_task_runtime,
        MAX(t.gc_time)                            AS max_gc_time,
        SUM(t.shuffle_read_bytes)                 AS total_shuffle_read,
        SUM(t.shuffle_write_bytes)                AS total_shuffle_write,
        -- ❌ task_id (STRING) 做 SUM
        SUM(t.task_id)                            AS sum_task_id,
        s.dt
    FROM spark_analytics.spark_stage_metrics s
    INNER JOIN spark_analytics.spark_task_metrics t
        ON CAST(s.stage_id AS INT) = t.stage_id   -- ❌ CAST 后 INT vs STRING
        AND s.app_id = t.app_id
        AND s.dt = t.dt
    WHERE s.dt = '20260308'
      AND t.dt = '20260308'
      AND s.num_tasks > 10
    GROUP BY s.app_id, s.stage_id, s.num_tasks, s.dt
)

-- 主查询：关联三个 CTE
SELECT
    ab.app_id,
    ab.app_name,
    ab.`user`,
    ab.result_label,
    ab.platform,
    ab.executor_num,
    ab.executor_memory,
    -- ❌ task_event_num (STRING) 参与算术运算
    ab.task_event_num * 1.0 / GREATEST(js.job_count, 1)
                                                  AS tasks_per_job,
    js.job_count,
    js.success_indicator,
    js.max_job_duration,
    sta.stage_id,
    sta.num_tasks,
    sta.actual_task_count,
    sta.avg_task_runtime,
    sta.max_gc_time,
    sta.total_shuffle_read,
    sta.total_shuffle_write,
    -- ❌ CASE 中混合 BIGINT 和 STRING
    CASE
        WHEN sta.avg_task_runtime > 60000 THEN CONCAT('慢任务_', sta.stage_id)
        WHEN sta.avg_task_runtime > 10000 THEN sta.avg_task_runtime
        ELSE 0
    END                                            AS performance_tag,
    -- ❌ ROUND 中 STRING 参与运算
    ROUND(
        ab.task_event_num / GREATEST(sta.actual_task_count, 1) * 100.0,
        2
    )                                              AS task_coverage_pct,
    ab.rn
FROM app_base ab
-- ❌ JOIN key: ab.dt (STRING) = js.dt (STRING) 但
--    ab 来自整数过滤的 CTE，类型可能已被引擎改变
INNER JOIN job_stats js
    ON ab.app_id = js.app_id
    AND ab.dt = js.dt
LEFT JOIN stage_task_agg sta
    ON ab.app_id = sta.app_id
    AND ab.dt = sta.dt
WHERE ab.rn <= 3                                  -- 每用户每平台 Top3
  -- ❌ 用 STRING 与 BIGINT 比较
  AND js.job_count > '2'
  AND sta.actual_task_count IS NOT NULL
ORDER BY ab.`user`, ab.platform, ab.rn
LIMIT 500;


-- ---------------------------------------------------------------------------
-- Case 10b: INSERT INTO ... SELECT 中的隐式转换
-- 模拟 ETL 写入目标表时，源表与目标表字段类型不匹配
-- 虽然不真正执行 INSERT，但 SELECT 部分的类型问题同样存在
-- ---------------------------------------------------------------------------
SELECT
    -- 假设目标表要求 INT 类型，但源表为 STRING
    CAST(t.app_id AS INT)                     AS app_id_int,        -- 可能转换失败
    t.task_id,                                 -- STRING
    t.stage_id,                                -- STRING
    -- ❌ 目标表是 DOUBLE，源表 task_run_time 是 BIGINT，直接赋值隐式转换
    t.task_run_time,
    t.gc_time,
    -- ❌ 拼接 stage_id (STRING) 与 task_run_time (BIGINT) 作为复合 key
    CONCAT(t.stage_id, '_', t.task_run_time)  AS composite_key,
    -- ❌ 条件聚合中 STRING status 参与数值运算
    CASE t.status
        WHEN 'SUCCESS' THEN 1
        WHEN 'FAILED'  THEN 2
        ELSE t.status                          -- ❌ STRING 与 INT 混合
    END                                        AS status_code,
    -- ❌ 窗口函数中 ORDER BY 混合类型
    SUM(t.task_run_time) OVER (
        PARTITION BY t.app_id
        ORDER BY t.stage_id                    -- STRING 排序：'9' > '10'
    )                                          AS cumulative_runtime,
    -- ❌ LEAD/LAG 中默认值类型不匹配
    LAG(t.task_run_time, 1, '0') OVER (
        PARTITION BY t.app_id, t.stage_id
        ORDER BY t.task_id                     -- ❌ STRING 排序
    )                                          AS prev_runtime,     -- default '0' STRING vs BIGINT
    t.dt
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  -- ❌ STRING status 与数值比较
  AND t.status != 0
  AND t.task_run_time > 1000
ORDER BY t.app_id, CAST(t.stage_id AS INT), t.task_run_time DESC
LIMIT 1000;
