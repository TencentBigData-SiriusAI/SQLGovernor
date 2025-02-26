-- ============================================================================
-- Case 06: 聚合函数参数隐式转换
-- ============================================================================
-- 【问题描述】
--   聚合函数（SUM/AVG/MAX/MIN）对参数类型有要求：SUM/AVG 需要数值类型，
--   MAX/MIN 依赖可比较类型。当传入 STRING 类型字段时，引擎会尝试隐式
--   转为 DOUBLE/BIGINT，可能导致：
--     1. 非数值字符串转换失败产生 NULL，聚合结果偏低
--     2. SUM(STRING) 丢失无法转换的行，统计结果不完整
--     3. AVG 分母不正确（NULL 行不参与 AVG 分母计算）
--     4. MAX/MIN 在数值语义和字典序之间切换
--
-- 【易犯场景】
--   1. task_event_num 实际为 STRING 类型，但名字暗示是数值，直接 SUM/AVG
--   2. stage_id/job_id 虽为 STRING 但内容是数字，被误用于数值聚合
--   3. status 字段混合了数字编码和文字描述，聚合时部分转换失败
--   4. 从 Hive 迁移到 Spark 时聚合行为差异导致结果不同
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - 聚合函数参数类型为 STRING，存在隐式类型转换，可能导致结果不准确
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 6a: SUM/AVG 传入 STRING 类型字段
-- task_event_num 在 app 表中是 STRING 类型（虽然名字暗示是数值）
-- 直接做 SUM/AVG 会触发 STRING -> DOUBLE 隐式转换
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.platform,
    COUNT(*)                                      AS app_count,
    -- ❌ task_event_num 是 STRING，SUM 触发隐式转换 STRING -> DOUBLE
    SUM(a.task_event_num)                         AS total_task_events,
    -- ❌ 同上，AVG 也触发隐式转换
    AVG(a.task_event_num)                         AS avg_task_events,
    -- ❌ MAX/MIN 对 STRING 按字典序，转 DOUBLE 后按数值序，结果不同
    MAX(a.task_event_num)                         AS max_task_events,
    MIN(a.task_event_num)                         AS min_task_events,
    -- 以下为正确的 BIGINT 聚合，不会触发转换
    SUM(a.job_event_num)                          AS total_jobs,
    AVG(a.executor_num)                           AS avg_executors,
    AVG(a.executor_memory)                        AS avg_exec_mem,
    -- 成功率
    ROUND(
        SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 2
    )                                              AS success_rate_pct
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.`user`, a.platform
HAVING COUNT(*) > 5
ORDER BY total_task_events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 6b: 对 STRING 类型的 ID 字段做数值聚合
-- stage_id 和 job_id 虽然内容可能是数字，但类型为 STRING
-- 对其做 SUM/AVG 毫无业务意义，且触发隐式转换
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    COUNT(*)                                      AS stage_count,
    -- ❌ stage_id 是 STRING，SUM 无业务意义且触发隐式转换
    SUM(s.stage_id)                               AS sum_stage_id,
    AVG(s.stage_id)                               AS avg_stage_id,
    -- ❌ stage_attempt_id 是 STRING，MAX 在字典序下 '9' > '10'
    MAX(s.stage_attempt_id)                       AS max_attempt,
    -- 正确用法：对数值字段聚合
    SUM(s.num_tasks)                              AS total_tasks,
    AVG(s.end_time - s.start_time)                AS avg_stage_duration,
    MAX(s.end_time - s.start_time)                AS max_stage_duration,
    -- ❌ 嵌套隐式转换：SUM(STRING) 后除以 COUNT 得到 DOUBLE
    ROUND(SUM(s.stage_id) / COUNT(*), 2)          AS meaningless_metric
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
GROUP BY s.app_id
ORDER BY stage_count DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 6c: 多表 JOIN 后聚合中的隐式转换
-- 先 JOIN task 和 stage，再做聚合，聚合中混用 STRING 和 BIGINT 字段
-- 更贴近实际数仓开发的复杂查询场景
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.num_tasks                                   AS expected_tasks,
    COUNT(t.task_id)                              AS actual_tasks,
    -- ❌ task_id 是 STRING，SUM 触发隐式转换
    SUM(t.task_id)                                AS sum_task_id,
    -- 正常数值聚合
    AVG(t.task_run_time)                          AS avg_run_time,
    MAX(t.gc_time)                                AS max_gc_time,
    SUM(t.shuffle_read_bytes)                     AS total_shuffle_read,
    SUM(t.shuffle_write_bytes)                    AS total_shuffle_write,
    -- ❌ 条件聚合中也存在隐式转换
    SUM(CASE
        WHEN t.status = 'SUCCESS' THEN t.task_run_time
        ELSE t.status                             -- ❌ STRING 与 BIGINT 混在同一个 SUM 中
    END)                                           AS weighted_runtime,
    -- GC 严重的 task 占比
    ROUND(
        SUM(CASE WHEN t.gc_time > t.task_run_time * 0.3 THEN 1 ELSE 0 END)
        * 100.0 / GREATEST(COUNT(*), 1), 2
    )                                              AS high_gc_pct
FROM spark_analytics.spark_stage_metrics s
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE s.dt = '20260308'
  AND s.num_tasks > 20
GROUP BY s.app_id, s.stage_id, s.num_tasks
HAVING COUNT(t.task_id) > 10
ORDER BY avg_run_time DESC
LIMIT 200;
