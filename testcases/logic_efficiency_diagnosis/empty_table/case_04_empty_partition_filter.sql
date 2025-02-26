-- ============================================================================
-- Case 04: 过滤条件指向空分区或不存在的分区
-- ============================================================================
-- 【问题描述】
--   分区表的分区字段作为数据组织的关键维度，当查询条件指向一个不存在
--   或尚无数据的分区时，查询结果为空。常见原因：
--     1. 分区日期格式错误（'2027-03-08' vs '20270308'）
--     2. 查询未来日期（数据尚未写入）
--     3. 数据源切换后分区规则改变（日分区变小时分区等）
--     4. 动态分区写入失败但查询侧不知情
--     5. 上游数据重跑导致某些分区被清空后未重新写入
--
-- 【易犯场景】
--   1. 定时调度任务的日期变量配置错误
--   2. 手动补数据时日期范围写错
--   3. 跨时区调度导致分区日期偏移一天
--   4. 节假日数据源停更但 ETL 照常执行
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - 查询分区可能不存在或无数据
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 4a: 分区格式错误 —— 带横线 vs 不带横线
-- 表的实际分区值为 '20270308'（yyyyMMdd），但查询用 '2027-03-08'
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.action,
    j.status,
    j.submit_time,
    j.start_time,
    j.end_time,
    j.failed_reason,
    j.stage_ids,
    (j.start_time - j.submit_time)               AS queue_ms,
    (j.end_time - j.start_time)                   AS exec_ms,
    (j.end_time - j.submit_time)                   AS total_ms
FROM spark_analytics.spark_job_metrics j
-- ❌ 分区格式错误：实际为 '20270308'，写成 '2027-03-08'
WHERE j.dt = '2027-03-08'
  AND j.status = 'FAILED'
ORDER BY j.submit_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 4b: 查询未来的不存在分区
-- 调度任务配置了 T+1 的查询，但数据是 T+1 才写入
-- 执行时分区尚不存在
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.stage_attempt_id,
    s.num_tasks,
    s.status,
    s.submit_time,
    s.start_time,
    s.end_time,
    (s.end_time - s.start_time)                   AS stage_duration_ms,
    (s.start_time - s.submit_time)                AS stage_queue_ms,
    CASE
        WHEN s.num_tasks > 0
        THEN ROUND((s.end_time - s.start_time) * 1.0 / s.num_tasks, 2)
        ELSE 0
    END                                            AS avg_task_ms
FROM spark_analytics.spark_stage_metrics s
-- ❌ 查询未来分区，数据不存在
WHERE s.dt = '20270315'
  AND s.num_tasks > 50
ORDER BY s.num_tasks DESC;


-- ---------------------------------------------------------------------------
-- Case 4c: 日期范围覆盖了空分区
-- 查询最近7天数据，但其中部分日期（如节假日或未来日期）无数据
-- 结果只包含有数据的日期，可能导致统计偏差
-- ---------------------------------------------------------------------------
SELECT
    t.dt,
    COUNT(*)                                      AS task_count,
    COUNT(DISTINCT t.app_id)                      AS app_count,
    COUNT(DISTINCT t.stage_id)                    AS stage_count,
    AVG(t.task_run_time)                          AS avg_runtime,
    MAX(t.task_run_time)                          AS max_runtime,
    SUM(t.shuffle_read_bytes)                     AS total_shuffle_read,
    SUM(t.shuffle_write_bytes)                    AS total_shuffle_write,
    SUM(t.gc_time)                                AS total_gc_time,
    ROUND(SUM(t.gc_time) * 100.0
        / GREATEST(SUM(t.task_run_time), 1), 2)   AS gc_ratio_pct
FROM spark_analytics.spark_task_metrics t
-- ❌ 日期范围包含了可能不存在的分区（如未来日期或节假日）
-- 有数据的日期正常返回，空分区日期直接被跳过不在结果中
-- 下游按天取平均时分母不对
WHERE t.dt BETWEEN '20270305' AND '20270312'
GROUP BY t.dt
ORDER BY t.dt;


-- ---------------------------------------------------------------------------
-- Case 4d: 多表查询中分区条件不一致导致某些表命中空分区
-- app 表查 20270308，但 job 表用了错误分区，导致 JOIN 为空
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    j.job_id,
    j.status,
    j.failed_reason
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
-- ❌ 两张表分区条件不一致：app 查正确日期，job 查错误格式
-- job 表命中空分区，LEFT JOIN 后所有 job 字段为 NULL
WHERE a.dt = '20270308'
  AND j.dt = '2027-03-08'
  AND a.`result` != 0
ORDER BY a.start_time DESC
LIMIT 50;
