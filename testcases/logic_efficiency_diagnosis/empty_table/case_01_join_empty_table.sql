-- ============================================================================
-- Case 01: JOIN 空表/空分区导致结果集为空
-- ============================================================================
-- 【问题描述】
--   INNER JOIN 时，如果其中一张表（或某个分区）没有数据，整个 JOIN 结果
--   将为空。这在数仓中非常常见但极易被忽略，因为：
--     1. SQL 不会报错，执行成功但结果为0行
--     2. 下游任务继续执行，产出空表或错误指标
--     3. 上游数据延迟/缺失时，当天分区可能为空
--     4. 新表上线初期某些分区尚未有数据写入
--
-- 【易犯场景】
--   1. 上游采集链路故障，当天分区为空但 ETL 照常调度
--   2. 跨天查询时，未来日期分区自然为空
--   3. 多表 JOIN 中某张冷门表长期无新增数据
--   4. 测试环境表数据与生产环境不同步，某些表为空
--   5. 分区字段格式不一致（如 '2027-03-08' vs '20270308'），
--      看似有数据实则匹配到空分区
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - JOIN 的表/分区可能为空，建议添加空表检查或数据就绪依赖
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 1a: INNER JOIN 未来日期分区 —— 分区必然为空
-- 研发人员写了 T+1 的查询逻辑，但分区数据要次日凌晨才写入
-- JOIN 结果必然为空，但 SQL 执行成功不报错
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    a.platform,
    j.job_id,
    j.action,
    j.status                                      AS job_status,
    j.submit_time,
    j.start_time                                  AS job_start,
    j.end_time                                    AS job_end,
    j.failed_reason
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
-- ❌ 查询明天的分区，数据尚未到位，JOIN 结果为空
WHERE a.dt = '20270310'
  AND j.dt = '20270310'
  AND a.`result` != 0;


-- ---------------------------------------------------------------------------
-- Case 1b: 多表 INNER JOIN 中某张表分区为空
-- 四表 JOIN，只要其中任何一张表的当天分区为空，整个结果集就为空
-- 在真实 ETL 中，task 表的数据采集往往比 app 表晚几个小时
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    j.job_id,
    j.status                                      AS job_status,
    s.stage_id,
    s.num_tasks,
    s.status                                      AS stage_status,
    t.task_id,
    t.status                                      AS task_status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time,
    -- 计算 GC 占比
    ROUND(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1), 2)
                                                  AS gc_pct
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
-- ❌ task 表数据延迟，当天分区可能为空，导致整个 JOIN 为空
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE a.dt = '20270308'
ORDER BY t.task_run_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 1c: 分区日期格式不一致导致匹配到空分区
-- 表实际分区值为 '20270308' 格式，但查询用了 '2027-03-08' 格式
-- 分区匹配不到任何数据，等同于空表 JOIN
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    j.job_id,
    j.status,
    j.failed_reason,
    (j.end_time - j.start_time)               AS job_duration_ms
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
-- ❌ 分区日期格式错误：表中为 '20270308'，这里写成 '2027-03-08'
-- 匹配不到分区，结果为空但不报错
WHERE a.dt = '2027-03-08'
  AND j.dt = '2027-03-08'
  AND a.`result` != 0
ORDER BY job_duration_ms DESC;
