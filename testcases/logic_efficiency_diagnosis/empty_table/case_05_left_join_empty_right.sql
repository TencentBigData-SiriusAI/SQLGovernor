-- ============================================================================
-- Case 05: LEFT JOIN 右表为空 —— 结果"看似正常"实则异常
-- ============================================================================
-- 【问题描述】
--   LEFT JOIN 的特性是：即使右表没有匹配行，左表的数据仍会保留（右侧
--   字段填充 NULL）。当右表整体为空时，LEFT JOIN 结果与单表查询相同，
--   所有右表字段为 NULL。这种情况极其危险：
--     1. 结果行数与左表一致，看起来"有数据"，实际右侧全部缺失
--     2. 基于右侧字段的计算全部返回 NULL，但不报错
--     3. 数据看似完整但关键维度缺失，产出的报表/指标有误导性
--     4. 下游 INNER JOIN 该结果时，NULL 值导致行被过滤掉
--
-- 【易犯场景】
--   1. 维度表为空（新建表尚未初始化数据），事实表 LEFT JOIN 维度表
--   2. 增量数据表当天无新增，LEFT JOIN 后增量字段全为 NULL
--   3. 依赖外部系统的数据表，外部接口故障导致数据为空
--   4. LEFT JOIN 多张表，其中某张为空但查询不会失败
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - LEFT JOIN 的右表可能为空，右侧字段将全部为 NULL
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 5a: LEFT JOIN 右表分区为空，右侧字段全 NULL
-- app 表有数据，但 job 表当天分区为空
-- LEFT JOIN 结果行数等于 app 表行数，job 字段全 NULL
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    a.platform,
    a.start_time,
    a.end_time,
    -- ❌ 如果 job 表为空，以下字段全部为 NULL
    j.job_id,
    j.action,
    j.status                                      AS job_status,
    j.submit_time,
    j.failed_reason,
    -- ❌ 基于 NULL 字段的计算也为 NULL，但不报错
    (j.end_time - j.start_time)                   AS job_duration_ms,
    (j.start_time - j.submit_time)                AS job_queue_ms,
    -- ❌ COALESCE 掩盖了右表为空的问题
    COALESCE(j.status, '未知')                    AS job_status_safe,
    COALESCE(j.job_id, 'no_job')                  AS job_id_safe
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20270308'
  -- ❌ job 表查未来分区，必然为空
  AND j.dt = '20270310'
ORDER BY a.start_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 5b: 多级 LEFT JOIN，中间某层为空导致后续层全 NULL
-- app LEFT JOIN job LEFT JOIN stage LEFT JOIN task
-- 如果 stage 表为空，stage 和 task 的字段全部为 NULL
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    j.job_id,
    j.status                                      AS job_status,
    -- ❌ 如果 stage 表为空，以下字段全 NULL
    s.stage_id,
    s.num_tasks,
    s.status                                      AS stage_status,
    -- ❌ stage 为空导致 task JOIN 条件也为 NULL，task 字段也全 NULL
    t.task_id,
    t.task_run_time,
    t.gc_time,
    -- ❌ 基于多个可能为 NULL 字段的复合计算
    CASE
        WHEN s.num_tasks IS NOT NULL AND t.task_run_time IS NOT NULL
        THEN ROUND(t.gc_time * 100.0 / t.task_run_time, 2)
        ELSE NULL
    END                                            AS gc_pct,
    -- ❌ 统计 "有效" stage 数量：如果 stage 表为空则始终为0
    CASE WHEN s.stage_id IS NOT NULL THEN 1 ELSE 0 END
                                                  AS has_stage_flag
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
LEFT JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
LEFT JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE a.dt = '20270308'
ORDER BY a.start_time DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- Case 5c: LEFT JOIN 空表后做聚合 —— 指标严重偏差
-- 左表有数据、右表为空，聚合时右侧 NULL 不参与 AVG/SUM
-- 导致指标失真但不报错
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.platform,
    COUNT(*)                                      AS app_count,
    COUNT(j.job_id)                               AS job_count,
    -- ❌ job 表为空时 job_count=0, app_count>0, 比值为0
    ROUND(COUNT(j.job_id) * 1.0 / GREATEST(COUNT(*), 1), 2)
                                                  AS jobs_per_app,
    -- ❌ SUM(NULL) = NULL，不是0
    SUM(j.end_time - j.start_time)                AS total_job_duration,
    -- ❌ AVG(NULL) = NULL
    AVG(j.end_time - j.start_time)                AS avg_job_duration,
    -- ❌ MAX/MIN(NULL) = NULL
    MAX(j.end_time - j.start_time)                AS max_job_duration,
    -- 有效 job 占比为0，但报表可能误判为"所有 app 都没有 job"
    ROUND(
        SUM(CASE WHEN j.status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0
        / GREATEST(COUNT(*), 1),
        2
    )                                              AS job_success_pct
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND j.dt = '20270310'            -- ❌ job 表查空分区
WHERE a.dt = '20270308'
GROUP BY a.`user`, a.platform
ORDER BY app_count DESC
LIMIT 50;
