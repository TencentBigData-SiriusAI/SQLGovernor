-- ============================================================================
-- Case 10: 空表级联传播 —— 上游空表通过 ETL 链路层层传播
-- ============================================================================
-- 【问题描述】
--   在多层 ETL 链路中，上游表为空时，空数据会沿着 JOIN/聚合/子查询
--   一层层向下传播，最终导致整条链路产出的数据都为空或异常：
--     1. ODS 层采集空 → DWD 层清洗空 → DWS 层聚合空 → ADS 层报表空
--     2. 中间某一层的空检查缺失，下游全部受影响
--     3. 多条链路交叉引用时，一条空链路会"感染"其他链路
--     4. 历史数据回刷时，某些分区为空导致下游级联失效
--
-- 【易犯场景】
--   1. 凌晨采集链路故障 → 当天分区为空 → 整条日报链路空跑
--   2. 多源数据一处故障 → 依赖该源的所有下游表为空
--   3. 重跑历史数据时先清空再写入，但写入失败，下游已触发
--   4. 测试环境数据不全，多层 ETL 后问题逐层放大
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - 多层依赖链路中存在空表风险，建议在每层添加数据非空校验
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 10a: 模拟三层 ETL 链路的空表传播
-- Layer 1 (DWD): 从 app + job 关联清洗
-- Layer 2 (DWS): 在 DWD 基础上 JOIN stage 做聚合
-- Layer 3 (ADS): 在 DWS 基础上 JOIN task 做最终报表
-- 如果 app 表空分区，整条链路全部为空
-- ---------------------------------------------------------------------------
WITH
-- ============ Layer 1: DWD 明细层 ============
-- 从 app + job 关联得到应用级明细
dwd_app_job AS (
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        a.`result`,
        a.platform,
        a.executor_num,
        a.executor_memory,
        a.start_time                              AS app_start,
        a.end_time                                AS app_end,
        j.job_id,
        j.status                                  AS job_status,
        j.failed_reason,
        j.submit_time                             AS job_submit,
        j.start_time                              AS job_start,
        j.end_time                                AS job_end,
        a.dt
    FROM spark_analytics.spark_app_metrics a
    -- ❌ 如果 app 表当天分区为空，INNER JOIN 结果为空
    -- 整个 DWD 层为空，向下传播
    INNER JOIN spark_analytics.spark_job_metrics j
        ON a.app_id = j.app_id
        AND a.dt = j.dt
    WHERE a.dt = '20270310'          -- ❌ 可能的空分区
      AND j.dt = '20270310'
),

-- ============ Layer 2: DWS 汇总层 ============
-- 在 DWD 基础上关联 stage，做 app 级别汇总
dws_app_summary AS (
    SELECT
        d.app_id,
        d.app_name,
        d.`user`,
        d.`result`,
        d.platform,
        d.executor_num,
        d.executor_memory,
        (d.app_end - d.app_start)                AS app_duration_ms,
        COUNT(DISTINCT d.job_id)                  AS job_count,
        SUM(CASE WHEN d.job_status = 'SUCCEEDED' THEN 1 ELSE 0 END)
                                                  AS success_jobs,
        COUNT(DISTINCT s.stage_id)                AS stage_count,
        SUM(s.num_tasks)                          AS total_tasks,
        AVG(s.end_time - s.start_time)            AS avg_stage_duration,
        d.dt
    -- ❌ dwd_app_job 为空 → DWS 层 FROM 为空 → 聚合无输入 → 0行结果
    FROM dwd_app_job d
    LEFT JOIN spark_analytics.spark_stage_metrics s
        ON d.app_id = s.app_id
        AND d.dt = s.dt
    GROUP BY
        d.app_id, d.app_name, d.`user`, d.`result`,
        d.platform, d.executor_num, d.executor_memory,
        d.app_start, d.app_end, d.dt
)

-- ============ Layer 3: ADS 应用层（最终报表） ============
-- 在 DWS 基础上关联 task 级别数据做最终分析
SELECT
    dws.app_id,
    dws.app_name,
    dws.`user`,
    dws.platform,
    dws.`result`,
    dws.executor_num,
    dws.executor_memory,
    dws.app_duration_ms,
    dws.job_count,
    dws.success_jobs,
    dws.stage_count,
    dws.total_tasks,
    dws.avg_stage_duration,
    -- ❌ DWS 为空 → 以下所有字段无意义
    COUNT(t.task_id)                              AS actual_task_count,
    AVG(t.task_run_time)                          AS avg_task_runtime,
    MAX(t.gc_time)                                AS max_gc_time,
    SUM(t.shuffle_read_bytes)                     AS total_shuffle_read,
    SUM(t.shuffle_write_bytes)                    AS total_shuffle_write,
    -- 综合评分（空数据时全为 NULL）
    ROUND(
        COALESCE(dws.success_jobs, 0) * 100.0
        / GREATEST(COALESCE(dws.job_count, 1), 1),
        2
    )                                              AS job_success_rate,
    CASE
        WHEN dws.app_duration_ms > 3600000 THEN '长任务'
        WHEN dws.app_duration_ms > 600000  THEN '中等任务'
        ELSE '短任务'
    END                                            AS duration_level
-- ❌ DWS 为空 → FROM 为空 → 最终结果为空
FROM dws_app_summary dws
LEFT JOIN spark_analytics.spark_task_metrics t
    ON dws.app_id = t.app_id
    AND dws.dt = t.dt
GROUP BY
    dws.app_id, dws.app_name, dws.`user`, dws.platform,
    dws.`result`, dws.executor_num, dws.executor_memory,
    dws.app_duration_ms, dws.job_count, dws.success_jobs,
    dws.stage_count, dws.total_tasks, dws.avg_stage_duration,
    dws.dt
ORDER BY dws.app_duration_ms DESC
LIMIT 500;


-- ---------------------------------------------------------------------------
-- Case 10b: 多源交叉引用中的空表感染
-- 两条独立链路的结果做 JOIN，一条为空导致另一条也受影响
-- ---------------------------------------------------------------------------
SELECT
    app_report.`user`,
    app_report.platform,
    app_report.app_count,
    app_report.avg_duration_ms,
    task_report.total_tasks,
    task_report.avg_gc_pct,
    -- ❌ 任一侧为空则此指标无意义
    ROUND(task_report.total_tasks * 1.0
        / GREATEST(app_report.app_count, 1), 2)
                                                  AS tasks_per_app
FROM (
    -- 链路 A：app 维度统计
    SELECT
        `user`,
        platform,
        COUNT(*)                              AS app_count,
        AVG(end_time - start_time)            AS avg_duration_ms
    FROM spark_analytics.spark_app_metrics
    WHERE dt = '20270308'
    GROUP BY `user`, platform
) app_report
-- ❌ INNER JOIN：如果 task_report 为空，整个结果为空
INNER JOIN (
    -- 链路 B：task 维度统计 —— ❌ 空分区导致0行结果
    SELECT
        a.`user`,
        a.platform,
        COUNT(t.task_id)                      AS total_tasks,
        ROUND(
            AVG(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1)),
            2
        )                                     AS avg_gc_pct
    FROM spark_analytics.spark_app_metrics a
    INNER JOIN spark_analytics.spark_task_metrics t
        ON a.app_id = t.app_id
        AND a.dt = t.dt
    -- ❌ task 表查空分区
    WHERE a.dt = '20270310'
      AND t.dt = '20270310'
    GROUP BY a.`user`, a.platform
) task_report
    ON app_report.`user` = task_report.`user`
    AND app_report.platform = task_report.platform
ORDER BY app_report.app_count DESC;
