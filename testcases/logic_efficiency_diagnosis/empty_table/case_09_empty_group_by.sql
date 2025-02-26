-- ============================================================================
-- Case 09: 空表上的 GROUP BY —— 产出0行结果
-- ============================================================================
-- 【问题描述】
--   GROUP BY 在空表/空分区上执行时，由于没有任何分组行，结果为 0 行
--   （注意：不是返回一行 NULL/0 值，而是完全没有行）。这与不带 GROUP BY
--   的全局聚合行为不同（后者返回1行）。差异导致的问题：
--     1. 下游期望"每个 user 都有一行统计"，但空表时0行
--     2. HAVING 条件永远不被评估（无分组 → 无 HAVING 过滤）
--     3. GROUP BY 嵌套在子查询中时，外层 JOIN 无匹配行
--     4. 基于 GROUP BY 结果做"是否有数据"判断时逻辑出错
--
-- 【易犯场景】
--   1. 定时报表每天 GROUP BY user 汇总，空分区日报表为空
--   2. HAVING COUNT > N 过滤后期望至少有一行，空表时0行
--   3. GROUP BY 结果写入汇总表，空分区导致汇总表缺少该天记录
--   4. 与维度表做补全时，GROUP BY 空表侧无法补全
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - GROUP BY 的源数据可能为空，结果将为0行
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 9a: 基础 GROUP BY 在空分区上 —— 0行 vs 1行
-- 展示 GROUP BY 与不带 GROUP BY 在空表上的行为差异
-- ---------------------------------------------------------------------------
-- 查询 A：带 GROUP BY —— 空分区返回 0 行
SELECT
    `user`,
    platform,
    COUNT(*)                                      AS app_count,
    SUM(CASE WHEN `result` = 0 THEN 1 ELSE 0 END)  AS success_count,
    AVG(executor_num)                             AS avg_executors,
    AVG(executor_memory)                          AS avg_memory,
    AVG(end_time - start_time)                    AS avg_duration_ms,
    MAX(end_time - start_time)                    AS max_duration_ms,
    MIN(start_time)                               AS first_run_time,
    MAX(end_time)                                 AS last_end_time
FROM spark_analytics.spark_app_metrics
-- ❌ 空分区 → GROUP BY 无输入 → 0行结果
-- 下游 ETL 期望每个 user 有一行，但空分区时一行都没有
WHERE dt = '20270310'
GROUP BY `user`, platform
ORDER BY app_count DESC;


-- ---------------------------------------------------------------------------
-- Case 9b: GROUP BY + HAVING 在空表上
-- HAVING 条件根本不会被评估，因为没有任何分组产生
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    COUNT(*)                                      AS stage_count,
    SUM(s.num_tasks)                              AS total_tasks,
    AVG(s.end_time - s.start_time)                AS avg_stage_duration,
    MAX(s.num_tasks)                              AS max_stage_tasks,
    -- 异常 stage 占比
    ROUND(
        SUM(CASE WHEN s.status = 'FAILED' THEN 1 ELSE 0 END) * 100.0
        / GREATEST(COUNT(*), 1),
        2
    )                                              AS fail_rate_pct
FROM spark_analytics.spark_stage_metrics s
-- ❌ 空分区
WHERE s.dt = '20270310'
GROUP BY s.app_id
-- ❌ HAVING 不会被评估（0个分组），永远不会过滤掉任何东西
HAVING COUNT(*) >= 5
   AND SUM(s.num_tasks) > 100
ORDER BY total_tasks DESC
LIMIT 50;


-- ---------------------------------------------------------------------------
-- Case 9c: GROUP BY 结果作为子查询参与外层 JOIN
-- 内层 GROUP BY 空表返回0行，外层 INNER JOIN 无匹配，最终结果为空
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    a.platform,
    stage_summary.stage_count,
    stage_summary.total_tasks,
    stage_summary.avg_stage_duration,
    stage_summary.max_stage_tasks,
    -- ❌ stage_summary 子查询为空 → INNER JOIN 结果为空
    ROUND(stage_summary.total_tasks * 1.0
        / GREATEST(stage_summary.stage_count, 1), 2)
                                                  AS avg_tasks_per_stage
FROM spark_analytics.spark_app_metrics a
INNER JOIN (
    -- ❌ 空分区 GROUP BY → 0行 → JOIN 无匹配
    SELECT
        app_id,
        COUNT(*)                              AS stage_count,
        SUM(num_tasks)                        AS total_tasks,
        AVG(end_time - start_time)            AS avg_stage_duration,
        MAX(num_tasks)                        AS max_stage_tasks
    FROM spark_analytics.spark_stage_metrics
    WHERE dt = '20270310'        -- ❌ 空分区
    GROUP BY app_id
) stage_summary
    ON a.app_id = stage_summary.app_id
WHERE a.dt = '20270308'
  AND a.`result` != 0
ORDER BY stage_summary.total_tasks DESC
LIMIT 100;
