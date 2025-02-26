-- ============================================================================
-- Case 07: CTE（WITH 子句）之间缺少关联导致笛卡尔积
-- ============================================================================
-- 【问题描述】
--   使用 CTE 组织复杂查询时，多个 CTE 在最终 SELECT 中 JOIN，
--   如果关联条件缺失或不正确，同样会产生笛卡尔积。CTE 场景下
--   笛卡尔积更隐蔽，因为：
--     1. CTE 的数据已经过聚合/过滤，开发者可能低估结果行数
--     2. CTE 名称简短，容易看漏关联条件
--     3. 多个 CTE 的维度（粒度）不同，关联容易出错
--     4. CTE 之间的关联关系不如直接 JOIN 表那么直观
--
-- 【易犯场景】
--   1. 分步写 CTE 统计指标，最终合并时忘了关联
--   2. CTE 维度不一致（一个按 app，一个按 user），强行 JOIN
--   3. 全局统计 CTE 与明细 CTE JOIN 时没有关联键
--   4. 调试时临时去掉关联条件后忘记恢复
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - CTE 之间 JOIN 缺少关联条件
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 7a: 两个 CTE 维度不同，JOIN 时缺少关联
-- app_daily 按 user+platform 统计，job_daily 按 app_id 统计
-- 两者维度不同，直接 JOIN 会产生笛卡尔积
-- ❌ 错误：CTE 维度不一致且无关联条件
-- ---------------------------------------------------------------------------
WITH app_daily AS (
    SELECT
        a.`user`,
        a.platform,
        COUNT(*)                      AS app_count,
        SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END)
                                      AS success_count,
        AVG(a.end_time - a.start_time)
                                      AS avg_app_duration,
        AVG(a.executor_num)           AS avg_executors,
        SUM(a.executor_memory * a.executor_num)
                                      AS total_memory_alloc
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
    GROUP BY a.`user`, a.platform
),
job_daily AS (
    SELECT
        j.app_id,
        COUNT(*)                      AS job_count,
        SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END)
                                      AS fail_count,
        AVG(j.end_time - j.start_time)
                                      AS avg_job_duration,
        MAX(j.end_time - j.submit_time)
                                      AS max_total_time
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
    GROUP BY j.app_id
)
SELECT
    ad.`user`,
    ad.platform,
    ad.app_count,
    ad.success_count,
    ad.avg_app_duration,
    jd.app_id,
    jd.job_count,
    jd.fail_count,
    jd.avg_job_duration
FROM app_daily ad
INNER JOIN job_daily jd
    ON 1 = 1                          -- ❌ 恒真关联，两个 CTE 维度不匹配
ORDER BY ad.app_count DESC
LIMIT 100;

-- ✅ 正确写法：通过 app 表桥接或统一 CTE 维度
-- WITH app_daily AS (... GROUP BY a.app_id, a.`user`, a.platform),
--      job_daily AS (... GROUP BY j.app_id)
-- SELECT ... FROM app_daily ad
-- INNER JOIN job_daily jd ON ad.app_id = jd.app_id


-- ---------------------------------------------------------------------------
-- Case 7b: 全局统计 CTE 与明细 CTE 笛卡尔积
-- global_stats 只有 1 行，与 app_detail 的每一行都做 CROSS JOIN
-- 虽然全局统计只有 1 行，但语义上仍是笛卡尔积
-- ❌ 错误：全局统计与明细直接 CROSS JOIN
-- ---------------------------------------------------------------------------
WITH global_stats AS (
    SELECT
        COUNT(DISTINCT a.app_id)      AS total_apps,
        COUNT(DISTINCT a.`user`)      AS total_users,
        AVG(a.end_time - a.start_time)
                                      AS global_avg_duration,
        SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*)                    AS global_success_rate
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
),
app_detail AS (
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        a.platform,
        a.result,
        a.executor_num,
        (a.end_time - a.start_time)   AS app_duration
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
      AND a.result != 0
)
SELECT
    ad.app_id,
    ad.app_name,
    ad.`user`,
    ad.app_duration,
    gs.total_apps,
    gs.global_avg_duration,
    -- 对比当前 app 与全局平均
    ROUND(ad.app_duration / GREATEST(gs.global_avg_duration, 1), 2)
                                      AS duration_ratio
FROM app_detail ad
CROSS JOIN global_stats gs            -- ❌ CROSS JOIN（虽然 gs 只有1行但仍是笛卡尔积）
ORDER BY duration_ratio DESC
LIMIT 100;

-- ✅ 更好的写法：使用窗口函数避免 CROSS JOIN
-- SELECT ...,
--     AVG(app_duration) OVER() AS global_avg_duration
-- FROM app_detail


-- ---------------------------------------------------------------------------
-- Case 7c: 三个 CTE 中两两之间缺少关联
-- app_cte、stage_cte、task_cte 分别统计，最终 JOIN 时关联不完整
-- ❌ 错误：stage_cte 与 task_cte 之间缺少 stage_id 关联
-- ---------------------------------------------------------------------------
WITH app_cte AS (
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        a.platform,
        a.result,
        a.executor_num
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
      AND a.result != 0
),
stage_cte AS (
    SELECT
        s.app_id,
        s.stage_id,
        s.num_tasks,
        s.status,
        (s.end_time - s.start_time)   AS stage_duration
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.dt = '20260308'
),
task_cte AS (
    SELECT
        t.app_id,
        t.stage_id,
        COUNT(*)                      AS actual_tasks,
        AVG(t.task_run_time)          AS avg_run_time,
        SUM(t.gc_time)                AS total_gc
    FROM spark_analytics.spark_task_metrics t
    WHERE t.dt = '20260308'
    GROUP BY t.app_id, t.stage_id
)
SELECT
    ac.app_id,
    ac.app_name,
    sc.stage_id,
    sc.num_tasks,
    tc.actual_tasks,
    tc.avg_run_time
FROM app_cte ac
INNER JOIN stage_cte sc ON ac.app_id = sc.app_id      -- ✅ 正确
INNER JOIN task_cte tc ON sc.app_id = tc.app_id        -- ❌ 缺少 stage_id 关联
    -- 同一 app 下的所有 stage 与所有 task 聚合结果交叉
ORDER BY tc.avg_run_time DESC
LIMIT 200;

-- ✅ 正确写法：
-- INNER JOIN task_cte tc
--     ON sc.app_id = tc.app_id
--     AND sc.stage_id = tc.stage_id
