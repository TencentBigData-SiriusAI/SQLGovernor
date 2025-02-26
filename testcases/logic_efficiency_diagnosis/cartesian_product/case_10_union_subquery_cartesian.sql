-- ============================================================================
-- Case 10: 复杂综合场景中的笛卡尔积
-- ============================================================================
-- 【问题描述】
--   在实际数仓开发中，SQL 往往包含 UNION ALL、子查询、CTE、窗口函数
--   等多种元素组合。在这些复杂场景下，笛卡尔积更加隐蔽：
--     1. UNION ALL 的某个分支中存在笛卡尔积
--     2. 子查询嵌套多层，内层的 JOIN 缺少关联
--     3. 窗口函数的 PARTITION BY 与 JOIN 条件不匹配
--     4. INSERT ... SELECT 中 SELECT 部分存在笛卡尔积
--   这类问题在代码审查中极难发现，往往在执行时才暴露。
--
-- 【易犯场景】
--   1. 多人协作时，不同人写的 UNION 分支关联条件不一致
--   2. 复制某个分支后修改不彻底
--   3. ETL 链路中上下游 SQL 的关联逻辑不对齐
--   4. 重构复杂查询时只改了部分 JOIN 条件
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - 复杂查询中存在笛卡尔积风险
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 10a: UNION ALL 中某个分支存在笛卡尔积
-- 三个 UNION 分支分别查 app/job/stage，第二个分支缺少关联条件
-- ❌ 错误：第二个 UNION 分支的 JOIN 条件缺失
-- ---------------------------------------------------------------------------
-- 分支1：app 单表查询（无问题）
SELECT
    a.app_id                          AS entity_id,
    'APP'                             AS entity_type,
    a.app_name                        AS entity_name,
    a.`user`,
    a.platform,
    (a.end_time - a.start_time)       AS duration_ms,
    a.result                          AS status_code,
    a.dt
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0

UNION ALL

-- 分支2：app + job 关联（❌ 缺关联条件，笛卡尔积）
SELECT
    j.job_id                          AS entity_id,
    'JOB'                             AS entity_type,
    CONCAT(a.app_name, '_', j.job_id) AS entity_name,
    a.`user`,
    a.platform,
    (j.end_time - j.start_time)       AS duration_ms,
    CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END
                                      AS status_code,
    j.dt
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.dt = j.dt           -- ❌ 只有日期关联
    -- 缺少 AND a.app_id = j.app_id
WHERE a.dt = '20260308'
  AND j.status = 'FAILED'

UNION ALL

-- 分支3：stage 单表查询（无问题）
SELECT
    s.stage_id                        AS entity_id,
    'STAGE'                           AS entity_type,
    CONCAT(s.app_id, '_stage_', s.stage_id)
                                      AS entity_name,
    ''                                AS `user`,
    ''                                AS platform,
    (s.end_time - s.start_time)       AS duration_ms,
    CASE WHEN s.status = 'COMPLETE' THEN 0 ELSE 1 END
                                      AS status_code,
    s.dt
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.num_tasks > 100
ORDER BY duration_ms DESC
LIMIT 500;

-- ✅ 正确写法：分支2添加 app_id 关联
-- ON a.app_id = j.app_id
--    AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 10b: INSERT ... SELECT 中 SELECT 存在笛卡尔积
-- 往结果表写数据的 ETL 语句中，SELECT 部分多表 JOIN 缺关联
-- ❌ 错误：写入结果表的数据是笛卡尔积膨胀后的，指标全部偏大
-- ---------------------------------------------------------------------------
INSERT OVERWRITE TABLE result_daily_metrics
    PARTITION (dt = '20260308')
SELECT
    a.app_id,
    a.`user`,
    a.platform,
    COUNT(DISTINCT j.job_id)          AS job_count,
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END)
                                      AS fail_job_count,
    COUNT(DISTINCT s.stage_id)        AS stage_count,
    SUM(s.num_tasks)                  AS total_tasks,
    AVG(s.end_time - s.start_time)    AS avg_stage_duration,
    MAX(s.end_time - s.start_time)    AS max_stage_duration
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt          -- ✅ 正确
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
    -- ❌ stage 只关联了 app，没有关联 job
    -- 一个 app 有多个 job，stage 与每个 job 都会匹配一次
    -- 导致 stage 数据被重复计数，SUM(num_tasks) 偏大
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.`user`, a.platform;

-- ✅ 正确写法：
-- 方案1：stage 通过 job.stage_ids EXPLODE 后精确关联
-- 方案2：分别统计 job 和 stage 指标，最后汇总避免 JOIN 膨胀


-- ---------------------------------------------------------------------------
-- Case 10c: 窗口函数 + JOIN 笛卡尔积 —— 窗口计算基于膨胀数据
-- 先做了有笛卡尔积的 JOIN，再在膨胀数据上做窗口函数
-- ❌ 错误：窗口函数是在笛卡尔积膨胀后的数据上计算的
-- ---------------------------------------------------------------------------
SELECT
    ranked.*
FROM (
    SELECT
        a.app_id,
        a.app_name,
        a.`user`,
        s.stage_id,
        s.num_tasks,
        t.task_id,
        t.task_run_time,
        t.gc_time,
        -- 窗口函数基于膨胀后的数据计算，排名和统计值都是错的
        ROW_NUMBER() OVER(
            PARTITION BY a.app_id
            ORDER BY t.task_run_time DESC
        )                             AS task_rank,
        AVG(t.task_run_time) OVER(
            PARTITION BY a.app_id
        )                             AS avg_run_time_per_app
    FROM spark_analytics.spark_app_metrics a
    INNER JOIN spark_analytics.spark_stage_metrics s
        ON a.dt = s.dt       -- ❌ 只有日期关联
        -- 缺少 AND a.app_id = s.app_id
    INNER JOIN spark_analytics.spark_task_metrics t
        ON s.app_id = t.app_id
        AND s.stage_id = t.stage_id
        AND s.dt = t.dt      -- ✅ stage-task 正确
    WHERE a.dt = '20260308'
      AND a.result != 0
) ranked
WHERE ranked.task_rank <= 10
ORDER BY ranked.app_id, ranked.task_rank;

-- ✅ 正确写法：
-- ON a.app_id = s.app_id
--    AND a.dt = s.dt
