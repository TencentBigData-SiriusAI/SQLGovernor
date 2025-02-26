-- ============================================================================
-- Case 04: 聚合粒度不匹配
-- ============================================================================
-- 【问题描述】
--   GROUP BY 的粒度决定了聚合结果的细节层级。常见错误包括：
--     1. GROUP BY 粒度过粗：业务需要 job 级别但只按 app 分组，丢失 job 细节
--     2. GROUP BY 粒度过细：包含不必要的字段，导致聚合结果过于碎片化
--     3. GROUP BY 与 SELECT 字段不一致，部分字段在聚合后语义不明确
--     4. 嵌套聚合的粒度层级错误，内外层聚合粒度不匹配
--     5. 窗口函数 PARTITION BY 粒度与业务需求不一致
--
-- 【易犯场景】
--   1. 统计每个 App 的 Job 汇总信息，但 GROUP BY 漏掉了 job_id
--   2. 想按 App 维度汇总，但多加了 stage_id 导致结果散碎
--   3. 窗口函数 PARTITION BY 用了 app_id，但业务要全局排名
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - GROUP BY 粒度可能与业务需求的统计维度不一致
--   - 建议检查聚合粒度是否匹配业务分析的层级
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 4a: GROUP BY 粒度过粗丢失 job 级别区分
-- 业务需求：统计每个 App 下每个 Job 的 Stage 数量
-- ❌ 错误：GROUP BY 只有 app_id，丢失了 job 级别的区分，
--   所有 job 的 stage 被合并到 app 级别
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    COUNT(DISTINCT s.stage_id)                               AS stage_count_per_job,
    SUM(s.task_num)                                          AS total_tasks_per_job
FROM spark_analytics.spark_job_metrics j
INNER JOIN spark_analytics.spark_stage_metrics s
    ON j.app_id = s.app_id
    AND j.dt = s.dt
WHERE j.dt = '20260308'
GROUP BY j.app_id;
-- ❌ 别名写 stage_count_per_job / total_tasks_per_job，暗示是 job 粒度
-- ❌ 但 GROUP BY 只有 app_id，实际是 app 粒度的汇总
-- ✅ 正确写法：GROUP BY 加上 job_id
-- GROUP BY j.app_id, j.job_id


-- ---------------------------------------------------------------------------
-- Case 4b: GROUP BY 粒度过细含不必要字段
-- 业务需求：统计每个 App 的总 Stage 数和总任务数
-- ❌ 错误：GROUP BY 包含了 stage_id，导致每个 stage 单独一行，
--   聚合结果过于碎片化，失去了 app 粒度的汇总意义
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    COUNT(*)                                                 AS stage_count,
    SUM(s.task_num)                                          AS total_tasks
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
GROUP BY s.app_id, s.stage_id;
-- ❌ 加了 stage_id 后，每个 stage 单独一行，COUNT(*) 几乎都是 1
-- ❌ 业务需要的是 app 维度汇总，但结果是 stage 维度，不是想要的
-- ✅ 正确写法：去掉 stage_id，只保留 app_id
-- SELECT s.app_id,
--     COUNT(DISTINCT s.stage_id) AS stage_count,
--     SUM(s.task_num) AS total_tasks
-- FROM spark_analytics.spark_stage_metrics s
-- WHERE s.dt = '20260308'
-- GROUP BY s.app_id;


-- ---------------------------------------------------------------------------
-- Case 4c: GROUP BY 包含高基数字段导致聚合无意义
-- 业务需求：统计每个用户的 App 数量和总耗时
-- ❌ 错误：GROUP BY 包含了 app_name（高基数），每个 app_name 几乎唯一，
--   导致 COUNT(*) 几乎都是 1，聚合毫无意义
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.app_name,
    COUNT(*)                                                 AS app_count,
    SUM(a.end_time - a.start_time)                           AS total_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.`user`, a.app_name;
-- ❌ GROUP BY 包含 app_name（每个 app 名称几乎唯一）
-- ❌ 聚合结果每行 app_count = 1，等同于没有聚合
-- ❌ 业务想看"每个用户的汇总"，但结果是"每个 app 一行"
-- ✅ 正确写法：只按 user 分组
-- SELECT a.`user`,
--     COUNT(*) AS app_count,
--     SUM(a.end_time - a.start_time) AS total_duration
-- FROM spark_analytics.spark_app_metrics a
-- WHERE a.dt = '20260308'
-- GROUP BY a.`user`;


-- ---------------------------------------------------------------------------
-- Case 4d: 嵌套聚合的粒度层级错误
-- 业务需求：统计每个平台下 App 的"平均 Job 数"
-- ❌ 错误：内层按 (platform, app_id) 分组统计 job 数没问题，
--   但外层直接 SUM 而非 AVG，得到的是总 job 数而非平均 job 数
-- ---------------------------------------------------------------------------
SELECT
    platform,
    SUM(job_count)                                           AS avg_jobs_per_app,
    COUNT(*)                                                 AS app_count
FROM (
    SELECT
        a.platform,
        a.app_id,
        COUNT(j.job_id)                                      AS job_count
    FROM spark_analytics.spark_app_metrics a
    INNER JOIN spark_analytics.spark_job_metrics j
        ON a.app_id = j.app_id
        AND a.dt = j.dt
    WHERE a.dt = '20260308'
    GROUP BY a.platform, a.app_id
) app_jobs
GROUP BY platform;
-- ❌ 外层别名写 avg_jobs_per_app，但用的是 SUM 不是 AVG
-- ❌ 结果是每个平台的总 job 数，不是平均 job 数
-- ✅ 正确写法：外层用 AVG
-- AVG(job_count) AS avg_jobs_per_app


-- ---------------------------------------------------------------------------
-- Case 4e: 窗口函数 PARTITION BY 粒度与业务不匹配
-- 业务需求：在全局范围内找出耗时 Top 10 的 App
-- ❌ 错误：PARTITION BY platform 导致每个平台内部各排一次名，
--   不是全局排名，可能返回超过 10 条记录
-- ---------------------------------------------------------------------------
SELECT *
FROM (
    SELECT
        a.app_id,
        a.app_name,
        a.platform,
        (a.end_time - a.start_time)                          AS duration_ms,
        ROW_NUMBER() OVER (
            PARTITION BY a.platform
            ORDER BY (a.end_time - a.start_time) DESC
        )                                                    AS global_rank
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt = '20260308'
) ranked
WHERE global_rank <= 10;
-- ❌ 别名写 global_rank，但 PARTITION BY platform 是分组排名
-- ❌ 每个平台各返回 10 条，总数 = 平台数 × 10，不是全局 Top 10
-- ✅ 正确写法：去掉 PARTITION BY，做全局排名
-- ROW_NUMBER() OVER (ORDER BY (a.end_time - a.start_time) DESC) AS global_rank
