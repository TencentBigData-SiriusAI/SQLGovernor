-- ============================================================================
-- Case 08: GROUP BY 聚合粒度与业务需求不匹配
-- ============================================================================
-- 【问题描述】
--   GROUP BY 的维度决定了聚合的粒度。粒度不匹配会导致：
--     1. 粒度过粗：多个维度被合并，指标被平均化，异常值被淹没
--     2. 粒度过细：聚合效果微弱，每组只有一行，聚合无意义
--     3. 维度遗漏：本应区分的维度被合并，不同维度的数据被混合聚合
--     4. 多余维度：GROUP BY 中加了不需要的列，打散了本应合并的数据
--
-- 【易犯场景】
--   1. 业务需要"用户级"指标，但 GROUP BY 细到了"app 级"
--   2. 业务需要"日级"趋势，但漏了日期维度导致全量聚合
--   3. GROUP BY 加了多余的明细字段，聚合被打散
--   4. 多表 JOIN 后 GROUP BY 的粒度被下游表打散
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - GROUP BY 粒度可能与业务需求不匹配
--   - 建议确认聚合维度与下游需求一致
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 8a: GROUP BY 粒度过细，加了多余的明细字段导致聚合无效
-- 业务需求：统计每个用户的 app 总数和平均时长
-- ❌ 错误：GROUP BY 加了 app_id，每组只有一行，聚合毫无意义
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.app_id,                                               -- ❌ 多余维度
    COUNT(*)                                                AS app_count,
    AVG(a.end_time - a.start_time)                          AS avg_duration,
    MAX(a.executor_num)                                     AS max_executors
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.`user`, a.app_id                                 -- ❌ 加了 app_id，粒度打散
ORDER BY app_count DESC
LIMIT 100;

-- ✅ 正确写法：只按 user 分组
-- GROUP BY a.`user`


-- ---------------------------------------------------------------------------
-- Case 8b: GROUP BY 粒度过粗，漏了日期维度导致跨天合并
-- 业务需求：查看最近 7 天每日的 app 成功/失败趋势
-- ❌ 错误：GROUP BY 中没有 dt，7 天数据合在一起
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    COUNT(*)                                                AS total_apps,
    SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END)          AS success_count,
    SUM(CASE WHEN a.result != 0 THEN 1 ELSE 0 END)         AS fail_count,
    AVG(a.end_time - a.start_time)                          AS avg_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt BETWEEN '20260302' AND '20260308'
GROUP BY a.platform                                         -- ❌ 缺少 dt
ORDER BY total_apps DESC;

-- ✅ 正确写法：加上日期维度
-- GROUP BY a.dt, a.platform


-- ---------------------------------------------------------------------------
-- Case 8c: 多表 JOIN 后聚合粒度被下游表打散
-- 业务需求：统计每个 app 的 stage 数量和 task 数量
-- ❌ 错误：直接三表 JOIN 后 GROUP BY app，task 膨胀了 stage 计数
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    COUNT(s.stage_id)                                       AS stage_count,   -- ❌ 被 task 膨胀
    COUNT(t.task_id)                                        AS task_count,
    AVG(s.end_time - s.start_time)                          AS avg_stage_dur,
    AVG(t.task_run_time)                                    AS avg_task_rt
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id AND a.dt = s.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name
ORDER BY stage_count DESC
LIMIT 100;

-- ✅ 正确写法：分层聚合后再 JOIN
-- WITH stage_agg AS (SELECT app_id, COUNT(DISTINCT stage_id) AS stage_count, ... GROUP BY app_id)
-- WITH task_agg AS (SELECT app_id, COUNT(*) AS task_count, ... GROUP BY app_id)
-- SELECT a.*, sa.stage_count, ta.task_count FROM app a LEFT JOIN stage_agg sa ...


-- ---------------------------------------------------------------------------
-- Case 8d: 维度遗漏导致不同平台数据被混合聚合
-- 业务需求：各平台各用户的失败率排行
-- ❌ 错误：GROUP BY 漏了 platform，不同平台的 user 被合并
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    COUNT(*)                                                AS total_apps,
    SUM(CASE WHEN a.result != 0 THEN 1 ELSE 0 END)         AS fail_count,
    ROUND(SUM(CASE WHEN a.result != 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 2)                                      AS fail_rate
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.`user`                                           -- ❌ 缺少 platform 维度
ORDER BY fail_rate DESC
LIMIT 50;

-- ✅ 正确写法：
-- GROUP BY a.`user`, a.platform
