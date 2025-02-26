-- ============================================================================
-- Case 10: 多表聚合膨胀
-- ============================================================================
-- 【问题描述】
--   先 JOIN 再聚合（SUM/COUNT）会导致数据膨胀的问题：
--     1. 先 JOIN 再 COUNT：1:N 关系中 COUNT 结果是 N 表的行数而非 1 表的行数
--     2. 先 JOIN 再 SUM：1 表的字段值被重复累加 N 次
--     3. 多层 1:N JOIN 导致指数级膨胀
--     4. 应先聚合再 JOIN 但顺序搞反
--     5. UNION ALL 替代 JOIN 的场景误用 JOIN
--
-- 【易犯场景】
--   1. app JOIN job 后 COUNT(*)，结果是 job 行数不是 app 数
--   2. app JOIN stage 后 SUM(driver_memory)，每个 app 的值被 stage 数翻倍
--   3. app JOIN job JOIN stage，膨胀倍数 = job数 × stage数
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - JOIN 后聚合可能导致数据膨胀
--   - 建议检查是否应先聚合再 JOIN
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 10a: 先 JOIN 再 COUNT 导致计数膨胀
-- 业务需求：统计 20260308 当天的 App 总数
-- ❌ 错误：先将 app 表 JOIN job 表（1:N），再 COUNT(*)，
--   结果是 job 行数而非 app 数
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)                                                 AS app_count
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308';
-- ❌ app:job = 1:N，JOIN 后行数 = SUM(每个 app 的 job 数)
-- ❌ COUNT(*) 统计的是 JOIN 后的行数（job 数），不是 app 数
-- ✅ 正确写法：直接从 app 表 COUNT，不需要 JOIN
-- SELECT COUNT(*) AS app_count
-- FROM spark_analytics.spark_app_metrics a
-- WHERE a.dt = '20260308';
-- 或者 JOIN 后用 COUNT(DISTINCT a.app_id)


-- ---------------------------------------------------------------------------
-- Case 10b: 先 JOIN 再 SUM 导致数值翻倍
-- 业务需求：统计各平台 App 的总 driver_memory 使用量
-- ❌ 错误：先将 app 表 JOIN stage 表（1:N），再 SUM(driver_memory)，
--   每个 app 的 driver_memory 被该 app 的 stage 数量翻倍
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    SUM(a.driver_memory)                                     AS total_driver_memory,
    COUNT(*)                                                 AS record_count
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
WHERE a.dt = '20260308'
GROUP BY a.platform;
-- ❌ app:stage = 1:N，JOIN 后每个 app 的 driver_memory 出现 N 次
-- ❌ SUM(driver_memory) 被 stage 数量放大
-- ❌ 如果一个 app 有 10 个 stage，其 driver_memory 被累加 10 次
-- ✅ 正确写法：先在 app 表内聚合，不需要 JOIN stage
-- SELECT a.platform,
--     SUM(a.driver_memory) AS total_driver_memory,
--     COUNT(*) AS app_count
-- FROM spark_analytics.spark_app_metrics a
-- WHERE a.dt = '20260308'
-- GROUP BY a.platform;


-- ---------------------------------------------------------------------------
-- Case 10c: 多层 1:N JOIN 导致指数级膨胀
-- 业务需求：统计每个 App 的总输入数据量（从 Stage 和 Task 汇总）
-- ❌ 错误：app JOIN stage JOIN task（1:N:M），
--   stage 的 input_size 被 task 数量放大，app 的字段被 stage×task 放大
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    SUM(s.input_size)                                        AS total_stage_input,
    SUM(t.input_size)                                        AS total_task_input,
    COUNT(*)                                                 AS total_records
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name;
-- ❌ stage:task = 1:M，每个 stage 的 input_size 被 task 数放大
-- ❌ total_stage_input = SUM(每个stage的input_size × 该stage的task数)
-- ❌ total_records = stage数 × 平均每stage的task数，行数爆炸
-- ✅ 正确写法：分别聚合再 JOIN
-- WITH stage_agg AS (
--     SELECT app_id, SUM(input_size) AS total_stage_input
--     FROM spark_analytics.spark_stage_metrics
--     WHERE dt = '20260308' GROUP BY app_id
-- ), task_agg AS (
--     SELECT app_id, SUM(input_size) AS total_task_input
--     FROM spark_analytics.spark_task_metrics
--     WHERE dt = '20260308' GROUP BY app_id
-- )
-- SELECT a.app_id, a.app_name, sa.total_stage_input, ta.total_task_input
-- FROM spark_analytics.spark_app_metrics a
-- LEFT JOIN stage_agg sa ON a.app_id = sa.app_id
-- LEFT JOIN task_agg ta ON a.app_id = ta.app_id
-- WHERE a.dt = '20260308';


-- ---------------------------------------------------------------------------
-- Case 10d: 应先聚合再 JOIN 但顺序搞反
-- 业务需求：统计每个 App 的 Job 数量和 Stage 数量
-- ❌ 错误：先 JOIN job 和 stage 再 COUNT，
--   由于 job 和 stage 之间没有直接关联，产生笛卡尔积效应
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    COUNT(DISTINCT j.job_id)                                 AS job_count,
    COUNT(DISTINCT s.stage_id)                               AS stage_count,
    COUNT(*)                                                 AS total_rows
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name;
-- ❌ job 和 stage 通过 app_id 间接关联，产生 job数×stage数 的行数
-- ❌ total_rows = job_count × stage_count（膨胀严重）
-- ❌ 虽然用 COUNT(DISTINCT) 得到的数字对了，但中间计算浪费大量资源
-- ✅ 正确写法：分别聚合再 JOIN
-- WITH job_agg AS (
--     SELECT app_id, COUNT(*) AS job_count
--     FROM spark_analytics.spark_job_metrics
--     WHERE dt = '20260308' GROUP BY app_id
-- ), stage_agg AS (
--     SELECT app_id, COUNT(*) AS stage_count
--     FROM spark_analytics.spark_stage_metrics
--     WHERE dt = '20260308' GROUP BY app_id
-- )
-- SELECT a.app_id, a.app_name, ja.job_count, sa.stage_count
-- FROM spark_analytics.spark_app_metrics a
-- LEFT JOIN job_agg ja ON a.app_id = ja.app_id
-- LEFT JOIN stage_agg sa ON a.app_id = sa.app_id
-- WHERE a.dt = '20260308';


-- ---------------------------------------------------------------------------
-- Case 10e: UNION ALL 替代 JOIN 的场景误用 JOIN
-- 业务需求：合并 Stage 和 Task 两个层级的输入数据量统计
-- ❌ 错误：使用 JOIN 合并两个层级的数据，导致行数膨胀，
--   实际应使用 UNION ALL 纵向合并
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.input_size                                             AS stage_input,
    t.input_size                                             AS task_input
FROM spark_analytics.spark_stage_metrics s
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE s.dt = '20260308';
-- ❌ stage:task = 1:N，JOIN 后每个 stage 行出现 N 次（按 task 展开）
-- ❌ 如果只是想分别看 stage 和 task 的输入量，不需要 JOIN
-- ✅ 正确写法：UNION ALL 纵向合并（如果需要分别汇总）
-- SELECT app_id, stage_id, 'stage' AS level, input_size
-- FROM spark_analytics.spark_stage_metrics
-- WHERE dt = '20260308'
-- UNION ALL
-- SELECT app_id, stage_id, 'task' AS level, input_size
-- FROM spark_analytics.spark_task_metrics
-- WHERE dt = '20260308'
