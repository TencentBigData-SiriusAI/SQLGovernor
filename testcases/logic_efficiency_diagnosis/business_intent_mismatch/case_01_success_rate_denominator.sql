-- ============================================================================
-- Case 01: 成功率/失败率分母错误
-- ============================================================================
-- 【问题描述】
--   计算成功率或失败率时，分母的选取直接决定了比率的正确性。常见错误包括：
--     1. 用 COUNT(*) 当分母，包含了不应参与统计的无效记录
--     2. result 字段值含义混淆（0 代表成功而非失败）
--     3. 多表 JOIN 后行数膨胀导致分母偏大，比率偏低
--     4. 分子和分母的过滤条件不对齐，导致比率无业务意义
--     5. CASE WHEN 分支遗漏导致部分记录未被统计
--
-- 【易犯场景】
--   1. 统计 Spark App 的成功率时，直接用总行数做分母
--   2. 不了解 result=0 代表成功，将 0 当作失败来计数
--   3. app 和 job 表 JOIN 后，一个 app 对应多条 job，分母膨胀
--   4. 分子加了时间过滤但分母没加，导致比率失真
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - 成功率/失败率计算的分母选择可能不正确
--   - 建议检查分母是否为有效记录数而非总行数
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 1a: COUNT(*) 当分母而非有效记录数
-- 业务需求：统计 20260308 当天 Spark App 的失败率
-- ❌ 错误：COUNT(*) 包含了所有记录（含 result=NULL 等无效状态），
--   导致分母偏大，失败率偏低
-- ---------------------------------------------------------------------------
SELECT
    COUNT(CASE WHEN a.`result` != 0 THEN 1 END)               AS failed_count,
    COUNT(*)                                                 AS total_count,
    COUNT(CASE WHEN a.`result` != 0 THEN 1 END) * 100.0
        / COUNT(*)                                           AS fail_rate_pct
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308';
-- ❌ COUNT(*) 包含 result IS NULL 的无效记录，分母偏大
-- ✅ 正确写法：分母应为 result IS NOT NULL 的有效记录
-- SELECT
--     COUNT(CASE WHEN a.`result` != 0 THEN 1 END) AS failed_count,
--     COUNT(CASE WHEN a.`result` IS NOT NULL THEN 1 END) AS valid_total,
--     COUNT(CASE WHEN a.`result` != 0 THEN 1 END) * 100.0
--         / NULLIF(COUNT(CASE WHEN a.`result` IS NOT NULL THEN 1 END), 0) AS fail_rate_pct
-- FROM spark_analytics.spark_app_metrics a
-- WHERE a.dt = '20260308';


-- ---------------------------------------------------------------------------
-- Case 1b: result 字段值含义混淆（0=成功当成失败）
-- 业务需求：统计 Spark App 的成功率
-- ❌ 错误：将 result=0 当作失败（实际 0 代表成功），
--   导致成功率和失败率完全反转
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    COUNT(CASE WHEN a.`result` = 0 THEN 1 END)                AS fail_count,
    COUNT(CASE WHEN a.`result` != 0 THEN 1 END)               AS success_count,
    COUNT(CASE WHEN a.`result` != 0 THEN 1 END) * 100.0
        / COUNT(*)                                           AS success_rate_pct
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.platform;
-- ❌ result=0 是成功，但这里把 result=0 当作 fail_count
-- ❌ result!=0 是失败/错误码，但这里把它当作 success_count
-- ✅ 正确写法：result=0 为成功，result!=0 为失败
-- SELECT
--     a.platform,
--     COUNT(CASE WHEN a.`result` = 0 THEN 1 END) AS success_count,
--     COUNT(CASE WHEN a.`result` != 0 THEN 1 END) AS fail_count,
--     COUNT(CASE WHEN a.`result` = 0 THEN 1 END) * 100.0
--         / NULLIF(COUNT(*), 0) AS success_rate_pct
-- FROM spark_analytics.spark_app_metrics a
-- WHERE a.dt = '20260308'
-- GROUP BY a.platform;


-- ---------------------------------------------------------------------------
-- Case 1c: 多表 JOIN 后分母膨胀导致比率偏低
-- 业务需求：统计每个 App 的 Job 成功率
-- ❌ 错误：先将 app 表和 job 表 JOIN，再用 app 级别的 result 计算成功率，
--   由于 1 个 app 对应 N 个 job，app.result 被重复计算 N 次，分母膨胀
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    COUNT(*)                                                 AS total_records,
    COUNT(CASE WHEN a.`result` = 0 THEN 1 END)                AS app_success_count,
    COUNT(CASE WHEN a.`result` = 0 THEN 1 END) * 100.0
        / COUNT(*)                                           AS app_success_rate
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name;
-- ❌ 1 个 app 有 N 个 job，所以 a.`result`=0 被计数 N 次
-- ❌ total_records 是 job 数量而非 app 数量，分母膨胀
-- ✅ 正确写法：用 job 表的 status 字段计算 job 成功率
-- SELECT
--     j.app_id,
--     a.app_name,
--     COUNT(*) AS total_jobs,
--     COUNT(CASE WHEN j.`status` = 0 THEN 1 END) AS success_jobs,
--     COUNT(CASE WHEN j.`status` = 0 THEN 1 END) * 100.0
--         / NULLIF(COUNT(*), 0) AS job_success_rate
-- FROM spark_analytics.spark_job_metrics j
-- INNER JOIN spark_analytics.spark_app_metrics a
--     ON j.app_id = a.app_id AND j.dt = a.dt
-- WHERE j.dt = '20260308'
-- GROUP BY j.app_id, a.app_name;


-- ---------------------------------------------------------------------------
-- Case 1d: 条件过滤不当导致分子分母不对齐
-- 业务需求：统计"运行超过 1 分钟"的 App 中的失败率
-- ❌ 错误：分子加了耗时过滤条件，但分母没有加，
--   导致分母包含所有 app（含短时 app），比率无业务意义
-- ---------------------------------------------------------------------------
SELECT
    COUNT(CASE WHEN a.`result` != 0
               AND (a.end_time - a.start_time) > 60000
               THEN 1 END)                                   AS long_running_fail,
    COUNT(*)                                                 AS total_count,
    COUNT(CASE WHEN a.`result` != 0
               AND (a.end_time - a.start_time) > 60000
               THEN 1 END) * 100.0
        / COUNT(*)                                           AS fail_rate_pct
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308';
-- ❌ 分子是"运行>1分钟 且 失败"的 app 数
-- ❌ 分母是所有 app 数（包含运行<1分钟的），分子分母不对齐
-- ✅ 正确写法：分母也应限定为"运行>1分钟"的 app
-- SELECT
--     COUNT(CASE WHEN a.`result` != 0 THEN 1 END) AS long_running_fail,
--     COUNT(*) AS long_running_total,
--     COUNT(CASE WHEN a.`result` != 0 THEN 1 END) * 100.0
--         / NULLIF(COUNT(*), 0) AS fail_rate_pct
-- FROM spark_analytics.spark_app_metrics a
-- WHERE a.dt = '20260308'
--   AND (a.end_time - a.start_time) > 60000;


-- ---------------------------------------------------------------------------
-- Case 1e: CASE WHEN 分支遗漏导致统计不完整
-- 业务需求：按状态分类统计 Job 的分布（成功/失败/killed/其他）
-- ❌ 错误：CASE WHEN 没有 ELSE 分支，status 为其他值时归为 NULL，
--   导致 SUM 统计不完整，各分类之和 != 总数
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    COUNT(*)                                                 AS total_jobs,
    SUM(CASE WHEN j.`status` = 0 THEN 1 END)                AS success_jobs,
    SUM(CASE WHEN j.`status` = -1 THEN 1 END)               AS failed_jobs,
    SUM(CASE WHEN j.`status` = -2 THEN 1 END)               AS killed_jobs,
    SUM(CASE WHEN j.`status` = 0 THEN 1 END) * 100.0
        / COUNT(*)                                           AS success_rate_pct
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
GROUP BY j.app_id;
-- ❌ CASE WHEN 没有 ELSE 0，当 status 不是 0/-1/-2 时返回 NULL
-- ❌ SUM 会忽略 NULL，导致 success_jobs + failed_jobs + killed_jobs < total_jobs
-- ❌ 成功率分子可能为 NULL（当没有 status=0 的 job 时）
-- ✅ 正确写法：加 ELSE 0 并处理 NULL
-- SELECT
--     j.app_id,
--     COUNT(*) AS total_jobs,
--     SUM(CASE WHEN j.`status` = 0 THEN 1 ELSE 0 END) AS success_jobs,
--     SUM(CASE WHEN j.`status` = -1 THEN 1 ELSE 0 END) AS failed_jobs,
--     SUM(CASE WHEN j.`status` = -2 THEN 1 ELSE 0 END) AS killed_jobs,
--     SUM(CASE WHEN j.`status` NOT IN (0, -1, -2) THEN 1 ELSE 0 END) AS other_jobs,
--     SUM(CASE WHEN j.`status` = 0 THEN 1 ELSE 0 END) * 100.0
--         / NULLIF(COUNT(*), 0) AS success_rate_pct
-- FROM spark_analytics.spark_job_metrics j
-- WHERE j.dt = '20260308'
-- GROUP BY j.app_id;
