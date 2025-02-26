-- ============================================================================
-- Case 03: 去重统计遗漏
-- ============================================================================
-- 【问题描述】
--   在统计"不同的 XXX 数量"时，遗漏 DISTINCT 是最常见的业务意图不匹配：
--     1. COUNT(app_id) 未加 DISTINCT，统计的是"行数"而非"不同 app 数"
--     2. DISTINCT 放在非关键列上，去重效果与业务预期不符
--     3. GROUP BY + COUNT 场景下遗漏 DISTINCT，多表 JOIN 后同一值出现多次
--     4. 子查询已去重但外层又重复计数
--     5. 多列组合去重遗漏，只去重了部分字段
--
-- 【易犯场景】
--   1. 统计"有多少个不同的 App"时，app 表和 job 表 JOIN 后 app_id 重复
--   2. 想统计"不同用户数"但 DISTINCT 加在了 app_id 上
--   3. 分组统计中，子查询做了去重但外层 COUNT(*) 又多算了
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - COUNT(column) 可能需要 DISTINCT 来统计不重复值
--   - 建议确认业务语义是"行数"还是"不重复值数"
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 3a: COUNT(app_id) 未加 DISTINCT
-- 业务需求：统计 20260308 当天有多少个不同的 Spark App 提交了 Job
-- ❌ 错误：COUNT(app_id) 统计的是 job 表的行数（每个 job 一行），
--   而非不同的 app 数量
-- ---------------------------------------------------------------------------
SELECT
    COUNT(j.app_id)                                          AS app_count
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308';
-- ❌ 一个 app 有 N 个 job，COUNT(app_id) = job 行数，不是 app 数
-- ❌ 如果有 100 个 app 各提交了 10 个 job，结果是 1000 而非 100
-- ✅ 正确写法：加 DISTINCT
-- SELECT COUNT(DISTINCT j.app_id) AS app_count
-- FROM spark_analytics.spark_job_metrics j
-- WHERE j.dt = '20260308';


-- ---------------------------------------------------------------------------
-- Case 3b: DISTINCT 放在非关键列上
-- 业务需求：统计每个平台有多少个不同的提交用户
-- ❌ 错误：DISTINCT 放在 app_id 上，统计的是不同 app 数而非不同用户数
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    COUNT(DISTINCT a.app_id)                                 AS user_count
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.platform;
-- ❌ 别名写 user_count，但 DISTINCT 的是 app_id
-- ❌ 一个用户提交多个 app，这里统计的是 app 数而非用户数
-- ✅ 正确写法：DISTINCT 放在 user 字段上
-- SELECT a.platform, COUNT(DISTINCT a.`user`) AS user_count
-- FROM spark_analytics.spark_app_metrics a
-- WHERE a.dt = '20260308'
-- GROUP BY a.platform;


-- ---------------------------------------------------------------------------
-- Case 3c: GROUP BY + COUNT 场景下遗漏 DISTINCT
-- 业务需求：按 Stage 的状态统计涉及的不同 App 数量
-- ❌ 错误：GROUP BY status 后 COUNT(app_id) 不加 DISTINCT，
--   一个 app 有多个 stage 时 app_id 被重复计数
-- ---------------------------------------------------------------------------
SELECT
    s.`status`,
    COUNT(s.app_id)                                          AS app_count,
    COUNT(s.stage_id)                                        AS stage_count
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
GROUP BY s.`status`;
-- ❌ 一个 app 有多个 stage，COUNT(app_id) 实际是 stage 行数
-- ❌ app_count 和 stage_count 值几乎一样（都是 stage 行数）
-- ✅ 正确写法：app_count 应加 DISTINCT
-- SELECT s.`status`,
--     COUNT(DISTINCT s.app_id) AS app_count,
--     COUNT(s.stage_id) AS stage_count
-- FROM spark_analytics.spark_stage_metrics s
-- WHERE s.dt = '20260308'
-- GROUP BY s.`status`;


-- ---------------------------------------------------------------------------
-- Case 3d: 子查询去重但外层又重复计数
-- 业务需求：统计有失败 Job 的不同 App 数量
-- ❌ 错误：子查询用 DISTINCT 去重了 app_id，但外层又 JOIN 了 app 表，
--   由于 app 表可能有多行（如多个 attempt），导致重复计数
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)                                                 AS failed_app_count
FROM (
    SELECT DISTINCT j.app_id
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt = '20260308'
      AND j.`status` = -1
) failed_apps
INNER JOIN spark_analytics.spark_app_metrics a
    ON failed_apps.app_id = a.app_id
    AND a.dt = '20260308';
-- ❌ 子查询已去重 app_id，但 JOIN app 表后如果 app 表有多行则 COUNT(*) 膨胀
-- ✅ 正确写法：直接用子查询的结果 COUNT，不再 JOIN
-- SELECT COUNT(*) AS failed_app_count
-- FROM (
--     SELECT DISTINCT j.app_id
--     FROM spark_analytics.spark_job_metrics j
--     WHERE j.dt = '20260308'
--       AND j.`status` = -1
-- ) failed_apps;


-- ---------------------------------------------------------------------------
-- Case 3e: 多列组合去重遗漏
-- 业务需求：统计不同的 (app_id, stage_id) 组合数量
-- ❌ 错误：只对 app_id 做了 DISTINCT，没有按组合去重，
--   不同 app 下的同 stage_id 被合并了
-- ---------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT s.app_id)                                 AS unique_app_stage_count
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308';
-- ❌ 只去重了 app_id，没有考虑 stage_id 的组合
-- ❌ 别名写 unique_app_stage_count，但实际只是不同 app 数量
-- ✅ 正确写法：用子查询或 concat 做组合去重
-- SELECT COUNT(*) AS unique_app_stage_count
-- FROM (
--     SELECT DISTINCT s.app_id, s.stage_id
--     FROM spark_analytics.spark_stage_metrics s
--     WHERE s.dt = '20260308'
-- ) t;
