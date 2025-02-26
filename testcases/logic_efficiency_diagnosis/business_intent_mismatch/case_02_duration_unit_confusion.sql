-- ============================================================================
-- Case 02: 耗时计算单位混淆
-- ============================================================================
-- 【问题描述】
--   时间相关计算中，单位混淆是最常见的业务意图不匹配场景：
--     1. start_time/end_time 为毫秒时间戳，相减后直接当秒用
--     2. start_time - end_time 顺序搞反，得到负值
--     3. 不同表的时间字段精度不一致，混合计算导致数量级错误
--     4. 毫秒相减后当分钟用，差 1000 × 60 倍
--     5. AVG 和 SUM 语义混淆——想要"平均耗时"却用了 SUM
--
-- 【易犯场景】
--   1. 对 Spark App 的运行时长做统计，忘记除以 1000 转秒
--   2. 计算 Stage 耗时时 start/end 顺序写反
--   3. Task 表的 taskreadtime 是纳秒，与其他表的毫秒混算
--   4. 想看"总耗时"却用了 AVG，想看"平均耗时"却用了 SUM
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - 时间计算结果的单位可能与展示/比较的单位不一致
--   - 建议确认 start_time/end_time 的单位（毫秒）并做正确转换
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 2a: 毫秒时间戳相减直接当秒用
-- 业务需求：查找运行超过 1 小时的 Spark App
-- ❌ 错误：end_time - start_time 结果是毫秒，直接和 3600（秒）比较
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    (a.end_time - a.start_time)                              AS duration,
    a.`result`
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND (a.end_time - a.start_time) > 3600
ORDER BY duration DESC
LIMIT 50;
-- ❌ end_time - start_time 的单位是毫秒，3600 毫秒 = 3.6 秒
-- ❌ 实际过滤的是"运行超过 3.6 秒"的 app，而非"超过 1 小时"
-- ✅ 正确写法：与 3600 * 1000 = 3600000 毫秒比较
-- AND (a.end_time - a.start_time) > 3600000


-- ---------------------------------------------------------------------------
-- Case 2b: start_time - end_time 顺序搞反为负值
-- 业务需求：统计每个 Stage 的运行耗时
-- ❌ 错误：start_time - end_time 得到负值，但没有做绝对值处理，
--   排序和聚合结果完全反转
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.stage_name,
    (s.start_time - s.end_time)                              AS stage_duration_ms,
    s.task_num
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.`status` = 0
ORDER BY stage_duration_ms DESC
LIMIT 100;
-- ❌ start_time - end_time 得到负值（因为 start < end）
-- ❌ ORDER BY DESC 排最大负值在前，实际是耗时最短的 stage
-- ✅ 正确写法：end_time - start_time
-- SELECT s.app_id, s.stage_id, s.stage_name,
--     (s.end_time - s.start_time) AS stage_duration_ms, s.task_num
-- FROM spark_analytics.spark_stage_metrics s
-- WHERE s.dt = '20260308' AND s.`status` = 0
-- ORDER BY stage_duration_ms DESC
-- LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 2c: 不同表的时间戳精度不一致混合计算
-- 业务需求：计算每个 Task 的"读取耗时占总耗时"的比例
-- ❌ 错误：taskreadtime 是纳秒，end_time - start_time 是毫秒，
--   直接相除导致比例差 1000000 倍
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    CAST(t.taskreadtime AS BIGINT)                           AS read_time,
    (t.end_time - t.start_time)                              AS total_time,
    CAST(t.taskreadtime AS BIGINT) * 100.0
        / (t.end_time - t.start_time)                        AS read_pct
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND (t.end_time - t.start_time) > 0
LIMIT 100;
-- ❌ taskreadtime 单位是纳秒，total_time 单位是毫秒
-- ❌ read_pct 结果会是正常值的 1000000 倍（比如 500000% 而非 0.5%）
-- ✅ 正确写法：将纳秒转换为毫秒后再计算
-- CAST(t.taskreadtime AS BIGINT) / 1000000 * 100.0 / (t.end_time - t.start_time)


-- ---------------------------------------------------------------------------
-- Case 2d: 用 end_time - start_time 当分钟但实际是毫秒
-- 业务需求：统计各平台 App 的平均运行时间（分钟）
-- ❌ 错误：end_time - start_time 是毫秒，注释和别名写"分钟"，
--   但没有做 /1000/60 的转换
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    COUNT(*)                                                 AS app_count,
    AVG(a.end_time - a.start_time)                           AS avg_duration_minutes,
    MAX(a.end_time - a.start_time)                           AS max_duration_minutes
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.`result` IS NOT NULL
GROUP BY a.platform;
-- ❌ 别名写 avg_duration_minutes 但实际值是毫秒
-- ❌ 如果真实平均耗时是 5 分钟，这里显示 300000（毫秒）
-- ✅ 正确写法：除以 60000 转换为分钟
-- AVG(a.end_time - a.start_time) / 60000.0 AS avg_duration_minutes,
-- MAX(a.end_time - a.start_time) / 60000.0 AS max_duration_minutes


-- ---------------------------------------------------------------------------
-- Case 2e: 聚合耗时时 AVG 和 SUM 语义混淆
-- 业务需求：统计每个 App 下所有 Stage 的"平均耗时"
-- ❌ 错误：使用 SUM 而非 AVG，得到的是总耗时而非平均耗时
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    COUNT(*)                                                 AS stage_count,
    SUM(s.end_time - s.start_time) / 1000                    AS avg_stage_duration_sec
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.`status` = 0
GROUP BY s.app_id
ORDER BY avg_stage_duration_sec DESC
LIMIT 50;
-- ❌ 别名写 avg_stage_duration_sec 但实际用了 SUM
-- ❌ stage 越多的 app 排名越靠前，但并不代表单个 stage 耗时长
-- ✅ 正确写法：使用 AVG 而非 SUM
-- AVG(s.end_time - s.start_time) / 1000 AS avg_stage_duration_sec
