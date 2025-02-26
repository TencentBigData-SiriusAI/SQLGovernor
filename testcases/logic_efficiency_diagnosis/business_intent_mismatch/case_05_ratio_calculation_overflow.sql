-- ============================================================================
-- Case 05: 比率/百分比计算溢出
-- ============================================================================
-- 【问题描述】
--   比率和百分比计算中的数值问题，常见错误包括：
--     1. 整数除法截断为 0：两个 BIGINT 相除在 Hive 中返回整数，小数部分被截断
--     2. 分母可能为零但未做保护，导致运行时报错或返回 NULL
--     3. 百分比计算时乘 100 的位置错误，导致精度丢失或结果失真
--     4. ROUND 精度丢失导致统计偏差
--     5. 多层嵌套比率计算中精度累积丢失
--
-- 【易犯场景】
--   1. 计算 input_size / total_size 比率时，两个 BIGINT 相除结果为 0
--   2. 某些 App 没有 stage，分母为 0 导致除零错误
--   3. 先做整数除法再乘 100，精度已经丢失
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - 整数除法可能导致结果为 0，建议转换为浮点数
--   - 分母可能为 0，建议添加 NULLIF 或 CASE WHEN 保护
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 5a: 整数除法截断为 0
-- 业务需求：计算每个 Stage 的 Shuffle 输出占总输出的比率
-- ❌ 错误：两个 BIGINT 相除，结果为整数，小数部分被截断为 0
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.shuffle_output_size,
    s.output_size,
    s.shuffle_output_size / s.output_size                    AS shuffle_ratio
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.output_size > 0
LIMIT 100;
-- ❌ shuffle_output_size 和 output_size 都是 BIGINT
-- ❌ BIGINT / BIGINT 在 Hive 中结果仍为 BIGINT，小数部分被截断
-- ❌ 当 shuffle_output_size < output_size 时，结果恒为 0
-- ✅ 正确写法：转换为 DOUBLE 后相除
-- CAST(s.shuffle_output_size AS DOUBLE) / s.output_size AS shuffle_ratio


-- ---------------------------------------------------------------------------
-- Case 5b: 分母可能为零未做保护
-- 业务需求：计算每个 App 的平均每 Stage 输入数据量
-- ❌ 错误：stage_event_num 可能为 0 或 NULL，直接做分母会报错或返回 NULL
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.stage_event_num,
    CAST(1024 * 1024 * 100 AS BIGINT)
        / a.stage_event_num                                  AS avg_input_per_stage
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
LIMIT 100;
-- ❌ stage_event_num 可能为 0，导致除零错误
-- ❌ stage_event_num 可能为 NULL，结果也为 NULL
-- ✅ 正确写法：用 NULLIF 保护分母
-- CAST(1024 * 1024 * 100 AS DOUBLE) / NULLIF(a.stage_event_num, 0) AS avg_input_per_stage


-- ---------------------------------------------------------------------------
-- Case 5c: 百分比计算时乘 100 的位置错误
-- 业务需求：计算 Task 的 GC 时间占运行时间的百分比
-- ❌ 错误：先做整数除法（结果已截断为 0），再乘 100，得到 0
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.gc_time,
    t.task_run_time,
    (t.gc_time / t.task_run_time) * 100                      AS gc_pct
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.task_run_time > 0
LIMIT 100;
-- ❌ gc_time / task_run_time 是整数除法，结果为 0
-- ❌ 0 * 100 = 0，百分比始终为 0
-- ✅ 正确写法：先乘 100.0（浮点），再除
-- t.gc_time * 100.0 / t.task_run_time AS gc_pct
-- 或：CAST(t.gc_time AS DOUBLE) / t.task_run_time * 100


-- ---------------------------------------------------------------------------
-- Case 5d: ROUND 精度丢失导致统计偏差
-- 业务需求：统计各平台 App 的成功率（保留 2 位小数）
-- ❌ 错误：先 ROUND 到 2 位再求 AVG，大量 0.00 和 1.00 的值拉偏了平均值
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    AVG(
        ROUND(
            CAST(CASE WHEN a.`result` = 0 THEN 1 ELSE 0 END AS DOUBLE),
            2
        )
    )                                                        AS avg_success_rate
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.platform;
-- ❌ 每行的值 ROUND 后只有 0.00 或 1.00，AVG 结果精度受限
-- ❌ 应该先 AVG 再 ROUND，而不是先 ROUND 再 AVG
-- ✅ 正确写法：先聚合再 ROUND
-- ROUND(
--     AVG(CAST(CASE WHEN a.`result` = 0 THEN 1 ELSE 0 END AS DOUBLE)),
--     4
-- ) AS avg_success_rate


-- ---------------------------------------------------------------------------
-- Case 5e: 多层嵌套比率计算中精度累积丢失
-- 业务需求：先计算每个 App 的 Stage 失败率，再统计所有 App 的平均失败率
-- ❌ 错误：内层整数除法截断为 0/1，外层 AVG 只能得到 0 或 1 的平均值
-- ---------------------------------------------------------------------------
SELECT
    AVG(stage_fail_rate)                                     AS overall_fail_rate
FROM (
    SELECT
        s.app_id,
        SUM(CASE WHEN s.`status` != 0 THEN 1 ELSE 0 END)
            / COUNT(*)                                       AS stage_fail_rate
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.dt = '20260308'
    GROUP BY s.app_id
) app_rates;
-- ❌ 内层 SUM/COUNT 都是整数，除法结果为 0 或 1（截断）
-- ❌ 外层 AVG 只能在 {0, 1} 中取平均，无法得到真实的失败率
-- ✅ 正确写法：内层转浮点
-- SUM(CASE WHEN s.`status` != 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS stage_fail_rate
