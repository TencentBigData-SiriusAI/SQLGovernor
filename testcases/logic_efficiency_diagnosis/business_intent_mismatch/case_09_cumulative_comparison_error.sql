-- ============================================================================
-- Case 09: 累计/环比计算错误
-- ============================================================================
-- 【问题描述】
--   窗口函数在累计和环比计算中的语义错误，常见包括：
--     1. ROWS 和 RANGE 边界差异：ROWS 按行数、RANGE 按值范围，效果不同
--     2. LAG/LEAD 方向搞反：LAG 取前面的行，LEAD 取后面的行
--     3. 累计求和窗口未指定 ORDER BY，导致结果不确定
--     4. 环比分母使用当期而非上期
--     5. 窗口函数缺少 PARTITION BY 变成全局计算
--
-- 【易犯场景】
--   1. 想做累计求和但用了 RANGE，相同日期的行被合并计算
--   2. 想比较"今天 vs 昨天"，但 LAG/LEAD 方向搞反
--   3. 计算环比增长率时，分母用了当期而非上期
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - 窗口函数的边界定义可能与累计/对比计算的语义不一致
--   - LAG/LEAD 方向可能与业务"与前期比较"的需求相反
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 9a: 窗口函数 ROWS vs RANGE 边界差异
-- 业务需求：计算每个 App 按日期的累计 Job 数（逐行递增）
-- ❌ 错误：使用 RANGE 而非 ROWS，当两天的 job 数相同时，
--   RANGE 会把这些行合并在同一个窗口中，导致累计值跳跃
-- ---------------------------------------------------------------------------
SELECT
    app_id,
    imp_date,
    daily_job_count,
    SUM(daily_job_count) OVER (
        PARTITION BY app_id
        ORDER BY imp_date
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                        AS cumulative_jobs
FROM (
    SELECT
        j.app_id,
        j.dt                                    AS imp_date,
        COUNT(*)                                             AS daily_job_count
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt BETWEEN '20260301' AND '20260308'
    GROUP BY j.app_id, j.dt
) daily_stats;
-- ❌ RANGE 模式下，ORDER BY 值相同的行会被合并到同一窗口
-- ❌ 如果连续两天 job 数恰好相同，累计值会出现"跳跃"而非逐行递增
-- ✅ 正确写法：使用 ROWS 确保逐行累加
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW


-- ---------------------------------------------------------------------------
-- Case 9b: LAG/LEAD 方向搞反
-- 业务需求：计算每天 App 数量与"前一天"的对比（环比）
-- ❌ 错误：使用 LEAD（取后一天的值）而非 LAG（取前一天的值），
--   导致比较方向反了——和"后一天"比而非"前一天"比
-- ---------------------------------------------------------------------------
SELECT
    imp_date,
    app_count,
    LEAD(app_count, 1) OVER (ORDER BY imp_date)              AS prev_day_count,
    app_count - LEAD(app_count, 1) OVER (ORDER BY imp_date)  AS day_over_day_change
FROM (
    SELECT
        a.dt                                    AS imp_date,
        COUNT(DISTINCT a.app_id)                             AS app_count
    FROM spark_analytics.spark_app_metrics a
    WHERE a.dt BETWEEN '20260301' AND '20260308'
    GROUP BY a.dt
) daily_stats;
-- ❌ 别名写 prev_day_count，但 LEAD 取的是后一天的值
-- ❌ day_over_day_change = 当天 - 后一天，语义完全反了
-- ✅ 正确写法：使用 LAG 取前一天
-- LAG(app_count, 1) OVER (ORDER BY imp_date) AS prev_day_count


-- ---------------------------------------------------------------------------
-- Case 9c: 累计求和窗口未指定 ORDER BY 导致结果不确定
-- 业务需求：按日期累计统计 App 的输入数据量
-- ❌ 错误：SUM() OVER 中没有 ORDER BY，窗口范围不确定，
--   结果可能是全部行的总和而非逐行累加
-- ---------------------------------------------------------------------------
SELECT
    a.dt,
    a.app_id,
    (a.end_time - a.start_time)                              AS duration_ms,
    SUM(a.end_time - a.start_time) OVER (
        PARTITION BY a.app_id
    )                                                        AS cumulative_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt BETWEEN '20260301' AND '20260308';
-- ❌ 没有 ORDER BY，SUM OVER 变成整个分区的总和
-- ❌ 每行的 cumulative_duration 都相同（= 该 app 所有天的总耗时）
-- ❌ 不是逐日递增的累计值
-- ✅ 正确写法：加 ORDER BY
-- SUM(a.end_time - a.start_time) OVER (
--     PARTITION BY a.app_id
--     ORDER BY a.dt
--     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- ) AS cumulative_duration


-- ---------------------------------------------------------------------------
-- Case 9d: 环比分母使用当期而非上期
-- 业务需求：计算每天 Job 数量的"环比增长率"（(今天-昨天)/昨天）
-- ❌ 错误：分母用了当天的 job_count 而非前一天的，
--   得到的不是环比增长率而是另一个含义的比率
-- ---------------------------------------------------------------------------
SELECT
    imp_date,
    job_count,
    LAG(job_count, 1) OVER (ORDER BY imp_date)               AS prev_day_count,
    (job_count - LAG(job_count, 1) OVER (ORDER BY imp_date))
        * 100.0 / job_count                                  AS growth_rate_pct
FROM (
    SELECT
        j.dt                                    AS imp_date,
        COUNT(*)                                             AS job_count
    FROM spark_analytics.spark_job_metrics j
    WHERE j.dt BETWEEN '20260301' AND '20260308'
    GROUP BY j.dt
) daily_stats;
-- ❌ 分母用了 job_count（当天），应该用 prev_day_count（前一天）
-- ❌ 环比增长率 = (今天-昨天)/昨天，分母应为昨天的值
-- ✅ 正确写法：分母用 LAG 取前一天的值
-- (job_count - LAG(job_count, 1) OVER (ORDER BY imp_date)) * 100.0
--     / NULLIF(LAG(job_count, 1) OVER (ORDER BY imp_date), 0) AS growth_rate_pct


-- ---------------------------------------------------------------------------
-- Case 9e: 窗口函数缺少 PARTITION BY 变成全局计算
-- 业务需求：在每个 App 内部按日期排名
-- ❌ 错误：ROW_NUMBER 没有 PARTITION BY，变成全局排名，
--   所有 App 的所有天混在一起排名
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.dt,
    (a.end_time - a.start_time)                              AS duration_ms,
    ROW_NUMBER() OVER (
        ORDER BY a.dt
    )                                                        AS date_rank_in_app
FROM spark_analytics.spark_app_metrics a
WHERE a.dt BETWEEN '20260301' AND '20260308';
-- ❌ 别名写 date_rank_in_app，暗示是 app 内部排名
-- ❌ 但没有 PARTITION BY a.app_id，排名是全局的
-- ❌ 同一 app 在不同天的排名不连续，混入了其他 app 的行
-- ✅ 正确写法：加 PARTITION BY app_id
-- ROW_NUMBER() OVER (PARTITION BY a.app_id ORDER BY a.dt) AS date_rank_in_app
