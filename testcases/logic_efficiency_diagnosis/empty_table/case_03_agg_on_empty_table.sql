-- ============================================================================
-- Case 03: 空表上做聚合运算导致指标异常
-- ============================================================================
-- 【问题描述】
--   当表或分区为空时，聚合函数的行为各不相同：
--     - COUNT(*) 返回 0（不是 NULL）
--     - SUM/AVG/MAX/MIN 返回 NULL（不是 0）
--     - COUNT(col) 返回 0
--   这些差异在空表场景下容易导致指标异常：
--     1. 成功率计算: SUM(success)/COUNT(*) → NULL/0 → 除零错误或 NULL
--     2. 平均值: AVG(duration) → NULL，下游误判为"无延迟"
--     3. 同比/环比: 当天 NULL vs 昨天有值 → 计算变化率失败
--     4. 空表 COUNT=0 与 SUM=NULL 混用导致逻辑分支错误
--
-- 【易犯场景】
--   1. 计算成功率/失败率时分母为0（空表 COUNT=0）
--   2. 计算环比时当天分区为空，NULL 参与运算传播
--   3. 用 COALESCE(AVG(x), 0) 掩盖了数据缺失问题
--   4. 多指标计算中部分为 NULL 部分为0，下游难以区分
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - 表/分区可能为空，聚合结果为 NULL/0，需添加空数据检查
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 3a: 空分区上的成功率/失败率计算
-- 当分区为空时，COUNT=0，SUM=NULL，成功率计算会出现除零或 NULL
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    COUNT(*)                                      AS total_apps,
    -- 空分区时 COUNT=0，以下计算不会报错但结果无意义
    SUM(CASE WHEN a.`result` = 0 THEN 1 ELSE 0 END)
                                                  AS success_count,
    SUM(CASE WHEN a.`result` != 0 THEN 1 ELSE 0 END)
                                                  AS fail_count,
    -- ❌ 空分区: SUM=NULL, COUNT=0 → NULL/0 或 0/0 → 异常
    ROUND(
        SUM(CASE WHEN a.`result` = 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*),
        2
    )                                              AS success_rate_pct,
    -- ❌ AVG 空分区返回 NULL，下游可能解读为"平均耗时=0"
    AVG(a.end_time - a.start_time)                AS avg_duration_ms,
    -- ❌ MAX/MIN 空分区返回 NULL
    MAX(a.end_time - a.start_time)                AS max_duration_ms,
    MIN(a.end_time - a.start_time)                AS min_duration_ms,
    -- ❌ 空分区 SUM=NULL，不是0
    SUM(a.executor_num)                           AS total_executors
FROM spark_analytics.spark_app_metrics a
-- ❌ 未来分区为空
WHERE a.dt = '20270310'
GROUP BY a.platform;
-- ❌ 注意：如果整个分区为空，GROUP BY 无分组行，结果是0行（不是NULL行）


-- ---------------------------------------------------------------------------
-- Case 3b: 不带 GROUP BY 的全局聚合在空表上
-- 无 GROUP BY 时，空表上 SELECT COUNT 返回1行(值为0)
-- 而 SELECT SUM/AVG 返回1行(值为NULL)
-- 这种不一致极易引发 bug
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)                                      AS total_apps,      -- 返回 0（不是 NULL）
    COUNT(a.app_id)                               AS app_count,       -- 返回 0
    SUM(a.executor_num)                           AS sum_executors,   -- ❌ 返回 NULL（不是 0）
    AVG(a.executor_memory)                        AS avg_memory,      -- ❌ 返回 NULL
    MAX(a.end_time - a.start_time)                AS max_duration,    -- ❌ 返回 NULL
    MIN(a.start_time)                             AS min_start_time,  -- ❌ 返回 NULL
    -- ❌ COUNT=0 时，除法计算返回 NULL 或报错
    SUM(a.executor_num) / COUNT(*)                AS avg_exec_wrong,
    -- ❌ COALESCE 掩盖了空表问题，下游认为"平均值=0"但实际是没有数据
    COALESCE(AVG(a.executor_num), 0)              AS avg_exec_masked,
    -- ❌ 空表上的条件聚合：CASE WHEN 不执行，SUM/COUNT 行为不一致
    SUM(CASE WHEN a.`result` = 0 THEN 1 ELSE 0 END)  AS success_sum,
    COUNT(CASE WHEN a.`result` = 0 THEN 1 END)        AS success_cnt
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20270310';


-- ---------------------------------------------------------------------------
-- Case 3c: 空表聚合结果参与后续计算 —— NULL 传播
-- 子查询从空分区做聚合得到 NULL，外层基于 NULL 做后续运算
-- NULL 参与任何运算都返回 NULL，层层传播
-- ---------------------------------------------------------------------------
SELECT
    today_stats.platform,
    today_stats.total_apps                        AS today_apps,
    today_stats.avg_duration                      AS today_avg_dur,
    today_stats.success_rate                      AS today_success_rate,
    -- ❌ today 指标为 NULL（来自空分区），任何运算结果都是 NULL
    today_stats.total_apps - 100                  AS apps_diff,
    -- ❌ NULL 参与比较，结果为 NULL（不是 TRUE 也不是 FALSE）
    CASE
        WHEN today_stats.total_apps > 0 THEN '有数据'
        WHEN today_stats.total_apps = 0 THEN '零条记录'
        ELSE '空值/NULL'                           -- 空分区 GROUP BY 返回0行时不走此分支
    END                                            AS data_status,
    -- ❌ NULL 参与除法
    ROUND(today_stats.avg_duration / 1000.0, 2)   AS avg_duration_sec,
    -- ❌ NULL + 数值 = NULL
    today_stats.success_rate + 0.0                AS success_rate_num
FROM (
    SELECT
        platform,
        COUNT(*)                                  AS total_apps,
        AVG(end_time - start_time)                AS avg_duration,
        ROUND(
            SUM(CASE WHEN `result` = 0 THEN 1 ELSE 0 END) * 100.0
            / GREATEST(COUNT(*), 1),
            2
        )                                          AS success_rate
    FROM spark_analytics.spark_app_metrics
    -- ❌ 空分区
    WHERE dt = '20270310'
    GROUP BY platform
) today_stats;
