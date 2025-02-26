-- ============================================================================
-- Case 08: 空表/空分区上的窗口函数行为
-- ============================================================================
-- 【问题描述】
--   窗口函数（ROW_NUMBER、RANK、LAG、LEAD、SUM OVER 等）在空数据集上
--   不会报错，但行为可能与预期不同：
--     1. 空分区 → 窗口函数无数据处理 → 返回0行结果
--     2. PARTITION BY 后某个分组为空 → 该分组不出现在结果中
--     3. LAG/LEAD 跨空分区取值 → 返回 NULL 或默认值
--     4. 累计求和窗口在部分分区为空时产生断层
--
-- 【易犯场景】
--   1. 计算 Top-N 排名，源数据为空时返回0行（不是返回空排名）
--   2. 使用 LAG 计算环比/同比，前一天分区为空导致环比为 NULL
--   3. 累计求和窗口期望连续时间线，但中间某天为空导致断层
--   4. DENSE_RANK 用于去重分组，空分区时整个分组消失
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - 窗口函数所操作的数据集可能为空，结果可能不完整
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 8a: ROW_NUMBER Top-N 在空分区上返回0行
-- 取每个用户当天耗时最长的 Top-3 应用
-- 如果当天分区为空，结果为0行（不是每个用户0条记录）
-- ---------------------------------------------------------------------------
SELECT
    app_id,
    app_name,
    `user`,
    `result`,
    platform,
    executor_num,
    executor_memory,
    start_time,
    end_time,
    duration_ms,
    rn
FROM (
    SELECT
        app_id,
        app_name,
        `user`,
        `result`,
        platform,
        executor_num,
        executor_memory,
        start_time,
        end_time,
        (end_time - start_time)               AS duration_ms,
        -- 空分区时无数据参与排名，整个子查询为空
        ROW_NUMBER() OVER (
            PARTITION BY `user`
            ORDER BY (end_time - start_time) DESC
        )                                      AS rn
    FROM spark_analytics.spark_app_metrics
    -- ❌ 空分区 → 窗口函数无输入 → 0行结果
    WHERE dt = '20270310'
) ranked
WHERE rn <= 3
ORDER BY `user`, rn;


-- ---------------------------------------------------------------------------
-- Case 8b: LAG 函数跨分区取前一天值，前一天为空
-- 计算每个平台的 app 数量日环比
-- 如果某天分区为空，LAG 取到的上一天值可能错位
-- ---------------------------------------------------------------------------
SELECT
    dt,
    platform,
    app_count,
    prev_day_count,
    -- ❌ 如果前一天为空（不在结果中），LAG 取的是更早一天的值
    -- 环比计算结果不是"与昨天比"而是"与前天比"
    CASE
        WHEN prev_day_count IS NOT NULL AND prev_day_count > 0
        THEN ROUND((app_count - prev_day_count) * 100.0 / prev_day_count, 2)
        ELSE NULL
    END                                        AS day_over_day_pct
FROM (
    SELECT
        dt,
        platform,
        COUNT(*)                               AS app_count,
        LAG(COUNT(*), 1) OVER (
            PARTITION BY platform
            ORDER BY dt
        )                                      AS prev_day_count
    FROM spark_analytics.spark_app_metrics
    -- ❌ 日期范围可能包含空分区日期
    -- 空分区不会出现在 GROUP BY 结果中
    -- LAG 跳过了空分区日期，取到的是非相邻天的值
    WHERE dt BETWEEN '20270304' AND '20270310'
    GROUP BY dt, platform
) daily_stats
ORDER BY platform, dt;


-- ---------------------------------------------------------------------------
-- Case 8c: 累计求和窗口在空分区间产生断层
-- 按天累计统计 stage 的 task 总数
-- 某天空分区导致累计线断层，趋势图不连续
-- ---------------------------------------------------------------------------
SELECT
    dt,
    daily_tasks,
    -- 累计求和：空分区天不存在于结果中，累计曲线跳过该天
    SUM(daily_tasks) OVER (
        ORDER BY dt
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                          AS cumulative_tasks,
    -- ❌ 移动平均：空分区导致窗口内数据点不足3天
    AVG(daily_tasks) OVER (
        ORDER BY dt
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                          AS moving_avg_3day,
    -- ❌ FIRST_VALUE/LAST_VALUE 在空窗口中的行为
    FIRST_VALUE(daily_tasks) OVER (
        ORDER BY dt
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                          AS first_of_3day,
    LAST_VALUE(daily_tasks) OVER (
        ORDER BY dt
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                          AS last_of_3day
FROM (
    SELECT
        dt,
        SUM(num_tasks)                         AS daily_tasks
    FROM spark_analytics.spark_stage_metrics
    -- ❌ 范围包含可能的空分区
    WHERE dt BETWEEN '20270304' AND '20270310'
    GROUP BY dt
) daily_stage_stats
ORDER BY dt;
