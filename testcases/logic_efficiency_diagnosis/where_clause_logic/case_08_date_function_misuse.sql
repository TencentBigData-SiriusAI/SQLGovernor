-- ============================================================================
-- Case 08: 日期/时间函数在 WHERE 中的误用
-- ============================================================================
-- 【问题描述】
--   数仓中时间相关的 WHERE 条件是最常见的过滤场景，但容易出错：
--     1. 对分区字段使用函数导致分区裁剪失效
--     2. 时间戳的毫秒/秒单位混淆
--     3. DATE_FORMAT / FROM_UNIXTIME 格式字符串写错
--     4. 时区问题导致时间范围偏移
--     5. 日期加减运算溢出或类型不匹配
--
-- 【易犯场景】
--   1. WHERE DATE_FORMAT(dt, 'yyyyMMdd') = '20260308'（分区裁剪失效）
--   2. WHERE FROM_UNIXTIME(start_time) > '2026-03-08'（毫秒/秒混淆）
--   3. 日期字符串格式不一致（yyyyMMdd vs yyyy-MM-dd）
--   4. DATEDIFF 参数顺序写反
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - 分区字段上使用函数，分区裁剪失效
--   - 时间戳单位可能不一致
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 8a: 对分区字段使用函数导致分区裁剪失效
-- ❌ 错误：SUBSTR 包裹分区字段，引擎无法直接裁剪分区
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result
FROM spark_analytics.spark_app_metrics a
WHERE SUBSTR(a.dt, 1, 6) = '202603'            -- ❌ 函数包裹分区字段，裁剪失效
  AND a.result != 0
ORDER BY a.app_id
LIMIT 100;

-- ✅ 正确写法：直接用分区字段比较
-- WHERE a.dt >= '20260301' AND a.dt <= '20260331'


-- ---------------------------------------------------------------------------
-- Case 8b: FROM_UNIXTIME 毫秒/秒混淆
-- start_time 是毫秒级时间戳，但 FROM_UNIXTIME 接受的是秒
-- ❌ 错误：直接传入毫秒时间戳，转出的时间是 2055 年
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.start_time,
    FROM_UNIXTIME(j.start_time, 'yyyy-MM-dd HH:mm:ss')     AS start_time_str  -- ❌ 毫秒被当秒
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND FROM_UNIXTIME(j.start_time, 'yyyy-MM-dd') = '2026-03-08'  -- ❌ 毫秒当秒，永远不等
ORDER BY j.start_time;

-- ✅ 正确写法：除以 1000 转为秒
-- FROM_UNIXTIME(j.start_time / 1000, 'yyyy-MM-dd') = '2026-03-08'


-- ---------------------------------------------------------------------------
-- Case 8c: 日期格式字符串写错
-- 业务需求：查找某一小时内的 stage
-- ❌ 错误：格式字符串用了 Java 风格的 HH 但平台可能不支持
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.start_time,
    FROM_UNIXTIME(s.start_time / 1000, 'yyyy-MM-dd HH')    AS start_hour
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND FROM_UNIXTIME(s.start_time / 1000, 'YYYY-MM-DD HH')  -- ❌ YYYY 是 week-year 不是 year
      = '2026-03-08 14'                                     -- 在某些引擎中 YYYY 和 yyyy 含义不同
ORDER BY s.start_time;

-- ✅ 正确写法：使用小写 yyyy-MM-dd
-- FROM_UNIXTIME(s.start_time / 1000, 'yyyy-MM-dd HH') = '2026-03-08 14'


-- ---------------------------------------------------------------------------
-- Case 8d: DATEDIFF 参数顺序写反导致正负号错误
-- 业务需求：查找运行超过 2 天的 app
-- ❌ 错误：DATEDIFF(start, end) 返回负值，条件 > 2 永远不满足
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    FROM_UNIXTIME(a.start_time / 1000, 'yyyy-MM-dd')       AS start_date,
    FROM_UNIXTIME(a.end_time / 1000, 'yyyy-MM-dd')         AS end_date,
    DATEDIFF(
        FROM_UNIXTIME(a.start_time / 1000, 'yyyy-MM-dd'),  -- ❌ start 在前
        FROM_UNIXTIME(a.end_time / 1000, 'yyyy-MM-dd')     -- ❌ end 在后
    )                                                       AS run_days
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND DATEDIFF(
        FROM_UNIXTIME(a.start_time / 1000, 'yyyy-MM-dd'),
        FROM_UNIXTIME(a.end_time / 1000, 'yyyy-MM-dd')
      ) > 2                                                 -- ❌ start - end 为负，永远 < 0
ORDER BY run_days DESC;

-- ✅ 正确写法：end 在前，start 在后
-- DATEDIFF(FROM_UNIXTIME(a.end_time/1000, 'yyyy-MM-dd'),
--          FROM_UNIXTIME(a.start_time/1000, 'yyyy-MM-dd')) > 2


-- ---------------------------------------------------------------------------
-- Case 8e: 日期加减混淆导致时间范围错误
-- 业务需求：查找"昨天"的数据（当前分区是 20260308）
-- ❌ 错误：字符串直接减 1 不是日期运算，'20260308' - 1 = 20260307 是数值运算
--   如果分区是 '20260301'，减 1 得到 20260300 而非 20260228
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.task_run_time,
    t.gc_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308' - 1                    -- ❌ 字符串减数值，隐式转换
                                                            -- 得到 20260307（碰巧对），但月初会出错
  AND t.status = 'FAILED'
ORDER BY t.task_run_time DESC;

-- ✅ 正确写法：使用日期函数
-- WHERE t.dt = DATE_FORMAT(DATE_SUB('2026-03-08', 1), 'yyyyMMdd')
-- 或直接写死：WHERE t.dt = '20260307'
