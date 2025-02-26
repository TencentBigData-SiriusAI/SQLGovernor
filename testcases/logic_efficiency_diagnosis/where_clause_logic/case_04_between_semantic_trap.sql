-- ============================================================================
-- Case 04: BETWEEN 语义陷阱
-- ============================================================================
-- 【问题描述】
--   BETWEEN 等价于 >= AND <=（左闭右闭），常见陷阱：
--     1. 开发者误以为右端不包含（左闭右开）
--     2. 字符串 BETWEEN 按字典序比较，不是数值比较
--     3. 时间戳 BETWEEN 包含右端，可能多算一秒/一天的数据
--     4. BETWEEN 与 NOT BETWEEN 的边界行为不一致
--     5. NULL 值参与 BETWEEN 的结果为 NULL（被 WHERE 过滤）
--
-- 【易犯场景】
--   1. 分区日期 BETWEEN '20260301' AND '20260308' 实际含 8 天
--   2. 毫秒时间戳 BETWEEN 包含了右端那一毫秒
--   3. 数值型字段的 BETWEEN 边界值被多算或少算
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - BETWEEN 包含两端值，可能导致多算或边界不精确
--   - 建议确认 BETWEEN 的边界语义
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 4a: BETWEEN 包含两端，多算一天分区数据
-- 业务需求：统计 3月1日到3月7日（7天）的数据
-- ❌ 错误：BETWEEN 含两端，如果写成 0301 到 0307 是对的，
--   但开发者习惯"起始+天数"思维，写成了 0301 到 0308
-- ---------------------------------------------------------------------------
SELECT
    a.dt,
    a.platform,
    COUNT(*)                                                AS app_count,
    SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END)          AS success_count,
    AVG(a.end_time - a.start_time)                          AS avg_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt BETWEEN '20260301' AND '20260308'   -- ❌ 实际是 8 天而非 7 天
GROUP BY a.dt, a.platform
ORDER BY a.dt;

-- ✅ 正确写法：明确 7 天范围
-- WHERE a.dt BETWEEN '20260301' AND '20260307'
-- 或用半开区间：WHERE a.dt >= '20260301' AND a.dt < '20260308'


-- ---------------------------------------------------------------------------
-- Case 4b: 字符串类型的数值字段用 BETWEEN，按字典序比较
-- 业务需求：查找 app_id 在 "100" 到 "200" 之间的 app
-- ❌ 错误：app_id 是 STRING 类型，BETWEEN 按字典序，"1000" < "200"
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id BETWEEN '100' AND '200'                      -- ❌ 字典序：'1000' < '200'，会包含 1000~1999
ORDER BY a.app_id;

-- ✅ 正确写法：转为数值比较
-- AND CAST(a.app_id AS BIGINT) BETWEEN 100 AND 200


-- ---------------------------------------------------------------------------
-- Case 4c: 毫秒时间戳 BETWEEN 包含右端边界
-- 业务需求：查找 submit_time 在某个小时范围内的 job
-- ❌ 错误：BETWEEN 包含右端，恰好等于右端的记录被多算
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.submit_time,
    j.start_time,
    (j.start_time - j.submit_time)                          AS queue_time
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.submit_time BETWEEN 1741363200000 AND 1741366800000 -- ❌ 包含右端 1741366800000
                                                            -- 等于右端的记录被算入当前时间段
                                                            -- 而下一个时间段也可能包含该记录
ORDER BY j.submit_time;

-- ✅ 正确写法：用半开区间避免重叠
-- AND j.submit_time >= 1741363200000 AND j.submit_time < 1741366800000


-- ---------------------------------------------------------------------------
-- Case 4d: NOT BETWEEN 的边界盲区
-- 业务需求：排除运行时间在 1-10 分钟的 task（保留极短和极长的）
-- ❌ 错误：NOT BETWEEN 排除两端，恰好 60000 和 600000 的 task 被排除
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.task_run_time NOT BETWEEN 60000 AND 600000          -- ❌ 排除了 60000 和 600000 本身
                                                            -- 开发者可能只想排除 (60000, 600000) 开区间
ORDER BY t.task_run_time DESC
LIMIT 200;

-- ✅ 正确写法：明确开区间
-- AND (t.task_run_time <= 60000 OR t.task_run_time >= 600000)
-- 或：AND (t.task_run_time < 60000 OR t.task_run_time > 600000)  -- 根据业务需求


-- ---------------------------------------------------------------------------
-- Case 4e: BETWEEN 左右值写反导致空结果
-- 业务需求：查找特定分区范围的 stage
-- ❌ 错误：BETWEEN 要求左值 <= 右值，写反则返回空集
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.num_tasks,
    (s.end_time - s.start_time)                             AS stage_duration
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt BETWEEN '20260308' AND '20260301'   -- ❌ 左 > 右，返回空集
  AND s.status = 'COMPLETE'
ORDER BY stage_duration DESC;

-- ✅ 正确写法：左值 <= 右值
-- WHERE s.dt BETWEEN '20260301' AND '20260308'
