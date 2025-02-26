-- ============================================================================
-- Case 06: WHERE 条件中隐式类型转换
-- ============================================================================
-- 【问题描述】
--   当 WHERE 条件中比较的两端类型不一致时，SQL 引擎会做隐式类型转换：
--     1. STRING 与 BIGINT 比较：字符串被转为数值，非数字字符串报错或为 NULL
--     2. 分区字段（通常是 STRING）与数值比较：分区裁剪失效，全表扫描
--     3. 隐式转换导致比较语义变化（如字符串 '100' < '99' 按字典序）
--     4. CAST 显式转换时目标类型选错
--     5. 日期字符串格式不一致导致比较错误
--
-- 【易犯场景】
--   1. dt 是 STRING，与 INT 比较导致分区裁剪失效
--   2. app_id 是 STRING，直接与数值比较
--   3. 时间戳是 BIGINT，与日期字符串比较
--   4. 从其他表 JOIN 时两端同名字段类型不一致
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - WHERE 条件存在隐式类型转换，可能导致分区裁剪失效或结果偏差
--   - 建议显式转换或使用一致的类型
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 6a: 分区字段与数值比较，分区裁剪失效
-- dt 是 STRING 类型，直接与 INT 比较
-- ❌ 错误：INT 导致 STRING 分区字段被隐式转换，分区裁剪失效
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = 20260308                          -- ❌ INT 类型，分区裁剪失效
  AND a.result != 0
ORDER BY duration_ms DESC
LIMIT 100;

-- ✅ 正确写法：使用 STRING 类型
-- WHERE a.dt = '20260308'


-- ---------------------------------------------------------------------------
-- Case 6b: STRING 类型的 app_id 与数值比较，字典序 vs 数值序
-- 业务需求：查找 app_id > 1000 的 app
-- ❌ 错误：app_id 是 STRING，直接与数值比较会隐式转换
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id > 1000                                       -- ❌ STRING 与 INT 比较
                                                            -- '999' 会被转为 999 参与比较
                                                            -- 非数字 app_id（如 'app_xxx'）转换失败
ORDER BY a.app_id
LIMIT 100;

-- ✅ 正确写法：显式转换
-- AND CAST(a.app_id AS BIGINT) > 1000
-- 或先过滤非数字：AND a.app_id RLIKE '^[0-9]+$' AND CAST(a.app_id AS BIGINT) > 1000


-- ---------------------------------------------------------------------------
-- Case 6c: BIGINT 时间戳与 STRING 日期字符串比较
-- 业务需求：查找 submit_time 在某天之后的 job
-- ❌ 错误：submit_time 是 BIGINT 毫秒时间戳，与日期字符串比较无意义
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.submit_time,
    j.start_time,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.submit_time > '2026-03-08 00:00:00'                 -- ❌ BIGINT 与 STRING 比较
                                                            -- STRING 被转为数值会失败或得到 NULL
ORDER BY j.submit_time;

-- ✅ 正确写法：使用一致的类型
-- AND j.submit_time > UNIX_TIMESTAMP('2026-03-08 00:00:00') * 1000


-- ---------------------------------------------------------------------------
-- Case 6d: 分区字段范围比较时类型不一致
-- 业务需求：查询最近 7 天的数据
-- ❌ 错误：一边是 STRING 一边是 INT，行为不可预测
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.num_tasks,
    s.dt
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt >= 20260302                         -- ❌ INT 类型
  AND s.dt <= '20260308'                       -- STRING 类型
  AND s.status = 'COMPLETE'
ORDER BY s.dt;

-- ✅ 正确写法：统一使用 STRING
-- WHERE s.dt >= '20260302' AND s.dt <= '20260308'


-- ---------------------------------------------------------------------------
-- Case 6e: CASE WHEN 中的隐式转换导致 WHERE 过滤异常
-- 业务需求：根据 executor_memory 分级，过滤 "large" 级别
-- ❌ 错误：CASE 返回混合类型（STRING 和 INT），隐式转换
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.task_run_time,
    t.gc_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND CASE
        WHEN t.task_run_time > 300000 THEN 'slow'
        WHEN t.task_run_time > 60000 THEN 'medium'
        ELSE 0                                              -- ❌ 返回 INT，与 STRING 混合
      END = 'slow'
ORDER BY t.task_run_time DESC
LIMIT 100;

-- ✅ 正确写法：CASE 返回一致的类型
-- ELSE 'fast'   -- 全部返回 STRING
