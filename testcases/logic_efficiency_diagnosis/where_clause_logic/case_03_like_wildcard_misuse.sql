-- ============================================================================
-- Case 03: LIKE 通配符误用
-- ============================================================================
-- 【问题描述】
--   在 SparkSQL 环境中，LIKE 模式匹配只有 % 是通配符：
--     1. % 匹配任意长度字符串（含空）
--     2. _ (下划线) 不是通配符，是普通字面字符
--   常见的误用：
--     1. 前导 % 导致无法利用索引/分区，全表扫描
--     2. 数据中含有 % 字面值但未转义
--     3. 双侧 % 包裹导致匹配范围远超预期
--     4. 忘记 LIKE 是大小写敏感的（某些引擎），或混淆 LIKE 和 RLIKE
--     5. 用 LIKE '%' 代替 IS NOT NULL，语义不等价
--
-- 【易犯场景】
--   1. 搜索包含特定关键字的 app_name，前后都加 %，匹配到非预期结果
--   2. failed_reason 中含 % 特殊字符，LIKE 匹配到非预期的行
--   3. 用 LIKE '%%' 想匹配"任意字符串"，但忽略了 NULL
--   4. 多重 LIKE OR 组合缺少括号，AND 优先级导致逻辑混乱
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - LIKE 前导 % 导致全表扫描
--   - 双侧 % 匹配范围过大
--   - 建议检查通配符位置和特殊字符转义
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 3a: 前导 % 导致全表扫描
-- 业务需求：搜索 app_name 包含 "etl" 的应用
-- ❌ 错误：前导 % 使得分区裁剪和索引完全失效
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.`result`,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_name LIKE '%etl%'                               -- ❌ 前导 % 全表扫描
ORDER BY duration_ms DESC
LIMIT 100;

-- ✅ 正确写法：尽量避免前导 %，或用专门的搜索函数
-- AND INSTR(a.app_name, 'etl') > 0
-- 或如果知道前缀：AND a.app_name LIKE 'etl%'


-- ---------------------------------------------------------------------------
-- Case 3b: 双侧 % 包裹导致过度匹配
-- 业务需求：搜索 app_name 以 "spark" 为前缀的应用（如 "spark_etl_daily"）
-- ❌ 错误：使用 '%spark%' 导致匹配了 "my_spark_job"、"test_spark" 等非预期结果
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.`result`,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_name LIKE '%spark%'                             -- ❌ 双侧 % 匹配范围过大
                                                            -- 会匹配 "spark_etl"、"my_spark_job"、"test_spark_v2" 等
ORDER BY duration_ms DESC
LIMIT 50;

-- ✅ 正确写法：如果只要前缀匹配，去掉前导 %
-- AND a.app_name LIKE 'spark%'
-- 或使用更精确的正则：AND a.app_name RLIKE '^spark_'


-- ---------------------------------------------------------------------------
-- Case 3c: failed_reason 中含特殊字符，LIKE 匹配偏差
-- 业务需求：查找 failed_reason 包含 "100%" 的 job
-- ❌ 错误：% 是通配符，匹配了 "100" 后面跟任意字符
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.status = 'FAILED'
  AND j.failed_reason LIKE '%100%%'                         -- ❌ 第二个 % 是通配符不是字面值
                                                            -- 会匹配 "100" 后面跟任何字符串
ORDER BY job_duration DESC;

-- ✅ 正确写法：转义 %
-- AND j.failed_reason LIKE '%100\%%' ESCAPE '\'


-- ---------------------------------------------------------------------------
-- Case 3d: LIKE '%' 代替 IS NOT NULL，忽略空字符串
-- 业务需求：查找有 failed_reason 的 job
-- ❌ 错误：LIKE '%' 不匹配 NULL，也不区分空字符串
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.failed_reason LIKE '%'                              -- ❌ 匹配所有非 NULL 值，包括空字符串
                                                            -- 开发者可能想要"有实际内容的 failed_reason"
ORDER BY job_duration DESC;

-- ✅ 正确写法：显式排除 NULL 和空字符串
-- AND j.failed_reason IS NOT NULL
-- AND j.failed_reason != ''
-- AND LENGTH(TRIM(j.failed_reason)) > 0


-- ---------------------------------------------------------------------------
-- Case 3e: 多重 LIKE OR 组合导致逻辑混乱
-- 业务需求：查找 app_name 以 "etl_" 开头或以 "_daily" 结尾的 app
-- ❌ 错误：OR 和 LIKE 组合后缺少括号，AND 优先级问题叠加
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.`result`,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_name LIKE 'etl_%'                               -- 匹配以 "etl_" 开头的（_ 是字面字符）
  OR a.app_name LIKE '%_daily'                              -- ❌ OR 优先级低于 AND，此行脱离了分区条件
  AND a.`result` = 0                                          -- ❌ 只约束了第二个 LIKE
ORDER BY duration_ms DESC;

-- ✅ 正确写法：用括号明确 OR 的范围
-- WHERE a.dt = '20260308'
--   AND (a.app_name LIKE 'etl_%' OR a.app_name LIKE '%_daily')
--   AND a.`result` = 0
