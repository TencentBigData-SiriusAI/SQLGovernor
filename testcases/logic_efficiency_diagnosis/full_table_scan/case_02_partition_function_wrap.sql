-- ============================================================================
-- Case 02: 分区列被函数包裹，导致分区裁剪失效
-- ============================================================================
-- 【问题描述】
--   虽然 WHERE 条件中出现了分区字段 dt，但使用了函数包裹，
--   导致引擎无法在编译期确定需要扫描哪些分区，退化为全分区扫描。
--
-- 【易犯场景】
--   1. 对日期分区列做 SUBSTR/SUBSTRING 截取后比较
--   2. 使用 CONCAT 拼接分区列再做比较
--   3. 使用 DATE_FORMAT/TO_DATE 等日期函数转换分区列
--   4. 对分区列做数学运算（如 CAST 为数字后比较）
--   这些操作在语义上看似限定了日期范围，但引擎无法优化为分区裁剪
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：分区列 dt 被函数包裹，无法进行分区裁剪
-- ============================================================================

-- Case 2a: SUBSTR 截取分区列做月份过滤
-- 研发人员想查某个月的数据，用 SUBSTR 截取年月
-- 正确做法：dt >= '20260201' AND dt < '20260301'
SELECT
    app_id,
    app_name,
    `user`,
    executor_num,
    executor_memory
FROM spark_analytics.spark_app_metrics
WHERE SUBSTR(dt, 1, 6) = '202602';


-- Case 2b: CONCAT 拼接分区列
-- 有些研发会先拼出完整日期字符串再做比较，引擎同样无法裁剪
SELECT
    app_id,
    job_id,
    status,
    start_time,
    end_time
FROM spark_analytics.spark_job_metrics
WHERE CONCAT(dt, '000000') >= '20260301000000';


-- Case 2c: CAST 分区列为整数后做数值比较
-- 将日期字符串转为数字来做范围查询，看似等价，实则分区裁剪完全失效
SELECT
    app_id,
    stage_id,
    stage_name,
    input_size,
    output_size
FROM spark_analytics.spark_stage_metrics
WHERE CAST(dt AS BIGINT) BETWEEN 20260301 AND 20260307;
