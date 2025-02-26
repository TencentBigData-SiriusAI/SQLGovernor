-- ============================================================================
-- Case 05: 隐式类型转换导致分区裁剪失效
-- ============================================================================
-- 【问题描述】
--   分区字段 dt 为 STRING 类型，但查询条件中使用了数值类型或
--   日期类型进行比较，引擎可能做隐式类型转换，导致分区裁剪失效。
--
-- 【易犯场景】
--   1. 分区列是 STRING，但用整数做等值/范围比较（如 dt = 20260308）
--   2. 使用 DATE/TIMESTAMP 类型与 STRING 分区列做比较
--   3. 参数化查询时传入了非 STRING 类型的参数
--   4. 从其他数据库迁移 SQL 时，未注意 SparkSQL 中分区列的实际类型
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：
--   - 分区列发生了隐式类型转换，可能导致分区裁剪失效
-- ============================================================================

-- Case 5a: 用整数与 STRING 类型分区列比较
-- dt 是 STRING 类型，用数值 20260308 比较会触发隐式转换
-- 某些引擎会将整个分区列 CAST 为 BIGINT，导致全分区扫描
SELECT
    app_id,
    app_name,
    `user`,
    result
FROM spark_analytics.spark_app_metrics
WHERE dt = 20260308;


-- Case 5b: 用整数做范围比较
-- 同理，BETWEEN 两侧为数值类型，STRING 列被隐式转换
SELECT
    app_id,
    job_id,
    action,
    status,
    failed_reason
FROM spark_analytics.spark_job_metrics
WHERE dt BETWEEN 20260301 AND 20260307;


-- Case 5c: 关联条件中的隐式类型转换
-- stage_id 在 stage 表中是 STRING，在 task 表中也是 STRING
-- 但如果研发人员对 stage_id 做了 CAST，也会导致问题
-- 这里演示分区列的隐式转换场景
SELECT
    t.app_id,
    t.task_id,
    t.task_run_time,
    t.gc_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt > 20260300
  AND t.status = 0;
