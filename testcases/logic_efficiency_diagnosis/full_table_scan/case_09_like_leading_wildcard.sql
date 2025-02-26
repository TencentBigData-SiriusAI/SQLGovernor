-- ============================================================================
-- Case 09: LIKE 前导通配符导致全表扫描
-- ============================================================================
-- 【问题描述】
--   使用 LIKE '%keyword' 或 LIKE '%keyword%' 时，由于前导通配符的存在，
--   引擎无法利用任何索引或分区裁剪优化，必须逐行扫描匹配。
--   在无分区条件的情况下更是雪上加霜——全分区 + 全行逐一匹配。
--
-- 【易犯场景】
--   1. 按 app_name 模糊搜索（如搜包含某关键字的任务）
--   2. 按失败原因 failed_reason 模糊查找错误类型
--   3. 在 details / dag_info 等大文本字段上做模糊搜索
--   4. 研发人员习惯性地用 LIKE '%xxx%' 做调试查询
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：
--   - LIKE 前导通配符无法利用索引/分区优化
--   - 缺少分区过滤条件（部分 case）
-- ============================================================================

-- Case 9a: LIKE '%xxx%' 无分区条件
-- 想找 app_name 中包含 "etl_daily" 的任务，但没加分区限定
-- 全分区 + 全行 LIKE 匹配，极度低效
SELECT
    app_id,
    app_name,
    `user`,
    platform,
    start_time,
    end_time
FROM spark_analytics.spark_app_metrics
WHERE app_name LIKE '%etl_daily%';


-- Case 9b: LIKE '%xxx' 后缀匹配 + 无分区条件
-- 想找失败原因以 "OutOfMemoryError" 结尾的 job，前导通配符导致全扫描
SELECT
    app_id,
    job_id,
    status,
    failed_reason,
    start_time,
    end_time
FROM spark_analytics.spark_job_metrics
WHERE failed_reason LIKE '%OutOfMemoryError';


-- Case 9c: 在大文本字段上做 LIKE + 缺分区条件
-- stage 表的 details 字段是执行说明（可能很长），对其做模糊匹配非常低效
-- 即使加了分区条件，对大文本 LIKE 仍然代价高昂
SELECT
    app_id,
    stage_id,
    stage_name,
    details,
    input_size
FROM spark_analytics.spark_stage_metrics
WHERE details LIKE '%HashAggregate%'
  AND details LIKE '%BroadcastHashJoin%';
