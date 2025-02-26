-- ============================================================================
-- Case 06: OR 条件绕过分区裁剪
-- ============================================================================
-- 【问题描述】
--   使用 OR 连接多个条件时，如果其中一个分支没有带分区过滤，
--   引擎无法进行分区裁剪，只能退化为全分区扫描。
--   即使另一个分支已经正确限定了分区范围，也无法避免。
--
-- 【易犯场景】
--   1. "查今天的数据 OR 查失败的数据"，后者没带日期条件
--   2. 逐步叠加查询条件时，OR 分支忘记同步添加分区过滤
--   3. 动态拼接 SQL 时，某些分支路径未正确注入分区条件
--   4. 业务方提需求"查这天的或者那个app_id的"，后者无法限定分区
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：
--   - OR 条件中存在未带分区过滤的分支，导致全分区扫描
-- ============================================================================

-- Case 6a: OR 左侧有分区条件，右侧无分区条件
-- 研发想查 "今天失败的" 或 "所有历史中特别慢的" 任务
-- 右侧 (end_time - start_time > 3600000) 没有分区限定，导致全表扫描
SELECT
    app_id,
    app_name,
    `user`,
    result,
    start_time,
    end_time
FROM spark_analytics.spark_app_metrics
WHERE (dt = '20260308' AND result != 0)
   OR (end_time - start_time > 3600000);


-- Case 6b: UNION ALL 替代 OR 但两侧分区条件不一致
-- 第一个子查询有分区条件，第二个没有，UNION ALL 后仍然触发全表扫描
SELECT app_id, stage_id, stage_name, input_size, status
FROM spark_analytics.spark_stage_metrics
WHERE dt = '20260308'
  AND status = 0

UNION ALL

SELECT app_id, stage_id, stage_name, input_size, status
FROM spark_analytics.spark_stage_metrics
WHERE input_size > 10737418240;  -- 10GB，无分区过滤


-- Case 6c: OR 条件嵌套在 CASE WHEN 中
-- 虽然表面上有 dt 过滤，但 OR 的第二分支破坏了裁剪
SELECT
    app_id,
    job_id,
    status,
    CASE WHEN status = 0 THEN 'SUCCESS' ELSE 'FAILED' END AS status_desc
FROM spark_analytics.spark_job_metrics
WHERE dt = '20260308'
   OR (status = -1 AND failed_reason IS NOT NULL);
