-- ============================================================================
-- Case 09: IN 列表膨胀与逻辑陷阱
-- ============================================================================
-- 【问题描述】
--   IN 列表在 WHERE 条件中非常常用，但有以下陷阱：
--     1. IN 列表过长（数百上千个值）导致 SQL 解析慢、执行计划爆炸
--     2. IN 与 OR 混用导致优先级问题
--     3. IN 列表中有重复值，浪费但不报错
--     4. IN (子查询) 与 IN (常量列表) 的性能差异
--     5. IN 与 NOT IN 在 NULL 处理上的不对称性
--
-- 【易犯场景】
--   1. 从 Excel 导入数百个 app_id 做 IN 过滤
--   2. IN 列表与其他条件 OR 组合时缺少括号
--   3. IN 列表太长导致编译超时或 OOM
--   4. IN 列表中混入了不同类型的值
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - IN 列表过长，建议改用临时表 JOIN
--   - IN 与 OR 混用可能存在优先级问题
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 9a: IN 列表过长导致 SQL 解析和执行性能问题
-- ❌ 错误：数百个常量值在 IN 列表中，解析极慢
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id IN (                                         -- ❌ IN 列表极长
        'app_001', 'app_002', 'app_003', 'app_004', 'app_005',
        'app_006', 'app_007', 'app_008', 'app_009', 'app_010',
        'app_011', 'app_012', 'app_013', 'app_014', 'app_015',
        'app_016', 'app_017', 'app_018', 'app_019', 'app_020',
        -- ... 假设这里有数百个值 ...
        'app_096', 'app_097', 'app_098', 'app_099', 'app_100'
        -- ❌ 实际场景中可能上千个，导致编译超时
      )
ORDER BY duration_ms DESC;

-- ✅ 正确写法：使用临时表 JOIN
-- CREATE TEMPORARY TABLE tmp_app_list AS SELECT 'app_001' AS app_id UNION ALL ...;
-- SELECT a.* FROM app a INNER JOIN tmp_app_list t ON a.app_id = t.app_id WHERE ...


-- ---------------------------------------------------------------------------
-- Case 9b: IN 与 AND/OR 混用导致优先级问题
-- 业务需求：查找 spark/flink 平台上的失败 app 或大内存 app
-- ❌ 错误：OR 打破了 IN 与 AND 的组合逻辑
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    a.executor_memory
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.platform IN ('spark', 'platform_c')
  AND a.result != 0
  OR a.executor_memory > 16384                              -- ❌ OR 脱离了前面所有 AND
ORDER BY a.executor_memory DESC;
-- 实际：(imp_date AND platform IN (...) AND result!=0) OR (memory>16384)
-- 第二个分支无日期过滤

-- ✅ 正确写法：用括号明确逻辑
-- AND (a.result != 0 OR a.executor_memory > 16384)
-- AND a.platform IN ('spark', 'platform_c')


-- ---------------------------------------------------------------------------
-- Case 9c: IN 列表中类型不一致导致隐式转换
-- 业务需求：查找特定 job_id 的 job
-- ❌ 错误：IN 列表中混入了数值类型，与 STRING 字段比较
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND j.job_id IN ('job_001', 'job_002', 12345, 67890)      -- ❌ STRING 和 INT 混合
                                                            -- 隐式转换可能导致意外匹配
ORDER BY job_duration DESC;

-- ✅ 正确写法：统一类型
-- AND j.job_id IN ('job_001', 'job_002', '12345', '67890')


-- ---------------------------------------------------------------------------
-- Case 9d: 嵌套 IN + EXISTS 逻辑混乱
-- 业务需求：查找 app_id 属于大内存列表 且 有失败 stage 的 task
-- ❌ 错误：IN 子查询和 EXISTS 子查询条件交叉，逻辑难以理解
-- ---------------------------------------------------------------------------
SELECT
    t.task_id,
    t.app_id,
    t.stage_id,
    t.task_run_time,
    t.gc_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.app_id IN (
        SELECT a.app_id
        FROM spark_analytics.spark_app_metrics a
        WHERE a.dt = '20260308'
          AND a.executor_memory > 8192
      )
  AND EXISTS (
        SELECT 1
        FROM spark_analytics.spark_stage_metrics s
        WHERE s.dt = '20260308'
          AND s.status = 'FAILED'
          -- ❌ 缺少 AND s.app_id = t.app_id AND s.stage_id = t.stage_id
          -- EXISTS 只检查是否有任何失败 stage，而非当前 task 所在的 stage
      )
ORDER BY t.gc_time DESC
LIMIT 200;

-- ✅ 正确写法：EXISTS 中添加完整关联条件
-- WHERE s.app_id = t.app_id AND s.stage_id = t.stage_id
--   AND s.dt = '20260308' AND s.status = 'FAILED'


-- ---------------------------------------------------------------------------
-- Case 9e: IN 列表有重复值且与 OR 冗余
-- ❌ 错误：IN 列表有重复，外层还有 OR 条件覆盖了 IN 的部分值
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.num_tasks
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND (s.status IN ('COMPLETE', 'RUNNING', 'COMPLETE',      -- ❌ 'COMPLETE' 重复
                     'PENDING', 'FAILED')
       OR s.status = 'COMPLETE')                            -- ❌ 冗余，已在 IN 列表中
ORDER BY s.num_tasks DESC;

-- ✅ 正确写法：去重，移除冗余 OR
-- AND s.status IN ('COMPLETE', 'RUNNING', 'PENDING', 'FAILED')
