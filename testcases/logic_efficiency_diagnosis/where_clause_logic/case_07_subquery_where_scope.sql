-- ============================================================================
-- Case 07: 子查询 WHERE 作用域错误
-- ============================================================================
-- 【问题描述】
--   在包含子查询的 WHERE 中，条件的作用域容易出错：
--     1. 外层 WHERE 条件误写到子查询中，缩小了子查询范围
--     2. 子查询的过滤条件漏到外层，改变了主查询的过滤逻辑
--     3. 关联子查询中关联条件不足，子查询范围过大
--     4. EXISTS 子查询中遗漏外层关联，变成"是否有任意数据"
--     5. 标量子查询中 WHERE 条件遗漏导致返回多行报错
--
-- 【易犯场景】
--   1. 把本应在外层的分区过滤写到了子查询中
--   2. EXISTS 子查询忘记写 WHERE 与外层的关联条件
--   3. IN 子查询中的条件与外层条件范围不一致
--   4. 嵌套多层子查询时条件层级混乱
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - 子查询 WHERE 条件作用域可能有误
--   - 建议检查内外层过滤条件的归属
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 7a: 外层过滤条件误写到子查询中
-- 业务需求：查找有失败 job 的 app（只看 20260308）
-- ❌ 错误：日期条件只在子查询中，外层 app 表无分区过滤
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.app_id IN (                                         -- ❌ 外层缺少分区过滤，全表扫描
        SELECT j.app_id
        FROM spark_analytics.spark_job_metrics j
        WHERE j.dt = '20260308'                -- 分区条件只在子查询中
          AND j.status = 'FAILED'
      )
  -- ❌ 缺少 AND a.dt = '20260308'
ORDER BY duration_ms DESC
LIMIT 100;

-- ✅ 正确写法：内外层都要加分区过滤
-- WHERE a.dt = '20260308'
--   AND a.app_id IN (SELECT j.app_id FROM ... WHERE j.dt = '20260308' ...)


-- ---------------------------------------------------------------------------
-- Case 7b: 子查询的条件漏写到外层导致主查询被错误过滤
-- 业务需求：统计每个 app 的失败 task 数量
-- ❌ 错误：t.status = 'FAILED' 应该在子查询中，但写到了外层
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    (
        SELECT COUNT(*)
        FROM spark_analytics.spark_task_metrics t
        WHERE t.app_id = a.app_id
          AND t.dt = a.dt
          -- ❌ 缺少 AND t.status = 'FAILED'，统计了所有 task 而非失败 task
    )                                                       AS failed_task_count
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0
  AND 'FAILED' = 'FAILED'                                   -- ❌ 条件漏到外层，变成恒真
ORDER BY failed_task_count DESC
LIMIT 100;

-- ✅ 正确写法：条件放到子查询中
-- SELECT COUNT(*) FROM task t
-- WHERE t.app_id = a.app_id AND t.dt = a.dt AND t.status = 'FAILED'


-- ---------------------------------------------------------------------------
-- Case 7c: EXISTS 子查询遗漏关联条件
-- 业务需求：找出有长耗时 stage 的 app
-- ❌ 错误：EXISTS 子查询没有与外层 app 关联，只要有任何长耗时 stage 就返回所有 app
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    (a.end_time - a.start_time)                             AS app_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND EXISTS (
        SELECT 1
        FROM spark_analytics.spark_stage_metrics s
        WHERE s.dt = '20260308'
          AND (s.end_time - s.start_time) > 600000
          -- ❌ 缺少 AND s.app_id = a.app_id
          -- 只要 stage 表有任何一条超过 10 分钟的记录，所有 app 都返回
      )
ORDER BY app_duration DESC;

-- ✅ 正确写法：添加关联条件
-- WHERE s.app_id = a.app_id AND s.dt = a.dt
--   AND (s.end_time - s.start_time) > 600000


-- ---------------------------------------------------------------------------
-- Case 7d: IN 子查询与外层条件范围不一致
-- 业务需求：查找 20260308 有失败 job 的 app
-- ❌ 错误：子查询查的是 20260307 的 job，与外层日期不一致
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'                        -- 外层查 0308
  AND a.app_id IN (
        SELECT j.app_id
        FROM spark_analytics.spark_job_metrics j
        WHERE j.dt = '20260307'                -- ❌ 子查询查 0307
          AND j.status = 'FAILED'
      )
ORDER BY duration_ms DESC;

-- ✅ 正确写法：统一日期
-- WHERE j.dt = '20260308'


-- ---------------------------------------------------------------------------
-- Case 7e: 多层嵌套子查询的条件层级混乱
-- 业务需求：找出有高 GC task 的失败 job 的 app
-- ❌ 错误：最内层 task 的 gc 条件写到了中间层 job 上
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id IN (
        SELECT j.app_id
        FROM spark_analytics.spark_job_metrics j
        WHERE j.dt = '20260308'
          AND j.status = 'FAILED'
          AND j.app_id IN (
                SELECT t.app_id
                FROM spark_analytics.spark_task_metrics t
                WHERE t.dt = '20260308'
                  -- ❌ 缺少 AND t.gc_time > 60000，高 GC 条件遗漏
              )
          -- ❌ gc_time 相关条件不在这层，而是完全缺失了
      )
ORDER BY a.app_id;

-- ✅ 正确写法：在最内层 task 子查询中加条件
-- SELECT t.app_id FROM task t
-- WHERE t.dt = '20260308' AND t.gc_time > 60000
