USE slow_query_log;

CREATE TEMPORARY TABLE `slow_query_log`.`statements_temp`

SELECT
  `SCHEMA_NAME`,
  `DIGEST`,
  `DIGEST_TEXT`,
  `COUNT_STAR`,
  `SUM_TIMER_WAIT`,
  `MIN_TIMER_WAIT`,
  `AVG_TIMER_WAIT`,
  `MAX_TIMER_WAIT`,
  `SUM_LOCK_TIME`,
  `SUM_ERRORS`,
  `SUM_WARNINGS`,
  `SUM_ROWS_AFFECTED`,
  `SUM_ROWS_SENT`,
  `SUM_ROWS_EXAMINED`,
  `SUM_CREATED_TMP_DISK_TABLES`,
  `SUM_CREATED_TMP_TABLES`,
  `SUM_SELECT_FULL_JOIN`,
  `SUM_SELECT_FULL_RANGE_JOIN`,
  `SUM_SELECT_RANGE`,
  `SUM_SELECT_RANGE_CHECK`,
  `SUM_SELECT_SCAN`,
  `SUM_SORT_MERGE_PASSES`,
  `SUM_SORT_RANGE`,
  `SUM_SORT_ROWS`,
  `SUM_SORT_SCAN`,
  `SUM_NO_INDEX_USED`,
  `SUM_NO_GOOD_INDEX_USED`,
  GREATEST("1990-01-01 00:00:00", `FIRST_SEEN`) AS FIRST_SEEN, -- хак чтобы обойти ограничение в MySQL 5.7 с 0000 в датах
  GREATEST("1990-01-01 00:00:00", `LAST_SEEN`) AS LAST_SEEN
FROM performance_schema.events_statements_summary_by_digest;

INSERT INTO `slow_query_log`.`events_statements` (DIGEST, DIGEST_TEXT, first_seen, last_seen)
SELECT DIGEST, DIGEST_TEXT, FIRST_SEEN, LAST_SEEN
FROM `slow_query_log`.`statements_temp`
  ON DUPLICATE KEY UPDATE
    first_seen = LEAST(VALUES(`slow_query_log`.`events_statements`.first_seen),
                              `slow_query_log`.`events_statements`.first_seen),
    last_seen = GREATEST(VALUES(`slow_query_log`.`events_statements`.last_seen),
                                `slow_query_log`.`events_statements`.last_seen);

SELECT CONCAT('INSERT IGNORE INTO events_statements_history (',
              GROUP_CONCAT(DISTINCT a.column_name),',hostname) SELECT ',
              GROUP_CONCAT(DISTINCT a.column_name),', @@hostname FROM statements_temp')
              INTO @stmt
FROM information_schema.columns a
  JOIN information_schema.columns b
    ON a.column_name=b.column_name AND b.table_name='events_statements_history'
WHERE a.table_schema='performance_schema' AND a.table_name='events_statements_summary_by_digest';

PREPARE stmt FROM @stmt;

EXECUTE stmt;

DROP TABLE IF EXISTS statements_temp;
-- TRUNCATE TABLE performance_schema.events_statements_summary_by_digest;