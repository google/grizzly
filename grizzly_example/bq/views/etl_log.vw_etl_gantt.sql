-- Copyright 2022 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE OR REPLACE VIEW etl_log.vw_etl_gantt AS
SELECT
  log.job_id,
  log.job_name AS Task,
  DATE(log.job_start_timestamp) AS start_date ,
  DATETIME(log.job_start_timestamp) AS start_date_time,
  FORMAT_TIME('%H:%M:%S',TIME(log.job_start_timestamp)) AS start_time,
  TIMESTAMP_TRUNC(log.job_start_timestamp, SECOND) AS Start,
  DATE(log.job_end_timestamp) AS finish_date ,
  DATETIME(log.job_end_timestamp) AS finish_date_time,
  FORMAT_TIME('%H:%M:%S',TIME(log.job_end_timestamp)) AS finish_time,
  TIMESTAMP_TRUNC(log.job_end_timestamp, SECOND) AS Finish,
  IF (TIMESTAMP_DIFF(log.job_end_timestamp, log.job_start_timestamp, SECOND) > 60 ,
    CONCAT("Duration: ",TIMESTAMP_DIFF(log.job_end_timestamp, log.job_start_timestamp, MINUTE)," min ",
      MOD(TIMESTAMP_DIFF(log.job_end_timestamp, log.job_start_timestamp, SECOND), 60), " sec"),
    CONCAT("Duration: ",TIMESTAMP_DIFF(log.job_end_timestamp, log.job_start_timestamp, SECOND), " sec")
  ) AS duration,
  TIMESTAMP_DIFF(log.job_end_timestamp, log.job_start_timestamp, SECOND) AS duration_sec,
  CAST(DATE(TIMESTAMP_TRUNC(MIN(log.job_start_timestamp)OVER(PARTITION BY log.job_id), SECOND)) AS STRING) AS dag_start_date,
  TIMESTAMP_TRUNC(MIN(log.job_start_timestamp)OVER(PARTITION BY log.job_id), SECOND) AS dag_start_time,
  TIMESTAMP_TRUNC(MAX(log.job_end_timestamp)OVER(PARTITION BY log.job_id), SECOND) AS dag_end_time,
  log.job_status,
  ARRAY_TO_STRING(log.source_table, ', ') AS source_table,
  log.target_table,
  log.subject_area,
  CASE WHEN log.job_status = 'SUCCESS' THEN 'green'
    ELSE 'red'
  END AS job_status_color
FROM `etl_log.composer_job_details` AS log
ORDER BY dag_start_time DESC, Start
