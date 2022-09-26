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

CREATE OR REPLACE FUNCTION `common_functions_library.LocalDatetime`
  (
    audit_time TIMESTAMP,
    timezone_hrs FLOAT64
  )
RETURNS DATETIME OPTIONS (description = "Input: time as TIMESTAMP, timezone_hrs as FLOAT64")
AS (
  DATETIME_ADD(DATETIME(audit_time), INTERVAL CAST(timezone_hrs * 60 AS INT64) MINUTE)
);
