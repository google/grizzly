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

CREATE OR REPLACE MODEL ml_models.crime_forcast OPTIONS (model_type='linear_reg',
    input_label_cols=['daily_crime_count']) AS
SELECT
  sum(crime_count) as daily_crime_count,
  DATE_DIFF(CAST(date AS DATE), CAST('2016-01-01' AS DATE),day) AS day_difference
FROM
  biz_store_research.daily_crime_count t1
  group by 2