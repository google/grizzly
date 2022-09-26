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

WITH crime AS (
  SELECT
    Row_number() OVER (
      ORDER BY
        unique_key
    ) AS rownumber,
    unique_key,
    address,
    census_tract,
    clearance_date,
    clearance_status,
    council_district_code,
    description,
    district,
    latitude,
    longitude,
    location,
    location_description,
    primary_type,
    timestamp,
    x_coordinate,
    y_coordinate,
    year,
    zipcode
  FROM
    `bigquery-public-data.austin_crime.crime` AS c
),
parameters AS (
  SELECT
    (
      SELECT
        count(1)
      FROM
        crime
    ) ids_count,
    date '2022-01-01' start_date,
    date '2024-01-01' finish_date
),
random_dates AS (
  SELECT
    id,
    date_from_unix_date(
      cast(
        start + (finish - start) * rand() AS int64
      )
    ) random_date
  FROM
    parameters,
    unnest(
      generate_array(1, ids_count)
    ) id,
    unnest(
      [STRUCT(
        UNIX_DATE(start_date) AS START,
        UNIX_DATE(finish_date) AS finish
      ) ]
    )
  ORDER BY
    random_date
),
random_dates_with_row_number AS (
  SELECT
    row_number() OVER (
      ORDER BY
        random_date
    ) AS rownumber,
    random_date
  FROM
    random_dates
),
crime_with_dates AS (
  SELECT
    *
  FROM
    random_dates_with_row_number FULL
    OUTER JOIN crime ON random_dates_with_row_number.rownumber = crime.rownumber
)
SELECT
  unique_key,
  address,
  census_tract,
  clearance_date,
  clearance_status,
  council_district_code,
  description,
  district,
  latitude,
  longitude,
  location,
  location_description,
  primary_type,
  timestamp,
  x_coordinate,
  y_coordinate,
  year,
  zipcode,
  random_date
FROM
  crime_with_dates