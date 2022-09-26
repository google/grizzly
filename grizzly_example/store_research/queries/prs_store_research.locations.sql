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

SELECT
    zip_code,
    year,
    supermarket_count,
    num_of_hazards,
    num_of_abandoned,
    riot_per_year,
    zip_code_geom,
    ROUND(households_per_sq_mile, 2) AS households_per_sq_mile,
    ROUND(commuters_by_public_transportation_rate * 100, 2) AS commuters_by_public_transportation_rate,
    ROUND(crime_violent_rate, 2) AS crime_violent_rate,
    ROUND(crime_property_rate, 2) AS crime_property_rate,
    CASE
        WHEN supermarket_count = 0 OR supermarket_count IS NULL THEN 20
        WHEN supermarket_count = 1 THEN 15
        WHEN supermarket_count = 2 THEN 10
        WHEN supermarket_count = 3 THEN 5
        ELSE 0
    END +
    CASE
        WHEN households_per_sq_mile > 20000  THEN 20
        WHEN households_per_sq_mile > 10000 THEN 15
        WHEN households_per_sq_mile > 5000 THEN 10
        WHEN households_per_sq_mile > 1000 THEN 5
        ELSE 0
    END +
    CASE
        WHEN commuters_by_public_transportation_rate * 100 > 80  THEN 20
        WHEN commuters_by_public_transportation_rate * 100 > 50 THEN 15
        WHEN commuters_by_public_transportation_rate * 100 > 20 THEN 10
        WHEN commuters_by_public_transportation_rate * 100 > 10 THEN 5
        ELSE 0
    END +
    CASE
        WHEN crime_violent_rate + crime_property_rate < 0.01  THEN 20
        WHEN crime_violent_rate + crime_property_rate < 0.5 THEN 15
        WHEN crime_violent_rate + crime_property_rate < 1 THEN 10
        WHEN crime_violent_rate + crime_property_rate < 3 THEN 5
        ELSE 0
    END +
    CASE
        WHEN num_of_hazards = 0 OR num_of_hazards IS NULL THEN 20
        WHEN num_of_hazards = 1 THEN 15
        WHEN num_of_hazards = 2 THEN 10
        WHEN num_of_hazards = 3 THEN 5
        ELSE 0
    END +
    CASE
        WHEN num_of_abandoned = 0 OR num_of_abandoned IS NULL THEN 20
        WHEN num_of_abandoned = 1 THEN 15
        WHEN num_of_abandoned = 2 THEN 10
        WHEN num_of_abandoned = 3 THEN 5
        ELSE 0
    END +
    CASE
        WHEN riot_per_year = 0 OR riot_per_year IS NULL THEN 20
        WHEN riot_per_year = 1 THEN 15
        WHEN riot_per_year = 2 THEN 10
        WHEN riot_per_year = 3 THEN 5
        ELSE 0
    END AS scout_score
FROM
    `biz_store_research.location_results`