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
  unique_key,
  company,
  dropoff_census_tract,
  dropoff_community_area,
  dropoff_latitude,
  dropoff_location,
  dropoff_longitude,
  extras,
  fare,
  payment_type,
  pickup_census_tract,
  pickup_community_area,
  pickup_latitude,
  pickup_location,
  pickup_longitude,
  taxi_id,
  tips,
  tolls,
  trip_end_timestamp,
  trip_miles,
  trip_seconds,
  trip_start_timestamp,
  trip_total
FROM EXTERNAL_QUERY(
    "grizzly-test-data.us.grizzly-test-data-grizzlypostgres",
    "SELECT unique_key, company, dropoff_census_tract, dropoff_community_area, dropoff_latitude, dropoff_location, dropoff_longitude, extras, fare, payment_type, pickup_census_tract, pickup_community_area, pickup_latitude, pickup_location, pickup_longitude, taxi_id, tips, tolls, trip_end_timestamp, trip_miles, trip_seconds, trip_start_timestamp, trip_total FROM taxi;"
)