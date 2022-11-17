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
    event.GLOBALEVENTID,
    event.SQLDATE,
    event.MonthYear,
    event.Year,
    event.FractionDate,
    event.Actor1Code,
    event.Actor1Name,
    event.Actor1CountryCode,
    event.Actor1KnownGroupCode,
    event.Actor1EthnicCode,
    event.Actor1Religion1Code,
    event.Actor1Religion2Code,
    event.Actor1Type1Code,
    event.Actor1Type2Code,
    event.Actor1Type3Code,
    event.Actor2Code,
    event.Actor2Name,
    event.Actor2CountryCode,
    event.Actor2KnownGroupCode,
    event.Actor2EthnicCode,
    event.Actor2Religion1Code,
    event.Actor2Religion2Code,
    event.Actor2Type1Code,
    event.Actor2Type2Code,
    event.Actor2Type3Code,
    event.IsRootEvent,
    event.EventCode,
    event.EventBaseCode,
    event.EventRootCode,
    event.QuadClass,
    event.GoldsteinScale,
    event.NumMentions,
    event.NumSources,
    event.NumArticles,
    event.AvgTone,
    event.Actor1Geo_Type,
    event.Actor1Geo_FullName,
    event.Actor1Geo_CountryCode,
    event.Actor1Geo_ADM1Code,
    event.Actor1Geo_ADM2Code,
    event.Actor1Geo_Lat,
    event.Actor1Geo_Long,
    event.Actor1Geo_FeatureID,
    event.Actor2Geo_Type,
    event.Actor2Geo_FullName,
    event.Actor2Geo_CountryCode,
    event.Actor2Geo_ADM1Code,
    event.Actor2Geo_ADM2Code,
    event.Actor2Geo_Lat,
    event.Actor2Geo_Long,
    event.Actor2Geo_FeatureID,
    event.ActionGeo_Type,
    event.ActionGeo_FullName,
    event.ActionGeo_CountryCode,
    event.ActionGeo_ADM1Code,
    event.ActionGeo_ADM2Code,
    event.ActionGeo_Lat,
    event.ActionGeo_Long,
    event.ActionGeo_FeatureID,
    event.DATEADDED,
    event.SOURCEURL,
    ST_GEOGPOINT(event.Actor1Geo_Long, event.Actor1Geo_Lat) AS geo_point,
    zc.zip_code,
    zc.zip_code_geom
FROM `bas_gdelt.events` AS event
INNER JOIN
    `biz_store_research.chicago_zip_codes` AS zc
ON ST_CONTAINS(zc.zip_code_geom, ST_GEOGPOINT(event.Actor1Geo_Long, event.Actor1Geo_Lat))
WHERE
    event.Year > 2012
    AND event.EventRootCode = '14'
