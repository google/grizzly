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

SELECT * FROM `bas_geo_us_boundaries.zip_codes`
WHERE zip_code in (
  '60290', '60601', '60602', '60603', '60604', '60605', '60606', '60607',
  '60608', '60610', '60611', '60614', '60615', '60618', '60619', '60622',
  '60623', '60624', '60628', '60609', '60612', '60613', '60616', '60617',
  '60620', '60621', '60625', '60626', '60629', '60630', '60632', '60636',
  '60637', '60631', '60633', '60634', '60638', '60641', '60642', '60643',
  '60646', '60647', '60652', '60653', '60656', '60660', '60661', '60664',
  '60639', '60640', '60644', '60645', '60649', '60651', '60654', '60655',
  '60657', '60659', '60666', '60668', '60673', '60677', '60669', '60670',
  '60674', '60675', '60678', '60680', '60681', '60682', '60686', '60687',
  '60688', '60689', '60694', '60695', '60697', '60699', '60684', '60685',
  '60690', '60691', '60693', '60696', '60701')