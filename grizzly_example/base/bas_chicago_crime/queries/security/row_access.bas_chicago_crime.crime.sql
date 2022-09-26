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

CREATE OR REPLACE ROW ACCESS POLICY v2_demo_ftes
ON `{{ table_name }}`
GRANT TO ("{{{DEFAULT_USER}}}")
FILTER USING ( 1 = 1);

CREATE OR REPLACE ROW ACCESS POLICY v2_demo
ON `{{ table_name }}`
GRANT TO ("{{{DEFAULT_USER}}}")
FILTER USING ( trim(upper(replace(primary_type," ",""))) not like 'NON-CRIMINAL%');

