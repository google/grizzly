# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Definition of all data extractors plugins alias.

  Custom plugins must be mentioned in EXTRACTOR_CLASSES_ALIAS to be
  availabe in grizzly platform. Key in EXTRACTOR_CLASSES_ALIAS dic should
  be in lower cases.

  Typical usage example:

  EXTRACTOR_CLASSES_ALIAS = {
    "grizzly.extractors.custom_bq.ExtractorCustomBQ".lower() :
        "grizzly.extractors.custom_bq.ExtractorCustomBQ"
  }
"""

EXTRACTOR_CLASSES_ALIAS = {
    "grizzly.extractors.bq_dlp.ExtractorBQDlp".lower() :
        "grizzly.extractors.bq_dlp.ExtractorBQDlp",
    "grizzly.extractors.bq.ExtractorBQ".lower() :
        "grizzly.extractors.bq.ExtractorBQ",
    "grizzly.extractors.csv_url.ExtractorCSV".lower() :
        "grizzly.extractors.csv_url.ExtractorCSV",
    "grizzly.extractors.excel_url.ExtractorExcel".lower() :
        "grizzly.extractors.excel_url.ExtractorExcel",
    "grizzly.extractors.gsheet.ExtractorGSheet".lower() :
        "grizzly.extractors.gsheet.ExtractorGSheet",
    "grizzly.extractors.gsheet.ExtractorGSheet".lower() :
        "grizzly.extractors.gsheet.ExtractorGSheet",
    "grizzly.extractors.shapefile.ExtractorShapefile".lower() :
        "grizzly.extractors.shapefile.ExtractorShapefile",
    "grizzly.extractors.wordpress.ExtractorWordpress".lower() :
        "grizzly.extractors.wordpress.ExtractorWordpress"
}
