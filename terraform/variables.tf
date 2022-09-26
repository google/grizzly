/**
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

variable "composer_environment_name" {}
variable "gcp_project_id" {}
variable "composer_image_version" {}
variable "composer_location" {}
variable "composer_node_zone" {}
variable "default_datacatalog_taxonomy_location" {}

# Check that Composer V2 is used. It requires different configuration.
locals {
  is_composer_v2 = length(regexall("composer-2.*", var.composer_image_version)) > 0
}