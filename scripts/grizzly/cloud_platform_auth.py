# Copyright 2021 Google LLC
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

"""Authenticate for work with GCP.

Typical usage example:
  auth()
"""

import google.auth.transport.requests


def auth() -> None:
  """GCP Authentication."""
  scopes = ['https://www.googleapis.com/auth/cloud-platform']
  credentials, _ = google.auth.default(scopes=scopes)
  auth_req = google.auth.transport.requests.Request()
  credentials.refresh(auth_req)
