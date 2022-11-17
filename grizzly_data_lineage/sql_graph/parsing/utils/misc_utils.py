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

from textwrap import wrap
from typing import Tuple


def wrap_text(
    text: str,
    text_width: int,
    letter_width: int,
    line_height: int,
    max_lines: int = 0,
  ) -> Tuple[str, int]:
  letters_per_line = int(text_width // letter_width)
  lines = wrap(text, width=letters_per_line)
  n_lines = len(lines)
  if max_lines > 0:
    if n_lines > max_lines:
      n_lines = max_lines
      lines[n_lines - 1] += "..."
  wrapped_text = "\n".join(lines[:n_lines])
  text_height = int(n_lines * line_height)
  return wrapped_text, text_height
