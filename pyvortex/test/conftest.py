#  (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import pathlib
import subprocess

logging.basicConfig(level=logging.DEBUG)


def pytest_sessionstart():
    """Pytest plugin to trigger maturin builds before running tests."""
    working_dir = pathlib.Path(__file__).parent.parent
    subprocess.check_call(["maturin", "develop", "--skip-install"], cwd=working_dir)
