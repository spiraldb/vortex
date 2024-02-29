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

import vortex


def test_int():
    assert str(vortex.int()) == "signed_int(_)"
    assert str(vortex.int(32)) == "signed_int(32)"
    assert str(vortex.int(32, signed=False)) == "unsigned_int(32)"
    assert str(vortex.float(16)) == "float(16)"
    assert str(vortex.bool(nullable=True)) == "bool?"
