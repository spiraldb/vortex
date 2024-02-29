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

import pyarrow as pa
import pytest

import vortex


def test_primitive_array_round_trip():
    a = pa.array([0, 1, 2, 3])
    arr = vortex.encode(a)
    assert isinstance(arr, vortex.PrimitiveArray)
    assert arr.to_pyarrow().combine_chunks() == a


def test_varbin_array_round_trip():
    a = pa.array(["a", "b", "c"])
    arr = vortex.encode(a)
    assert isinstance(arr, vortex.VarBinArray)
    assert arr.to_pyarrow().combine_chunks() == a


@pytest.mark.xfail(strict=True)
def test_varbin_array_doesnt_round_trip():
    a = pa.array(["a", "b", "c"], type=pa.large_utf8())
    arr = vortex.encode(a)
    assert isinstance(arr, vortex.VarBinArray)
    assert arr.to_pyarrow().combine_chunks() == a


def test_empty_array():
    a = pa.array([], type=pa.uint8())
    primitive = vortex.encode(a)
    assert primitive.to_pyarrow().type == pa.uint8()
