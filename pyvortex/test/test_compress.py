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

import numpy as np
import pyarrow as pa

import vortex


def test_primitive_compress():
    a = pa.array([0, 0, 0, 0, 9, 9, 9, 9, 1, 5])
    arr_compressed = vortex.compress(vortex.encode(a))
    assert not isinstance(arr_compressed, vortex.PrimitiveArray)
    assert arr_compressed.nbytes < a.nbytes


def test_bool_compress():
    a = vortex.encode(pa.array([False] * 10_000 + [True] * 10_000))
    arr_compressed = vortex.compress(a)
    assert len(arr_compressed) == 20_000
    assert isinstance(arr_compressed, vortex.RoaringBoolArray)
    assert arr_compressed.nbytes < a.nbytes


def test_roaring_bool_encode():
    a = vortex.encode(pa.array([True] * 10_000))
    rarr = vortex.RoaringBoolArray.encode(a)
    assert isinstance(rarr, vortex.RoaringBoolArray)
    assert rarr.nbytes < a.nbytes


def test_roaring_int_encode():
    a = vortex.encode(pa.array(np.arange(10_000), type=pa.uint32()))
    compressed = vortex.compress(a)
    assert compressed.encoding == "roaring.int"


def test_zigzag_encode():
    a = vortex.encode(pa.array([-1, -1, 0, -1, 1, -1]))
    zarr = vortex.ZigZagArray.encode(a)
    assert isinstance(zarr, vortex.ZigZagArray)
    # TODO(ngates): support decoding once we have decompressor.


def test_chunked_encode():
    chunked = pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])])
    encoded = vortex.encode(chunked)
    assert isinstance(encoded, vortex.ChunkedArray)
    assert encoded.to_pyarrow().combine_chunks() == pa.array([0, 1, 2, 3, 4, 5])


def test_table_encode():
    table = pa.table(
        {
            "number": pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])]),
            "string": pa.chunked_array([pa.array(["a", "b", "c"]), pa.array(["d", "e", "f"])]),
        }
    )
    encoded = vortex.encode(table)
    assert isinstance(encoded, vortex.ChunkedArray)
    assert encoded.to_pyarrow().combine_chunks() == pa.StructArray.from_arrays(
        [pa.array([0, 1, 2, 3, 4, 5]), pa.array(["a", "b", "c", "d", "e", "f"])], names=["number", "string"]
    )
