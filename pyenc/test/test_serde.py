import pyarrow as pa
from pyarrow import fs

import enc

local = fs.LocalFileSystem()


def test_serde(tmp_path):
    a = pa.array([0, 1, 2, 3])
    arr = enc.encode(a)
    assert isinstance(arr, enc.PrimitiveArray)
    subfs = fs.SubTreeFileSystem(str(tmp_path), local)
    with subfs.open_output_stream("array.enc", buffer_size=8192) as nf:
        enc.write(arr, nf)

    with subfs.open_input_stream("array.enc", buffer_size=8192) as nf:
        read_array = enc.read(arr.dtype, nf)
        assert isinstance(read_array, enc.PrimitiveArray)
