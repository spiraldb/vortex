import pyarrow as pa
import vortex
from pyarrow import fs

local = fs.LocalFileSystem()


def test_serde(tmp_path):
    a = pa.array([0, 1, 2, 3])
    arr = vortex.encode(a)
    assert isinstance(arr, vortex.PrimitiveArray)
    subfs = fs.SubTreeFileSystem(str(tmp_path), local)
    with subfs.open_output_stream("array.enc", buffer_size=8192) as nf:
        vortex.write(arr, nf)

    with subfs.open_input_stream("array.enc", buffer_size=8192) as nf:
        read_array = vortex.read(arr.dtype, nf)
        assert isinstance(read_array, vortex.PrimitiveArray)
