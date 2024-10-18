from typing import Any, Iterator

import warnings
import pyarrow as pa
import pyarrow.compute as pc

from ._lib import io
from .arrow.expression import arrow_to_vortex as arrow_to_vortex_expr


class VortexDataset(pa.dataset.Dataset):
    def __init__(self, fname: str):
        self._fname = fname
        self._dataset = io.dataset(fname)

    @property
    def schema(self) -> pa.Schema:
        return self._dataset.schema()

    def count_rows(
        self,
        filter: pc.Expression | None = None,
        batch_size: int | None = None,
        batch_readahead: int | None = None,
        fragment_readahead: int | None = None,
        fragment_scan_options: pa.dataset.FragmentScanOptions | None = None,
        use_threads: bool = True,
        memory_pool: pa.MemoryPool = None,
    ) -> int:
        raise NotImplementedError("count_rows")

    def filter(self, expression: pc.Expression) -> "VortexDataset":
        raise NotImplementedError("filter")

    def get_fragments(self, filter: pc.Expression | None = None) -> Iterator[pa.dataset.Fragment]:
        raise NotImplementedError("get_fragments")

    def head(
        self,
        num_rows: int,
        columns: list[str] | None = None,
        filter: pc.Expression | None = None,
        batch_size: int | None = None,
        batch_readahead: int | None = None,
        fragment_readahead: int | None = None,
        fragment_scan_options: pa.dataset.FragmentScanOptions | None = None,
        use_threads: bool | None = None,
        memory_pool: pa.MemoryPool = None,
    ) -> pa.Table:
        if batch_readahead is not None:
            raise ValueError("batch_readahead not supported")
        if fragment_readahead is not None:
            raise ValueError("fragment_readahead not supported")
        if fragment_scan_options is not None:
            raise ValueError("fragment_scan_options not supported")
        if use_threads is True:
            warnings.warn("Vortex does not support threading. Ignoring use_threads=True")
        if columns is not None and len(columns) == 0:
            raise ValueError("empty projections are not currently supported")
        del memory_pool
        if filter is not None:
            filter = arrow_to_vortex_expr(filter, self.schema)
        return self._dataset.to_array(columns, batch_size, filter).slice(0, num_rows).to_arrow_table()

    def join(
        self,
        right_dataset,
        keys,
        right_keys=None,
        join_type=None,
        left_suffix=None,
        right_suffix=None,
        coalesce_keys=True,
        use_threads=True,
    ) -> pa.dataset.InMemoryDataset:
        raise NotImplementedError("join")

    def join_asof(self, right_dataset, on, by, tolerance, right_on=None, right_by=None) -> pa.dataset.InMemoryDataset:
        raise NotImplementedError("join_asof")

    def replace_schema(self, schema: pa.Schema):
        raise NotImplementedError("replace_schema")

    # py:meth reference target not found: Scanner.from_dataset [ref.meth]
    #
    # def scanner(
    #     self,
    #     columns: list[str] | None = None,
    #     filter: pc.Expression | None = None,
    #     batch_size: int | None = None,
    #     batch_readahead: int | None = None,
    #     fragment_readahead: int | None = None,
    #     fragment_scan_options: pa.dataset.FragmentScanOptions | None = None,
    #     use_threads: bool = True,
    #     memory_pool: pa.MemoryPool = None,
    # ) -> pa.dataset.Scanner:
    #     raise NotImplementedError('scanner')

    # Inline strong start-string without end-string. [docutils]
    #
    # def sort_by(self, sorting, **kwargs) -> pa.dataset.InMemoryDataset:
    #     raise NotImplementedError('sort_by')

    def take(
        self,
        indices: pa.Array | Any,
        columns: list[str] | None = None,
        filter: pc.Expression | None = None,
        batch_size: int | None = None,
        batch_readahead: int | None = None,
        fragment_readahead: int | None = None,
        fragment_scan_options: pa.dataset.FragmentScanOptions | None = None,
        use_threads: bool = True,
        memory_pool: pa.MemoryPool = None,
    ) -> pa.Table:
        raise NotImplementedError("take")

    def to_batches(
        self,
        columns: list[str] | None = None,
        filter: pc.Expression | None = None,
        batch_size: int | None = None,
        batch_readahead: int | None = None,
        fragment_readahead: int | None = None,
        fragment_scan_options: pa.dataset.FragmentScanOptions | None = None,
        use_threads: bool = True,
        memory_pool: pa.MemoryPool = None,
    ) -> Iterator[pa.RecordBatch]:
        raise NotImplementedError("to_batches")

    def to_table(
        self,
        columns=None,
        filter: pc.Expression | None = None,
        batch_size: int | None = None,
        batch_readahead: int | None = None,
        fragment_readahead: int | None = None,
        fragment_scan_options: pa.dataset.FragmentScanOptions | None = None,
        use_threads: bool = True,
        memory_pool: pa.MemoryPool = None,
    ) -> pa.Table:
        if batch_readahead is not None:
            raise ValueError("batch_readahead not supported")
        if fragment_readahead is not None:
            raise ValueError("fragment_readahead not supported")
        if fragment_scan_options is not None:
            raise ValueError("fragment_scan_options not supported")
        if use_threads is True:
            warnings.warn("Vortex does not support threading. Ignoring use_threads=True")
        if columns is not None and len(columns) == 0:
            raise ValueError("empty projections are not currently supported")
        del memory_pool
        if filter is not None:
            filter = arrow_to_vortex_expr(filter, self.schema)
        return self._dataset.to_array(columns, batch_size, filter).to_arrow_table()


class VortexScanner(pa.dataset.Scanner):
    """A PyArrow Dataset Scanner that reads from a Vortex Array."""

    def __init__(self):
        pass

    @property
    def schema(self):
        return self._schema

    def count_rows(self):
        raise NotImplementedError

    def head(self, num_rows: int) -> pa.Table:
        raise NotImplementedError

    def scan_batches(self) -> Iterator[pa.dataset.TaggedRecordBatch]:
        raise NotImplementedError

    def to_batches(self) -> Iterator[pa.RecordBatch]:
        raise NotImplementedError

    def to_reader(self) -> pa.RecordBatchReader:
        raise NotImplementedError

    def to_table(self) -> pa.Table:
        raise NotImplementedError
