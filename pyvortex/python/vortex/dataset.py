from typing import Any, Iterator, Callable

import warnings
import operator
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset
from ._lib import io, expr, dtype
import substrait
from substrait.proto import ExtendedExpression


def _arrow_expression_to_vortex_expression(arrow_expression: pc.Expression, schema: pa.Schema) -> list[expr.Expr]:
    functions = []

    def literal(substrait_object) -> expr.Expr:
        # https://github.com/substrait-io/substrait/blob/main/proto/substrait/algebra.proto#L890
        match substrait_object.WhichOneof("literal_type"):
            case "boolean":
                return expr.literal(dtype.bool(False), substrait_object.boolean)
            case "i8":
                return expr.literal(dtype.int(8, False), substrait_object.i8)
            case "i16":
                return expr.literal(dtype.int(16, False), substrait_object.i16)
            case "i32":
                return expr.literal(dtype.int(32, False), substrait_object.i32)
            case "i64":
                return expr.literal(dtype.int(64, False), substrait_object.i64)
            case "fp32":
                return expr.literal(dtype.float(32, False), substrait_object.fp32)
            case "fp64":
                return expr.literal(dtype.float(64, False), substrait_object.fp64)
            case "string":
                return expr.literal(dtype.utf8(False), substrait_object.string)
            case "binary":
                return expr.literal(dtype.binary(False), substrait_object.binary)
            case "timestamp":
                raise NotImplementedError
            case "date":
                raise NotImplementedError
            case "time":
                raise NotImplementedError
            case "interval_year_to_month":
                raise NotImplementedError
            case "interval_day_to_second":
                raise NotImplementedError
            case "interval_compound":
                raise NotImplementedError
            case "fixed_char":
                raise NotImplementedError
            case "var_char":
                raise NotImplementedError
            case "fixed_binary":
                raise NotImplementedError
            case "decimal":
                raise NotImplementedError
            case "precision_timestamp":
                raise NotImplementedError
            case "precision_timestamp_tz":
                raise NotImplementedError
            case "struct":
                raise NotImplementedError
            case "map":
                raise NotImplementedError
            case "timestamp_tz":
                raise NotImplementedError
            case "uuid":
                raise NotImplementedError
            case "null":
                # substrait_object.null is a Type which needs to be converted
                raise NotImplementedError
            case "list":
                raise NotImplementedError
            case "empty_list":
                # substrait_object.empty_list is a Type which needs to be converted
                raise NotImplementedError
            case "empty_map":
                # substrait_object.empty_map is a Type which needs to be converted
                raise NotImplementedError
            case "user_defined":
                raise NotImplementedError
            case literal_type:
                raise ValueError(f"unknown literal_type {literal_type}")

    def field_reference(substrait_object) -> expr.Expr:
        # https://github.com/substrait-io/substrait/blob/main/proto/substrait/algebra.proto#L1415
        match substrait_object.WhichOneof("reference_type"):
            case "direct_reference":
                segments = reference_segment(substrait_object.direct_reference)
                if len(segments) == 0 or len(segments) > 1:
                    raise NotImplementedError
                arrow_field = schema[segments[0]]
                return expr.column(arrow_field.name)
            case "masked_reference":
                raise NotImplementedError
            case reference_type:
                raise ValueError(f"unknown reference_type {reference_type}")

    def reference_segment(substrait_object) -> list[int]:
        # NB: returns the field ids in reverse order i.e. [deepest, next_deepest, ..., top_level]
        #
        # https://github.com/substrait-io/substrait/blob/main/proto/substrait/algebra.proto#L1312
        match substrait_object.WhichOneof("reference_type"):
            case "map_key":
                raise NotImplementedError
            case "struct_field":
                return struct_field(substrait_object.struct_field)
            case "list_element":
                raise NotImplementedError
            case reference_type:
                raise ValueError(f"unknown reference_type {reference_type}")

    def struct_field(substrait_object) -> list[int]:
        if substrait_object.HasField("child"):
            segment = reference_segment(substrait_object.child)
            segment.append(substrait_object.field)
            return segment
        else:
            return [substrait_object.field]

    def scalar_function(substrait_object) -> expr.Expr:
        # https://github.com/substrait-io/substrait/blob/main/proto/substrait/extensions/extensions.proto#L57
        function = functions[substrait_object.function_reference]
        if len(substrait_object.options) != 0:
            raise NotImplementedError(substrait_object.options)
        arguments = [function_argument(argument) for argument in substrait_object.arguments]
        return function(*arguments)

    def function_argument(substrait_object) -> expr.Expr:
        # https://github.com/substrait-io/substrait/blob/main/proto/substrait/algebra.proto#L832
        match substrait_object.WhichOneof("arg_type"):
            case "enum":
                raise NotImplementedError
            case "type":
                raise NotImplementedError
            case "value":
                return expression(substrait_object.value)
            case arg_type:
                raise ValueError(f"unknown arg_type {arg_type}")

    def extension_function(
        substrait_object, extension_uris: list["substrait.proto.extensions.SimpleExtensionURI"]
    ) -> Callable:
        # https://github.com/substrait-io/substrait/blob/main/proto/substrait/extensions/extensions.proto#L57
        match extension_uris[substrait_object.extension_uri_reference].uri:
            case "https://github.com/substrait-io/substrait/blob/main/extensions/functions_boolean.yaml":
                match substrait_object.name:
                    case "or":
                        return operator.or_
                    case "and":
                        return operator.and_
                    case "xor":
                        return operator.__xor__
                    case "not":
                        return operator.__not__
                    case name:
                        raise NotImplementedError(f"Function name {name} not supported")
            case "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml":
                match substrait_object.name:
                    case "equal":
                        return operator.__eq__
                    case "not_equal":
                        return operator.__ne__
                    case "lt":
                        return operator.__lt__
                    case "lte":
                        return operator.__le__
                    case "gt":
                        return operator.__gt__
                    case "gte":
                        return operator.__ge__
                    case "is_null":
                        raise NotImplementedError
                    case "is_not_null":
                        raise NotImplementedError
                    case name:
                        raise NotImplementedError(f"Function name {name} not supported")
            case uri:
                raise NotImplementedError(f"Extension URI {uri} not supported")

    def expression(substrait_object) -> expr.Expr:
        # https://github.com/substrait-io/substrait/blob/main/proto/substrait/algebra.proto#L857
        match substrait_object.WhichOneof("rex_type"):
            case "literal":
                return literal(substrait_object.literal)
            case "selection":
                return field_reference(substrait_object.selection)
            case "scalar_function":
                return scalar_function(substrait_object.scalar_function)
            case "window_function":
                raise NotImplementedError
            case "if_then":
                raise NotImplementedError
            case "switch":
                raise NotImplementedError
            case "singular":
                raise NotImplementedError
            case "multi":
                raise NotImplementedError
            case "cast":
                raise NotImplementedError
            case "subquery":
                raise NotImplementedError
            case "nested":
                raise NotImplementedError
            case rex_type:
                raise ValueError(f"unknown rex_type {rex_type}")

    def expression_reference(substrait_object) -> expr.Expr:
        # https://github.com/substrait-io/substrait/blob/main/proto/substrait/extended_expression.proto#L16
        match substrait_object.WhichOneof("expr_type"):
            case "expression":
                return expression(substrait_object.expression)
            case _:
                raise ValueError("unknown expr_type: {}")

    # https://github.com/substrait-io/substrait/blob/main/proto/substrait/extended_expression.proto#L27
    substrait_object = ExtendedExpression()
    substrait_object.ParseFromString(arrow_expression.to_substrait(schema).to_pybytes())
    substrait_schema = substrait_object.base_schema
    extension_uris = substrait_object.extension_uris

    for extension in substrait_object.extensions:
        match extension.WhichOneof("mapping_type"):
            case "extension_function":
                functions.append(extension_function(extension.extension_function, extension_uris))
            case mapping_type:
                raise ValueError(f"unsupported extension mapping_type {mapping_type}")

    expressions = [expression_reference(substrait_object) for substrait_object in substrait_object.referred_expr]

    if len(expressions) < 0 or len(expressions) > 1:
        raise ValueError("_arrow_expression_to_vortex_expression: only exactly one expression support")
    return expressions[0]


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
            filter = _arrow_expression_to_vortex_expression(filter, self.schema)
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
            filter = _arrow_expression_to_vortex_expression(filter, self.schema)
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
