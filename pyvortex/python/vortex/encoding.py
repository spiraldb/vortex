from typing import TYPE_CHECKING

import pandas
import pyarrow

from ._lib import encoding as _encoding

if TYPE_CHECKING:
    import numpy
    import pandas

__doc__ = _encoding.__doc__

Array = _encoding.Array
compress = _encoding.compress


def empty_arrow_table(schema: pyarrow.Schema) -> pyarrow.Table:
    return pyarrow.Table.from_arrays([[] for _ in schema], schema=schema)


def arrow_table_from_struct_array(array: pyarrow.StructArray | pyarrow.ChunkedArray) -> pyarrow.Table:
    if len(array) == 0:
        return empty_arrow_table(pyarrow.schema(array.type))
    return pyarrow.Table.from_struct_array(array)


def _Array_to_arrow_table(self: _encoding.Array) -> pyarrow.Table:
    """Construct an Arrow table from this Vortex array.

    .. seealso::
        :meth:`.to_arrow_array`

    Warning
    -------

    Only struct-typed arrays can be converted to Arrow tables.

    Returns
    -------

    :class:`pyarrow.Table`

    Examples
    --------

    >>> array = vortex.encoding.array([
    ...     {'name': 'Joseph', 'age': 25},
    ...     {'name': 'Narendra', 'age': 31},
    ...     {'name': 'Angela', 'age': 33},
    ...     {'name': 'Mikhail', 'age': 57},
    ... ])
    >>> array.to_arrow_table()
    pyarrow.Table
    age: int64
    name: string_view
    ----
    age: [[25,31,33,57]]
    name: [["Joseph","Narendra","Angela","Mikhail"]]

    """
    return arrow_table_from_struct_array(self.to_arrow_array())


Array.to_arrow_table = _Array_to_arrow_table


def _Array_to_pandas(self: _encoding.Array) -> "pandas.DataFrame":
    """Construct a Pandas dataframe from this Vortex array.

    Warning
    -------

    Only struct-typed arrays can be converted to Pandas dataframes.

    Returns
    -------
    :class:`pandas.DataFrame`

    Examples
    --------

    Construct a dataframe from a Vortex array:

    >>> array = vortex.encoding.array([
    ...     {'name': 'Joseph', 'age': 25},
    ...     {'name': 'Narendra', 'age': 31},
    ...     {'name': 'Angela', 'age': 33},
    ...     {'name': 'Mikhail', 'age': 57},
    ... ])
    >>> array.to_pandas().dtypes
    age           int64[pyarrow]
    name    string_view[pyarrow]
    dtype: object


    Lift the struct fields to the top-level in the dataframe:

    """
    return self.to_arrow_table().to_pandas(types_mapper=pandas.ArrowDtype)


Array.to_pandas = _Array_to_pandas


def _Array_to_polars_dataframe(
    self: _encoding.Array,
):  # -> 'polars.DataFrame':  # breaks docs due to Polars issue #7027
    """Construct a Polars dataframe from this Vortex array.

    .. seealso::
        :meth:`.to_polars_series`

    Warning
    -------

    Only struct-typed arrays can be converted to Polars dataframes.

    Returns
    -------

    ..
        Polars excludes the DataFrame class from their Intersphinx index https://github.com/pola-rs/polars/issues/7027

    `polars.DataFrame <https://docs.pola.rs/api/python/stable/reference/dataframe/index.html>`__

    Examples
    --------

    >>> array = vortex.encoding.array([
    ...     {'name': 'Joseph', 'age': 25},
    ...     {'name': 'Narendra', 'age': 31},
    ...     {'name': 'Angela', 'age': 33},
    ...     {'name': 'Mikhail', 'age': 57},
    ... ])
    >>> array.to_polars_dataframe()
    shape: (4, 2)
    ┌─────┬──────────┐
    │ age ┆ name     │
    │ --- ┆ ---      │
    │ i64 ┆ str      │
    ╞═════╪══════════╡
    │ 25  ┆ Joseph   │
    │ 31  ┆ Narendra │
    │ 33  ┆ Angela   │
    │ 57  ┆ Mikhail  │
    └─────┴──────────┘

    """
    import polars

    return polars.from_arrow(self.to_arrow_table())


Array.to_polars_dataframe = _Array_to_polars_dataframe


def _Array_to_polars_series(self: _encoding.Array):  # -> 'polars.Series':  # breaks docs due to Polars issue #7027
    """Construct a Polars series from this Vortex array.

    .. seealso::
        :meth:`.to_polars_dataframe`

    Returns
    -------

    ..
        Polars excludes the Series class from their Intersphinx index https://github.com/pola-rs/polars/issues/7027

    `polars.Series <https://docs.pola.rs/api/python/stable/reference/series/index.html>`__

    Examples
    --------

    Convert a numeric array with nulls to a Polars Series:

    >>> vortex.encoding.array([1, None, 2, 3]).to_polars_series()  # doctest: +NORMALIZE_WHITESPACE
    shape: (4,)
    Series: '' [i64]
    [
        1
        null
        2
        3
    ]

    Convert a UTF-8 string array to a Polars Series:

    >>> vortex.encoding.array(['hello, ', 'is', 'it', 'me?']).to_polars_series()  # doctest: +NORMALIZE_WHITESPACE
    shape: (4,)
    Series: '' [str]
    [
        "hello, "
        "is"
        "it"
        "me?"
    ]

    Convert a struct array to a Polars Series:

    >>> array = vortex.encoding.array([
    ...     {'name': 'Joseph', 'age': 25},
    ...     {'name': 'Narendra', 'age': 31},
    ...     {'name': 'Angela', 'age': 33},
    ...     {'name': 'Mikhail', 'age': 57},
    ... ])
    >>> array.to_polars_series()  # doctest: +NORMALIZE_WHITESPACE
    shape: (4,)
    Series: '' [struct[2]]
    [
        {25,"Joseph"}
        {31,"Narendra"}
        {33,"Angela"}
        {57,"Mikhail"}
    ]

    """
    import polars

    return polars.from_arrow(self.to_arrow_array())


Array.to_polars_series = _Array_to_polars_series


def _Array_to_numpy(self: _encoding.Array, *, zero_copy_only: bool = True) -> "numpy.ndarray":
    """Construct a NumPy array from this Vortex array.

    This is an alias for :code:`self.to_arrow_array().to_numpy(zero_copy_only)`

    Returns
    -------
    :class:`numpy.ndarray`

    Examples
    --------

    Construct an ndarray from a Vortex array:

    >>> array = vortex.encoding.array([1, 0, 0, 1])
    >>> array.to_numpy()
    array([1, 0, 0, 1])

    """
    return self.to_arrow_array().to_numpy(zero_copy_only=zero_copy_only)


Array.to_numpy = _Array_to_numpy


def array(obj: pyarrow.Array | list) -> Array:
    """The main entry point for creating Vortex arrays from other Python objects.

    This function is also available as ``vortex.array``.

    Parameters
    ----------
    obj : :class:`pyarrow.Array` or :class:`list`
        The elements of this array or list become the elements of the Vortex array.

    Returns
    -------
    :class:`vortex.encoding.Array`

    Examples
    --------

    A Vortex array containing the first three integers.

    >>> vortex.encoding.array([1, 2, 3]).to_arrow_array()
    <pyarrow.lib.Int64Array object at ...>
    [
      1,
      2,
      3
    ]

    The same Vortex array with a null value in the third position.

    >>> vortex.encoding.array([1, 2, None, 3]).to_arrow_array()
    <pyarrow.lib.Int64Array object at ...>
    [
      1,
      2,
      null,
      3
    ]

    Initialize a Vortex array from an Arrow array:

    >>> arrow = pyarrow.array(['Hello', 'it', 'is', 'me'])
    >>> vortex.encoding.array(arrow).to_arrow_array()
    <pyarrow.lib.StringViewArray object at ...>
    [
      "Hello",
      "it",
      "is",
      "me"
    ]

    """
    if isinstance(obj, list):
        return _encoding._encode(pyarrow.array(obj))
    return _encoding._encode(obj)
