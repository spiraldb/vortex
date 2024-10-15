import pyarrow
import pandas

from ._lib import encoding as _encoding

__doc__ = _encoding.__doc__

Array = _encoding.Array
compress = _encoding.compress


def _Array_to_pandas(self: _encoding.Array):
    """Construct a Pandas dataframe from this Vortex array.

    Warning
    -------

    Only struct-typed arrays can be converted to Pandas dataframes.

    Parameters
    ----------
    obj : :class:`pyarrow.Array` or :class:`list`
        The elements of this array or list become the elements of the Vortex array.

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
    >>> array.to_pandas()
                                     x
    0    {'age': 25, 'name': 'Joseph'}
    1  {'age': 31, 'name': 'Narendra'}
    2    {'age': 33, 'name': 'Angela'}
    3   {'age': 57, 'name': 'Mikhail'}

    Lift the struct fields to the top-level in the dataframe:

    """
    return table_from_struct_array(self.to_arrow()).to_pandas(types_mapper=pandas.ArrowDtype)


def empty_table(schema: pyarrow.Schema) -> pyarrow.Table:
    return pyarrow.Table.from_arrays([[] for _ in schema], schema=schema)


def table_from_struct_array(array: pyarrow.StructArray | pyarrow.ChunkedArray):
    if len(array) == 0:
        return empty_table(pyarrow.schema(array.type))
    return pyarrow.Table.from_struct_array(array)


Array.to_pandas = _Array_to_pandas


def _Array_to_numpy(self: _encoding.Array, *, zero_copy_only: bool = True):
    """Construct a NumPy array from this Vortex array.

    This is an alias for :code:`self.to_arrow().to_numpy(zero_copy_only)`

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
    return self.to_arrow().to_numpy(zero_copy_only=zero_copy_only)


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

    >>> vortex.encoding.array([1, 2, 3]).to_arrow()
    <pyarrow.lib.Int64Array object at ...>
    [
      1,
      2,
      3
    ]

    The same Vortex array with a null value in the third position.

    >>> vortex.encoding.array([1, 2, None, 3]).to_arrow()
    <pyarrow.lib.Int64Array object at ...>
    [
      1,
      2,
      null,
      3
    ]

    Initialize a Vortex array from an Arrow array:

    >>> arrow = pyarrow.array(['Hello', 'it', 'is', 'me'])
    >>> vortex.encoding.array(arrow).to_arrow()
    <pyarrow.lib.StringArray object at ...>
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
