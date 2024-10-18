Dataset
=======

Vortex files implement the Arrow Dataset interface permitting efficient use of a Vortex file within
a query engines like DuckDB and Polars. In particular, Vortex will read data proportional to the
number of rows passing a filter condition and the number of columns in a selection. For most Vortex
encodings, this property holds true even when the filter condition specifies a single row.

.. automodule:: vortex.dataset
   :members:
