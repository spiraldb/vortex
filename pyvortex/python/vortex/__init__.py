from ._lib import __doc__ as module_docs, io, expr, dtype
from . import encoding


__doc__ = module_docs
del module_docs
array = encoding.array
