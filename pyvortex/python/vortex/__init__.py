from ._lib import __doc__ as module_docs, io, expr
from . import encoding, dtype

__doc__ = module_docs
del module_docs

encode = encoding.encode
