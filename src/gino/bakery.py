import copy

from sqlalchemy import text

from .api import GinoExecutor, _PlaceHolder
from .exceptions import UninitializedError


class BakedQuery(GinoExecutor):
    def __init__(self, elem, metadata, hash_=None):
        super().__init__(self)
        self._elem = elem
        self._metadata = metadata
        if hash_ is None:
            self._hash = hash(elem)
        else:
            self._hash = hash_
        self._compiled_sql = None
        self._sql = None

    def _set_sql(self, sql):
        self._sql = sql

    def _execute_on_connection(self, conn, multiparams, params):
        return conn._execute_baked_query(self, multiparams, params)

    def get(self, _):
        return self.compiled_sql

    def __setitem__(self, key, value):
        self._compiled_sql = value

    @property
    def compiled_sql(self):
        return self._compiled_sql

    @property
    def elem(self):
        return self._elem

    @property
    def sql(self):
        return self._sql

    @property
    def query(self):
        return self.elem

    @property
    def bind(self):
        rv = self.elem.bind
        if rv is not None:
            return rv
        if self._metadata is None:
            return _PlaceHolder(UninitializedError("Gino engine is not initialized."))
        return self._metadata.bind

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return self._hash == getattr(other, "_hash", None)

    def execution_options(self, **kwargs):
        rv = _ShadowBakedQuery(self)
        return rv.execution_options(**kwargs)


class _ShadowBakedQuery(BakedQuery):
    def __init__(self, bq):
        super().__init__(bq.elem, bq._metadata, hash(bq))
        self._compiled_sql = copy.copy(bq.compiled_sql)
        self._sql = bq._sql

    def execution_options(self, **kwargs):
        self._elem = self._elem.execution_options(**kwargs)
        self._compiled_sql.execution_options = self._elem.get_execution_options()
        return self


class Bakery:
    query_cls = BakedQuery

    def __init__(self):
        self._queries = []

    def __iter__(self):
        return iter(self._queries)

    def bake(self, func_or_elem=None, **execution_options):
        if func_or_elem is None:

            def _wrapper(val):
                return self.bake(val, **execution_options)

            return _wrapper

        metadata = execution_options.pop("metadata", None)
        elem = func_or_elem() if callable(func_or_elem) else func_or_elem
        if isinstance(elem, str):
            elem = text(elem)
        if execution_options:
            elem = elem.execution_options(**execution_options)
        bq = self.query_cls(elem, metadata)
        self._queries.append(bq)
        return bq
