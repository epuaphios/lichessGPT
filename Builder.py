from bson.son import SON


def expr():
    """Get a new ``Expr`` instance.

    Returns:
        Expr: The ``Expr`` instance.
    """
    return Expr()


# noinspection PyTypeChecker
class Builder(object):
    def __init__(self, collection):
        self.collection = collection
        self.query = {'type': QueryTypes.TYPE_FIND}
        self.expression = Expr()

    @staticmethod
    def expr():
        return Expr()

    def field(self, field):
        self.expression.field(str(field))

        return self

    def find(self):
        self.query['type'] = QueryTypes.TYPE_FIND

        return self

    def find_and_remove(self):
        self.query['type'] = QueryTypes.TYPE_FIND_AND_REMOVE

        return self

    def find_and_update(self):
        self.query['type'] = QueryTypes.TYPE_FIND_AND_UPDATE

        return self

    def update(self, multi=False):
        self.query['type'] = QueryTypes.TYPE_UPDATE if not multi else QueryTypes.TYPE_UPDATE_MANY

        return self

    def upsert(self, upsert=True):
        self.query['upsert'] = bool(upsert)

        return self

    def insert(self):
        self.query['type'] = QueryTypes.TYPE_INSERT

        return self

    def build(self, **kwargs):
        query = self.query
        query['query'] = self.expression.query
        query['newObj'] = self.expression.new_obj

        return Query(self.collection, query, dict(kwargs))

    def get_query_list(self):
        return self.expression.query

    def select(self, *field_names):
        self.query.setdefault('select', {}).update({field_name: 1
                                                    for field_name in field_names})

        return self

    def select_elem_match(self, field_name, expression):
        self.query.setdefault('select', {}).update({
            field_name: {'$elemMatch': _get_query(expression)}
        })

        return self

    def select_meta(self, metadata_keyword):
        self.query.setdefault('select', {}).update({
            '$meta': metadata_keyword
        })

    def select_slice(self, field_name, count_or_skip, limit=None):
        slice = count_or_skip
        if limit is not None:
            slice = [slice, int(limit)]
        self.query.setdefault('select', {})[field_name] = {'$slice': slice}

        return self

    def set(self, value, atomic=True):
        self.expression.set(value, atomic and self.query['type'] != QueryTypes.TYPE_INSERT)

        return self

    def __getattr__(self, method_name):
        if hasattr(self.expression, method_name):
            def wrapper(*args, **kwargs):
                getattr(self.expression, method_name)(*args, **kwargs)

                return self

            return wrapper
        raise AttributeError(method_name)


class Query(object):
    def __init__(self, collection, query=(), options=()):
        self.collection = collection
        self.query = dict(query)
        self.options = dict(options)

    def execute(self):
        options = self.options

        # FIND
        if self.query['type'] == QueryTypes.TYPE_FIND:
            cursor = self.collection.find(
                self.query['query'],
                self.query['select'] if self.query.setdefault('select', []) else None
            )

            return cursor

        # INSERT
        if self.query['type'] == QueryTypes.TYPE_INSERT:
            return self.collection.insert(self.query['newObj'], options)

        # UPDATE
        if self.query['type'] == QueryTypes.TYPE_UPDATE:
            self.query.setdefault('multiple', False)
            self.query.setdefault('upsert', False)
            for option, value in self._get_query_options('multiple', 'upsert').iteritems():
                options.setdefault(option, value)

            multi = options.pop('multiple')
            func = getattr(self.collection, 'update_many' if multi else 'update_one')

            return func(self.query['query'],
                        self.query['newObj'])

        # REMOVE
        if self.query['type'] == QueryTypes.TYPE_REMOVE:
            return self.collection.remove(self.query['query'], options)

    def _get_query_options(self, *args):
        return {key: self.query[key]
                for key in args
                if key in self.query and self.query[key] is not None}


class QueryTypes(object):
    TYPE_FIND = 'find'
    TYPE_FIND_AND_UPDATE = 'find_and_modify'
    TYPE_FIND_AND_REMOVE = 'find_one_and_delete'
    TYPE_INSERT = 'insert'
    TYPE_INSERT_ONE = 'insert'
    TYPE_INSERT_MANY = 'insert_many'
    TYPE_UPDATE = 'update_one'
    TYPE_UPDATE_MANY = 'update_many'
    TYPE_REMOVE = 'remove'
    TYPE_GROUP = 'group'
    TYPE_MAP_REDUCE = 'map_reduce'
    TYPE_DISTINCT = 'distinct'
    TYPE_COUNT = 'count'
    TYPE_AGGREGATE = 'aggregate'


class Expr(object):
    def __init__(self):
        self.query = {}
        self.new_obj = {}
        self.current_field = None

    def add_and(self, expression):
        self.query.setdefault('$and', []).append(
            _get_query(expression)
        )

        return self

    def add_many_to_set(self, values):
        self._requires_current_field()
        self.new_obj.setdefault('$addToSet', {})[self.current_field] = {'$each': values}

        return self

    def add_nor(self, expression):
        self.query.setdefault('$nor', []).append(
            _get_query(expression)
        )

    def add_or(self, expression):
        self.query.setdefault('$or', []).append(
            _get_query(expression)
        )

    def add_to_set(self, value_or_expression):
        self._requires_current_field()
        self.new_obj.setdefault('$addToSet', {})[self.current_field] = _get_query(value_or_expression)

    def all(self, values):
        return self.operator('$all', list(values))

    def operator(self, operator, value):
        self._wrap_equality_criteria()

        if self.current_field:
            self.query.setdefault(self.current_field, {})[operator] = value
        else:
            self.query[operator] = value

        return self

    def _bit(self, operator, value):
        self._requires_current_field()
        self.new_obj.setdefault('$bit', {}).setdefault(self.current_field, {})[operator] = value

        return self

    def bit_and(self, value):
        return self._bit('and', value)

    def bit_or(self, value):
        return self._bit('or', value)

    def bits_all_clear(self, value):
        self._requires_current_field()
        return self.operator('$bitsAllClear', value)

    def bits_all_set(self, value):
        self._requires_current_field()
        return self.operator('$bitsAllSet', value)

    def case_sensitive(self, case_sensitive):
        if '$text' not in self.query:
            raise RuntimeError('This method requires a $text operator (call text() first)')

        if case_sensitive:
            self.query['$text']['$caseSensitive'] = True
        elif '$caseSensitive' in self.query['$text']:
            del self.query['$text']['$caseSensitive']

        return self

    def comment(self, comment):
        self.query['$comment'] = comment

        return self

    def current_date(self, type='date'):
        if type not in ('date', 'timestamp'):
            raise ValueError('Type for current_date operator must be "date" or "timestamp".')

        self._requires_current_field()
        self.new_obj.setdefault('$currentDate', {}).setdefault(self.current_field, {})['$type'] = type

        return self

    def each(self, values):
        return self.operator('$each', values)

    def elem_match(self, expression):
        return self.operator('$elemMatch', _get_query(expression))

    def equals(self, value):
        if self.current_field:
            self.query[self.current_field] = value
        else:
            self.query = value

        return self

    def exists(self, value=True):
        return self.operator('$exists', bool(value))

    def field(self, field):
        self.current_field = str(field)

        return self

    def gt(self, value):
        return self.operator('$gt', value)

    def gte(self, value):
        return self.operator('$gte', value)

    def lt(self, value):
        return self.operator('$lt', value)

    def lte(self, value):
        return self.operator('$lte', value)

    def is_in(self, values):
        return self.operator('$in', list(values.values()) if isinstance(values, dict) else values)

    def is_not_in(self, values):
        return self.operator('$nin', list(values.values()) if isinstance(values, dict) else values)

    def inc(self, value):
        self._requires_current_field()
        self.new_obj.setdefault('$inc', {})[self.current_field] = value

        return self

    def max(self, value):
        self._requires_current_field()
        self.new_obj.setdefault('$max', {})[self.current_field] = value

        return self

    def min(self, value):
        self._requires_current_field()
        self.new_obj.setdefault('$min', {})[self.current_field] = value

        return self

    def mul(self, value):
        self._requires_current_field()
        self.new_obj.setdefault('$mul', {})[self.current_field] = value

        return self

    def is_not(self, expression):
        return self.operator('$not', _get_query(expression))

    def not_equals(self, value):
        return self.ne(value)

    def ne(self, value):
        return self.operator('$ne', value)

    def text(self, search, language=None, case_sensitive=False, diacritic_sensitive=False):
        search_expression = {
            '$search': search,
            '$language': language,
            '$caseSensitive': bool(case_sensitive),
            '$diacriticSensitive': bool(diacritic_sensitive),
        }

        if search_expression['$language'] is None:
            # We need to exclude the property to force mongo to use it's default.
            del search_expression['$language']

        return self.operator('$text', search_expression)

    def search(self, search, language=None, case_sensitive=False, diacritic_sensitive=False):
        return self.text(search=search,
                         language=language,
                         case_sensitive=case_sensitive,
                         diacritic_sensitive=diacritic_sensitive)

    def null(self):
        return self.equals(None)

    def not_null(self):
        return self.ne(None)

    def pop_first(self):
        self._requires_current_field()
        self.new_obj.setdefault('$pop', {})[self.current_field] = 1

        return self

    def pop_last(self):
        self._requires_current_field()
        self.new_obj.setdefault('$pop', {})[self.current_field] = -1

        return self

    def position(self, position):
        return self.operator('$position', position)

    def pull(self, value_or_expression):
        self._requires_current_field()
        self.new_obj.setdefault('$pull', {})[self.current_field] = _get_query(value_or_expression)

        return self

    def pull_all(self, values):
        self._requires_current_field()
        self.new_obj.setdefault('$pullAll', {})[self.current_field] = values

        return self

    def push(self, value_or_expression):
        if isinstance(value_or_expression, Expr):
            value_or_expression = value_or_expression.get_query()
            value_or_expression.setdefault('$each', [])

        self._requires_current_field()
        self.new_obj.setdefault('$push', {})[self.current_field] = value_or_expression

        return self

    def push_all(self, values):
        self._requires_current_field()
        self.new_obj.setdefault('$pushAll', {})[self.current_field] = values

        return self

    def range(self, start, end):
        return self.operator('$gte', start).operator('$lt', end)

    def regex(self, pattern):
        return self.operator('$regex', pattern)

    def rename(self, name):
        self._requires_current_field()
        self.new_obj.setdefault('$rename', {})[self.current_field] = name

        return self

    def set(self, value, atomic=True):
        self._requires_current_field()
        if atomic:
            self.new_obj.setdefault('$set', {})[self.current_field] = value
            return self

        if '.' not in self.current_field:
            self.new_obj[self.current_field] = value
            return self

        keys = self.current_field.split('.')
        current = self.new_obj
        for key in keys[:-1]:
            current = current[key]
        current[keys[-1]] = value

        return self

    def set_on_insert(self, value):
        self._requires_current_field()
        self.new_obj.setdefault('$setOnInsert', {})[self.current_field] = value

        return self

    def size(self, size):
        return self.operator('$size', size)

    def slice(self, size):
        return self.operator('$slice', size)

    def sort(self, *sort_criteria):
        if not sort_criteria:
            raise ValueError('This method requires the criteria for the sort.')

        sort_criteria = SON(sort_criteria[0]
                            if isinstance(sort_criteria[0], list)
                            else sort_criteria)

        return self.operator('$sort', sort_criteria)


    def unset_field(self):
        self._requires_current_field()
        self.new_obj.setdefault('$unset', {})[self.current_field] = 1

        return self

    def where(self, javascript):
        self.query['$where'] = javascript

        return self

    def get_query(self):
        return self.query

    def _requires_current_field(self):
        if not self.current_field:
            raise RuntimeError('This method requires you set a current field using field().')

    def _wrap_equality_criteria(self):
        if self.current_field and (self.current_field not in self.query or not self.query[self.current_field]):
            # If the current field has no criteria yet, do nothing.
            # This ensures that we do not inadvertently inject {"$in": null} into the query.
            return

        if self.current_field:
            query = self.query[self.current_field]
        else:
            query = self.query

        if isinstance(query, (dict, SON)) and (not query or query.keys()[0][0] == '$'):
            # Don't do anything if we already have a query dictionary
            # and it's either empty or already a "query" dictionary.
            # We can assume this when "$" is the first character of the first key.
            return

        if self.current_field:
            self.query[self.current_field] = {'$in': [self.query[self.current_field]]}
        else:
            self.query = {'$in': [self.query]}


def _get_query(expr):
    if isinstance(expr, Expr):
        return expr.get_query()
    return expr
