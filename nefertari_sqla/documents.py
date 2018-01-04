import copy
import logging
from collections import defaultdict

import six
from sqlalchemy import text, inspect
from sqlalchemy.orm import (
    class_mapper, object_session, properties, attributes)
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.exc import (
    InvalidRequestError, IntegrityError, DataError)
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound, DetachedInstanceError
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.properties import RelationshipProperty
from pyramid_sqlalchemy import Session, BaseObject
from sqlalchemy_utils.types.json import JSONType
from sqlalchemy.orm.dynamic import AppenderQuery

from nefertari.json_httpexceptions import (
    JHTTPBadRequest, JHTTPNotFound, JHTTPConflict)
from nefertari.utils import (
    process_fields, process_limit, _split, dictset,
    drop_reserved_params)

from nefertari.utils.data import DocumentView
from nefertari.utils import ThreadLocalSingleton
from .signals import ESMetaclass, on_bulk_delete
from .fields import ListField, DictField, IntegerField
from .utils import is_indexable, get_backref_props
from . import types


log = logging.getLogger(__name__)


class SessionHolder(ThreadLocalSingleton):

    def __init__(self):
        # use pyramid_sqlaclhemy default session factory
        self.session_factory = Session

    def set_session_factory(self, factory):
        self.session_factory = factory

    def __call__(self):
        return self.session_factory()

    def restore_default(self):
        from pyramid_sqlalchemy import Session
        self.session_factory = Session


def get_document_cls(name):
    try:
        return BaseObject._decl_class_registry[name]
    except KeyError:
        raise ValueError('SQLAlchemy model `{}` does not exist'.format(name))


def get_document_classes():
    """ Get all defined not abstract document classes

    Class is assumed to be non-abstract if it has `__table__` or
    `__tablename__` attributes defined.
    """
    document_classes = {}
    registry = BaseObject._decl_class_registry
    for model_name, model_cls in registry.items():
        tablename = (getattr(model_cls, '__table__', None) is not None or
                     getattr(model_cls, '__tablename__', None) is not None)
        if tablename:
            document_classes[model_name] = model_cls
    return document_classes


def is_object_document(document):
    return isinstance(document, BaseDocument)


def process_lists(_dict):
    for k in _dict:
        if hasattr(k, 'partition'):
            new_k, _, _t = k.partition('__')
            if _t == 'in' or _t == 'all':
                _dict[k] = _dict.aslist(k)
    return _dict


def process_bools(_dict):
    for k in _dict:
        if hasattr(k, 'partition'):
            new_k, _, _t = k.partition('__')
            if _t == 'bool':
                _dict[new_k] = _dict.pop_bool_param(k)
    return _dict


TYPES_MAP = {
    types.LimitedString: {'type': 'string'},
    types.LimitedText: {'type': 'string'},
    types.LimitedUnicode: {'type': 'string'},
    types.LimitedUnicodeText: {'type': 'string'},
    types.Choice: {'type': 'string'},

    types.Boolean: {'type': 'boolean'},
    types.LargeBinary: {'type': 'object'},
    JSONType: {'type': 'object', 'enabled': False},

    types.LimitedNumeric: {'type': 'double'},
    types.LimitedFloat: {'type': 'double'},

    types.LimitedInteger: {'type': 'long'},
    types.LimitedBigInteger: {'type': 'long'},
    types.LimitedSmallInteger: {'type': 'long'},
    types.Interval: {'type': 'long'},

    types.DateTime: {'type': 'date', 'format': 'dateOptionalTime'},
    types.Date: {'type': 'date', 'format': 'dateOptionalTime'},
    types.Time: {'type': 'date', 'format': 'HH:mm:ss'},
}


class BaseMixin(object):
    """ Represents mixin class for models.

    Attributes:
        _auth_fields: String names of fields meant to be displayed to
            authenticated users.
        _public_fields: String names of fields meant to be displayed to
            non-authenticated users.
        _hidden_fields: String names of fields meant to be hidden but editable.
        _nested_relationships: String names of relationship fields
            that should be included in JSON data of an object as full
            included documents. If relationship field is not
            present in this list, this field's value in JSON will be an
            object's ID or list of IDs.
        _nesting_depth: Depth of relationship field nesting in JSON.
            Defaults to 1(one) which makes only one level of relationship
            nested.
    """
    _public_fields = None
    _auth_fields = None
    _hidden_fields = None
    _nested_relationships = ()
    _nesting_depth = 1

    session_factory = SessionHolder()

    _type = property(lambda self: self.__class__.__name__)

    @classmethod
    def get_es_mapping(cls, _depth=None, types_map=None):
        """ Generate ES mapping from model schema. """
        from nefertari.elasticsearch import ES

        if types_map is None:
            types_map = TYPES_MAP

        if _depth is None:
            _depth = cls._nesting_depth

        depth_reached = _depth <= 0

        nested_substitutions = []
        properties = {}
        mapping = {
            ES.src2type(cls.__name__): {
                'properties': properties
            }
        }
        mapper = class_mapper(cls)
        columns = {c.name: c for c in mapper.columns}
        relationships = {r.key: r for r in mapper.relationships}
        for name, column in columns.items():
            column_type = column.type
            if isinstance(column_type, types.ChoiceArray):
                column_type = column_type.impl.item_type
            column_type = type(column_type)

            if column_type not in types_map:
                continue

            if getattr(column, '_custom_analyzer', False):
                properties[name] = {'analyzer': column._custom_analyzer}
                properties[name].update(types_map[column_type])
                continue

            properties[name] = types_map[column_type]

            if hasattr(column, "_es_multi_field") and getattr(column, "_es_multi_field"):
                multi_fields = getattr(column, "_es_multi_field")
                properties[name] = properties[name].copy()
                properties[name]["fields"] = {}

                for multi_field_name in multi_fields:
                    properties[name]["fields"][multi_field_name] = multi_fields[multi_field_name].copy()
                    properties[name]["fields"][multi_field_name].update(types_map[column_type])

        for name, column in relationships.items():

            if name in cls._nested_relationships and not depth_reached:
                column_type = {'type': 'nested', 'include_in_parent': True}
                nested_substitutions.append(name)

                submapping, sub_substitutions = column.mapper.class_.get_es_mapping(
                    _depth=_depth - 1)

                column_type.update(list(submapping.values())[0])
                properties[name + "_nested"] = column_type

            rel_pk_field = column.mapper.class_.pk_field_type()
            column_type = types_map[rel_pk_field]
            properties[name] = column_type

        properties['_pk'] = {'type': 'string'}
        return mapping, nested_substitutions

    @classmethod
    def autogenerate_for(cls, model, set_to):
        """ Setup `after_insert` event handler.

        Event handler is registered for class :model: and creates a new
        instance of :cls: with a field :set_to: set to an instance on
        which the event occured.
        """
        from sqlalchemy import event

        def generate(mapper, connection, target):
            cls(**{set_to: target})

        event.listen(model, 'after_insert', generate)

    @classmethod
    def pk_field(cls):
        """ Get a primary key field name. """
        return class_mapper(cls).primary_key[0].name

    @classmethod
    def pk_field_type(cls):
        return class_mapper(cls).primary_key[0].type.__class__

    @classmethod
    def check_fields_allowed(cls, fields):
        """ Check if `fields` are allowed to be used on this model. """
        fields = [f.split('__')[0] for f in fields]
        fields_to_query = set(cls.fields_to_query())
        if not set(fields).issubset(fields_to_query):
            not_allowed = set(fields) - fields_to_query
            raise JHTTPBadRequest(
                "'%s' object does not have fields: %s" % (
                    cls.__name__, ', '.join(not_allowed)))

    @classmethod
    def filter_fields(cls, params):
        """ Filter out fields with invalid names. """
        fields = cls.fields_to_query()
        return dictset({
                           name: val for name, val in params.items()
                           if name.split('__')[0] in fields
                           })

    @classmethod
    def apply_fields(cls, query_set, _fields):
        """ Apply fields' restrictions to `query_set`.

        First, fields are split to fields that should only be included and
        fields that should be excluded. Then excluded fields are removed
        from included fields.
        """
        fields_only, fields_exclude = process_fields(_fields)
        if not (fields_only or fields_exclude):
            return query_set
        try:
            fields_only = fields_only or cls.native_fields()
            fields_exclude = fields_exclude or []
            if fields_exclude:
                # Remove fields_exclude from fields_only
                fields_only = [
                    f for f in fields_only if f not in fields_exclude]
            if fields_only:
                # Add PK field
                fields_only.append(cls.pk_field())
                fields_only = [
                    getattr(cls, f) for f in sorted(set(fields_only))]
                query_set = query_set.with_entities(*fields_only)

        except InvalidRequestError as e:
            raise JHTTPBadRequest('Bad _fields param: %s ' % e)

        return query_set

    @classmethod
    def apply_sort(cls, query_set, _sort):
        if not _sort:
            return query_set
        sorting_fields = []
        for field in _sort:
            if field.startswith('-'):
                sorting_fields.append(getattr(cls, field[1:]).desc())
            else:
                sorting_fields.append(getattr(cls, field))
        return query_set.order_by(*sorting_fields)

    @classmethod
    def count(cls, query_set):
        return query_set.count()

    @classmethod
    def filter_objects(cls, objects, first=False, **params):
        """ Perform query with :params: on instances sequence :objects:

        :param object: Sequence of :cls: instances on which query should be run.
        :param params: Query parameters to filter :objects:.
        """
        id_name = cls.pk_field()
        ids = [getattr(obj, id_name, None) for obj in objects]
        ids = [str(id_) for id_ in ids if id_ is not None]
        field_obj = getattr(cls, id_name)

        if not ids:
            if first:
                return None
            return []

        query_set = cls.session_factory().query(cls).filter(field_obj.in_(ids))

        if params:
            params['query_set'] = query_set.from_self()
            query_set = cls.get_collection(**params)

        if first:
            first_obj = query_set.first()
            if not first_obj:
                msg = "'{}({})' resource not found".format(
                    cls.__name__, params)
                raise JHTTPNotFound(msg)
            return first_obj

        return query_set

    @classmethod
    def _pop_iterables(cls, params):
        """ Pop iterable fields' parameters from :params: and generate
        SQLA expressions to query the database.

        Iterable values are found by checking which keys from :params:
        correspond to names of List fields on model.
        If ListField uses the `postgresql.ARRAY` type, the value is
        wrapped in a list.
        """
        iterables = {}
        columns = class_mapper(cls).columns
        columns = {c.name: c for c in columns
                   if isinstance(c, (ListField, DictField))}

        for key, val in params.items():
            col = columns.get(key)
            if col is None:
                continue

            field_obj = getattr(cls, key)
            is_postgres = getattr(col.type, 'is_postgresql', False)

            if isinstance(col, ListField):
                val = [val] if is_postgres else val
                expr = field_obj.contains(val)

            if isinstance(col, DictField):
                raise Exception('DictField database querying is not '
                                'supported')

            iterables[key] = expr

        for key in iterables.keys():
            params.pop(key)

        return list(iterables.values()), params

    @classmethod
    def get_collection(cls, **params):
        """ Query collection and return results.

        Notes:
        *   Before validating that only model fields are present in params,
            reserved params, query params and all params starting with
            double underscore are dropped.
        *   Params which have value "_all" are dropped.
        *   When ``_count`` param is used, objects count is returned
            before applying offset and limit.

        :param bool _strict: If True ``params`` are validated to contain
            only fields defined on model, exception is raised if invalid
            fields are present. When False - invalid fields are dropped.
            Defaults to ``True``.
        :param bool _item_request: Indicates whether it is a single item
            request or not. When True and DataError happens on DB request,
            JHTTPNotFound is raised. JHTTPBadRequest is raised when False.
            Defaults to ``False``.
        :param list _sort: Field names to sort results by. If field name
            is prefixed with "-" it is used for "descending" sorting.
            Otherwise "ascending" sorting is performed by that field.
            Defaults to an empty list in which case sorting is not
            performed.
        :param list _fields: Names of fields which should be included
            or excluded from results. Fields to excluded should be
            prefixed with "-". Defaults to an empty list in which
            case all fields are returned.
        :param int _limit: Number of results per page. Defaults
            to None in which case all results are returned.
        :param int _page: Number of page. In conjunction with
            ``_limit`` is used to calculate results offset. Defaults to
            None in which case it is ignored. Params ``_page`` and
            ``_start` are mutually exclusive.
        :param int _start: Results offset. If provided ``_limit`` and
            ``_page`` params are ignored when calculating offset. Defaults
            to None. Params ``_page`` and ``_start`` are mutually
            exclusive. If not offset-related params are provided, offset
            equals to 0.
        :param Query query_set: Existing queryset. If provided, all queries
            are applied to it instead of creating new queryset. Defaults
            to None.
        :param _count: When provided, only results number is returned as
            integer.
        :param _explain: When provided, query performed(SQL) is returned
            as a string instead of query results.
        :param bool _raise_on_empty: When True JHTTPNotFound is raised
            if query returned no results. Defaults to False in which case
            error is just logged and empty query results are returned.

        :returns: Query results as ``sqlalchemy.orm.query.Query`` instance.
            May be sorted, offset, limited.
        :returns: Dict of {'field_name': fieldval}, when ``_fields`` param
            is provided.
        :returns: Number of query results as an int when ``_count`` param
            is provided.
        :returns: String representing query ran when ``_explain`` param
            is provided.

        :raises JHTTPNotFound: When ``_raise_on_empty=True`` and no
            results found.
        :raises JHTTPNotFound: When ``_item_request=True`` and
            ``sqlalchemy.exc.DataError`` exception is raised during DB
            query. Latter exception is raised when querying DB with
            an identifier of a wrong type. E.g. when querying Int field
            with a string.
        :raises JHTTPBadRequest: When ``_item_request=False`` and
            ``sqlalchemy.exc.DataError`` exception is raised during DB
            query.
        :raises JHTTPBadRequest: When ``sqlalchemy.exc.InvalidRequestError``
            or ``sqlalchemy.exc.IntegrityError`` errors happen during DB
            query.
        """
        log.debug('Get collection: {}, {}'.format(cls.__name__, params))
        params.pop('__confirmation', False)
        _strict = params.pop('_strict', True)
        _item_request = params.pop('_item_request', False)

        _sort = _split(params.pop('_sort', []))
        _fields = _split(params.pop('_fields', []))
        _limit = params.pop('_limit', None)
        _page = params.pop('_page', None)
        _start = params.pop('_start', None)
        query_set = params.pop('query_set', None)

        _count = '_count' in params
        params.pop('_count', None)
        _explain = '_explain' in params
        params.pop('_explain', None)
        _raise_on_empty = params.pop('_raise_on_empty', False)

        if query_set is None:
            query_set = cls.session_factory().query(cls)

        # Remove any __ legacy instructions from this point on
        params = dictset({
                             key: val for key, val in params.items()
                             if not key.startswith('__')
                             })

        iterables_exprs, params = cls._pop_iterables(params)

        params = drop_reserved_params(params)
        if _strict:
            _check_fields = [
                f.strip('-+') for f in list(params.keys()) + _fields + _sort]
            cls.check_fields_allowed(_check_fields)
        else:
            params = cls.filter_fields(params)

        process_lists(params)
        process_bools(params)

        # If param is _all then remove it
        params.pop_by_values('_all')

        try:

            query_set = query_set.filter_by(**params)

            # Apply filtering by iterable expressions
            for expr in iterables_exprs:
                query_set = query_set.from_self().filter(expr)

            _total = query_set.count()
            if _count:
                return _total

            # Filtering by fields has to be the first thing to do on
            # the query_set!
            query_set = cls.apply_fields(query_set, _fields)
            query_set = cls.apply_sort(query_set, _sort)

            if _limit is not None:
                _start, _limit = process_limit(_start, _page, _limit)
                query_set = query_set.offset(_start).limit(_limit)

            if not query_set.count():
                msg = "'%s(%s)' resource not found" % (cls.__name__, params)
                if _raise_on_empty:
                    raise JHTTPNotFound(msg)
                else:
                    log.debug(msg)

        except DataError as ex:
            if _item_request:
                msg = "'{}({})' resource not found".format(
                    cls.__name__, params)
                raise JHTTPNotFound(msg, explanation=ex.message)
            else:
                raise JHTTPBadRequest(str(ex), extra={'data': ex})
        except (InvalidRequestError,) as ex:
            raise JHTTPBadRequest(str(ex), extra={'data': ex})

        query_sql = str(query_set).replace('\n', '')
        if _explain:
            return query_sql

        log.debug('get_collection.query_set: %s (%s)', cls.__name__, query_sql)

        if _fields:
            query_set = cls.add_field_names(query_set, _fields)

        query_set._nefertari_meta = dict(
            total=_total,
            start=_start,
            fields=_fields)
        return query_set

    @classmethod
    def add_field_names(cls, query_set, requested_fields):
        """ Convert list of tuples to dict with proper field keys. """
        from .utils import FieldsQuerySet
        fields = [col['name'] for col in query_set.column_descriptions] + [
            '_type']
        add_vals = (cls.__name__,)
        pk_field = cls.pk_field()

        def _convert(val):
            return dict(zip(fields, val + add_vals))

        def _add_pk(obj):
            if pk_field in obj:
                obj['_pk'] = obj[pk_field]
                if pk_field not in requested_fields:
                    obj.pop(pk_field)
            return obj

        values = query_set.all()
        converted = [_add_pk(_convert(val)) for val in values]
        return FieldsQuerySet(converted)

    @classmethod
    def has_field(cls, field):
        return field in cls.native_fields()

    @classmethod
    def native_fields(cls):
        columns = list(cls._mapped_columns().keys())
        relationships = list(cls._mapped_relationships().keys())
        return columns + relationships

    @classmethod
    def _mapped_columns(cls):
        return {c.name: c for c in class_mapper(cls).columns}

    @classmethod
    def _mapped_relationships(cls):
        return {c.key: c for c in class_mapper(cls).relationships}

    @classmethod
    def fields_to_query(cls):
        query_fields = [
            'id', '_limit', '_page', '_sort', '_fields', '_count', '_start']
        return list(set(query_fields + cls.native_fields()))

    @classmethod
    def get_item(cls, **params):
        """ Get single item and raise exception if not found.

        Exception raising when item is not found can be disabled
        by passing ``_raise_on_empty=False`` in params.

        :returns: Single collection item as an instance of ``cls``.
        """
        params.setdefault('_raise_on_empty', True)
        params['_limit'] = 1
        params['_item_request'] = True
        query_set = cls.get_collection(**params)
        return query_set.first()

    def unique_fields(self):
        native_fields = class_mapper(self.__class__).columns
        return [f for f in native_fields if f.unique or f.primary_key]

    @classmethod
    def get_or_create(cls, **params):
        defaults = params.pop('defaults', {})
        _limit = params.pop('_limit', 1)
        query_set = cls.get_collection(_limit=_limit, **params)
        try:
            obj = query_set.one()
            return obj, False
        except NoResultFound:
            defaults.update(params)
            new_obj = cls(**defaults)
            new_obj.save()
            return new_obj, True
        except MultipleResultsFound:
            raise JHTTPBadRequest('Bad or Insufficient Params')

    def _update(self, params, **kw):
        process_bools(params)
        self.check_fields_allowed(list(params.keys()))
        columns = {c.name: c for c in class_mapper(self.__class__).columns}
        iter_columns = set(
            k for k, v in columns.items()
            if isinstance(v, (DictField, ListField)))
        pk_field = self.pk_field()

        for key, new_value in params.items():
            # Can't change PK field
            if key == pk_field:
                continue
            if key in iter_columns:
                self.update_iterables(new_value, key, save=False)
            else:
                setattr(self, key, new_value)
        return self

    @classmethod
    def _delete_many(cls, items, synchronize_session=False):
        """ Delete :items: queryset or objects list.

        When queryset passed, Query.delete() is used to delete it but
        first queryset is re-queried to clean it from explicit
        limit/offset/etc.

        If some of the methods listed above were called, or :items: is not
        a Query instance, one-by-one items update is performed.

        `on_bulk_delete` function is called to delete objects from index
        and to reindex relationships. This is done explicitly because it is
        impossible to get access to deleted objects in signal handler for
        'after_bulk_delete' ORM event.
        """
        if isinstance(items, Query):
            del_queryset = cls._clean_queryset(items)
            del_items = del_queryset.all()
            del_count = del_queryset.delete(
                synchronize_session=synchronize_session)
            on_bulk_delete(cls, del_items)
            return del_count
        items_count = len(items)
        session = cls.session_factory()
        for item in items:
            session.delete(item)
        session.flush()
        return items_count

    @classmethod
    def bulk_expire_parents(cls, items, session=None):
        if session is None:
            session = cls.session_factory()
        for item in items:
            item.expire_parents(session)

    @classmethod
    def _update_many(cls, items, params, synchronize_session='fetch', return_documents=True):
        """ Update :items: queryset or objects list.

        When queryset passed, Query.update() is used to update it but
        first queryset is re-queried to clean it from explicit
        limit/offset/etc.

        If some of the methods listed above were called, or :items: is not
        a Query instance, one-by-one items update is performed.
        """
        if isinstance(items, Query):
            upd_queryset = cls._clean_queryset(items)
            items_count = upd_queryset.update(
                params, synchronize_session=synchronize_session)
            updated_items = upd_queryset.all() if return_documents is True else []
        else:
            if cls.is_update_batchable(params):
                pk_field = cls.pk_field()

                pks = []
                for item in items:
                    pks.append(getattr(item, pk_field))
                if pks:
                    primary_key = getattr(cls, pk_field)
                    upd_queryset = cls.session_factory().query(cls).filter(primary_key.in_(pks))
                    items_count = upd_queryset.update(params, synchronize_session=synchronize_session)
                    updated_items = upd_queryset.all() if return_documents is True else []
                else:
                    items_count = 0
                    updated_items = []
            else:
                items_count = len(items)

                for item in items:
                    item.update(params)

                updated_items = items if return_documents is True else []

        return {"count": items_count, "items": updated_items}

    @classmethod
    def is_update_batchable(cls, params):
        """
        Determines whether the update_many is batchable
        """
        is_batchable = True
        # If params doesn't contain iterable values, we can batch update it
        for key, value in params.copy().items():
            if isinstance(value, (list, dict, BaseDocument)):
                is_batchable = False
                break
            # None can be assigned to a ForeignKey column
            elif value is None:
                attribute = getattr(cls, key, None)
                if attribute and isinstance(attribute.property, RelationshipProperty):
                    # Assigning the value to the foreignkey column rather than to the backref property
                    [column] = attribute.property.local_columns
                    del params[key]
                    params[column.key] = value
        return is_batchable

    @classmethod
    def _clean_queryset(cls, queryset):
        """ Clean :queryset: from explicit limit, offset, etc.

        New queryset is created by querying collection by IDs from
        passed queryset.
        """
        pk_field = getattr(cls, cls.pk_field())
        pks_query = queryset.with_entities(pk_field)
        return queryset.session.query(cls).filter(
            pk_field.in_(pks_query))

    def __repr__(self):
        pk_field = self.pk_field()
        try:
            pk = getattr(self, pk_field)
        except DetachedInstanceError:
            pk = '`detached`'
        parts = [
            '{}={}'.format(pk_field, pk),
        ]
        return '<{}: {}>'.format(self.__class__.__name__, ', '.join(parts))

    @classmethod
    def get_by_ids(cls, ids, **params):
        if not ids:
            return []
        query_set = cls.get_collection(**params)
        cls_id = getattr(cls, cls.pk_field())
        return query_set.from_self().filter(cls_id.in_(ids)).limit(len(ids))

    @classmethod
    def get_null_values(cls):
        """ Get null values of :cls: fields. """
        skip_fields = set(['_acl'])
        null_values = {}
        columns = cls._mapped_columns()
        columns.update(cls._mapped_relationships())
        for name, col in columns.items():
            if name in skip_fields:
                continue
            if isinstance(col, RelationshipProperty) and col.uselist:
                value = []
            else:
                value = None
            null_values[name] = value
        return null_values

    @staticmethod
    def get_encoded_field_value(value, _depth, nest_objects=False):
        if nest_objects:
            encoder = lambda v: v.to_dict(_depth=_depth - 1)
        else:
            encoder = lambda v: getattr(v, v.pk_field(), None)

        if isinstance(value, BaseMixin):
            value = encoder(value)
        elif isinstance(value, InstrumentedList):
            value = [encoder(val) for val in value]
        elif isinstance(value,  AppenderQuery):
            gen = value.values('id')
            value = [item[0] for item in gen]
        elif hasattr(value, 'to_dict'):
            value = value.to_dict(_depth=_depth - 1)

        return value

    @classmethod
    def get_non_indexable_fields(cls):
        backref_props = get_backref_props(cls)

        def filter_func(f):
            return not is_indexable(f)

        fields = [column.name for column in filter(filter_func, class_mapper(cls).columns)]
        fields.extend([prop.key for prop in filter(filter_func, backref_props)])

        return fields

    def get_reverse_non_indexable_fields(self):
        fields = []
        related_items = [(getattr(self, item.key), item) for item in get_backref_props(self.__class__)]

        for related_item, relation in related_items:
            if isinstance(related_item, list):
                for item in related_item:
                    if relation.back_populates in item.get_non_indexable_fields():
                        fields.append(relation.key)
                        break
            else:
                if isinstance(related_item, AppenderQuery):
                    related_item = related_item.column_descriptions[0]['type']

                if not related_item:
                    continue

                if relation.back_populates in related_item.get_non_indexable_fields():
                    fields.append(relation.key)
        return fields

    def to_dict(self, **kwargs):
        _depth = kwargs.get('_depth')
        _obj_type = kwargs.get('type', dictset)
        indexable = kwargs.get('indexable', False)

        if _depth is None:
            _depth = self._nesting_depth
        depth_reached = _depth is not None and _depth <= 0

        _data = _obj_type()

        native_fields = set(self.__class__.native_fields()) - set(self.get_non_indexable_fields())

        for field in native_fields:
            value = getattr(self, field, None)

            is_nested = field in self._nested_relationships

            # This is where we expand nested backrefs into a "name_nested"->Object and "name"->Pk indexed field
            if indexable and is_nested and not depth_reached:
                # Recursive step
                _data[field + "_nested"] = BaseMixin.get_encoded_field_value(value, _depth, nest_objects=True)

            # Recursive step
            _data[field] = BaseMixin.get_encoded_field_value(
                value,
                _depth,
                nest_objects=(not indexable and is_nested and not depth_reached)
            )

        if hasattr(self, '_injected_fields'):
            for field in self._injected_fields:
                _data[field] = getattr(self, field, None)

        _data['_type'] = self._type
        _data['_pk'] = str(getattr(self, self.pk_field()))

        return _data

    def to_indexable_dict(self, **kwargs):
        kwargs["indexable"] = True
        return self.to_dict(**kwargs)

    def get_view(self, **kwargs):
        kwargs["_depth"] = 0
        kwargs["type"] = DocumentView
        return self.to_dict(**kwargs)

    def refresh(self):
        session = object_session(self) or self.session_factory()

        if self not in session:
            session.add(self)

    def update_iterables(self, params, attr, save=True):
        mapper = class_mapper(self.__class__)
        columns = {c.name: c for c in mapper.columns}
        is_dict = isinstance(columns.get(attr), DictField)
        is_list = isinstance(columns.get(attr), ListField)

        def split_keys(keys):
            neg_keys, pos_keys = [], []

            for key in keys:
                if key.startswith('__'):
                    continue
                if key.startswith('-'):
                    neg_keys.append(key[1:])
                else:
                    pos_keys.append(key.strip())
            return pos_keys, neg_keys

        def update_dict(update_params):
            final_value = getattr(self, attr, {}) or {}
            final_value = final_value.copy()
            if update_params is None or update_params == '':
                if not final_value:
                    return
                update_params = {
                    '-' + key: val for key, val in final_value.items()}
            positive, negative = split_keys(list(update_params.keys()))

            # Pop negative keys
            for key in negative:
                final_value.pop(key, None)

            # Set positive keys
            for key in positive:
                final_value[str(key)] = update_params[key]

            setattr(self, attr, final_value)
            if save:
                self.save()

        def update_list(update_params):
            final_value = getattr(self, attr) or []
            final_value = copy.deepcopy(final_value)

            if update_params is None or update_params == '':
                if not final_value:
                    return
                update_params = ['-' + val for val in final_value]
            if isinstance(update_params, dict):
                keys = list(update_params.keys())
            else:
                keys = update_params

            positives, negatives = split_keys(keys)

            if not positives and not negatives:
                raise JHTTPBadRequest('Missing params')

            parameters = {}
            cls_name = self.__class__.__name__
            location_dot_notation = "\""+cls_name.lower()+"\"" + "." + attr
            sql_expression = location_dot_notation

            for index, negative in enumerate(negatives):
                index_str = str(index)

                if sql_expression == location_dot_notation:
                    sql_expression = "array_remove(" + location_dot_notation + ", :negative_" + index_str + ")"
                else:
                    sql_expression = "array_remove(" + sql_expression + ", :negative_" + index_str + ")"

                parameters["negative_" + index_str] = negative

            if len(positives) > 0:
                sql_expression = "array_cat(" + sql_expression + ", :added_items)"
                parameters["added_items"] = list(set(positives) - set(final_value))

            expression = text(sql_expression).bindparams(**parameters)
            setattr(self, attr, expression)

            object_session(self).flush()

            if save:
                self.save()

        if is_dict:
            update_dict(params)
        elif is_list:
            update_list(params)

    def get_related_documents(self, nested_only=False):
        """ Return pairs of (Model, istances) of relationship fields.

        Pair contains of two elements:
          :Model: Model class object(s) contained in field.
          :instances: Model class instance(s) contained in field

        :param nested_only: Boolean, defaults to False. When True, return
            results only contain data for models on which current model
            and field are nested.
        """
        backref_props = get_backref_props(self.__class__)
        non_indexable_fields = self.get_reverse_non_indexable_fields()

        for prop in backref_props:
            value = getattr(self, prop.key)

            if prop.key in non_indexable_fields:
                continue

            # Do not index empty values
            if not value:
                continue

            # we don't use appender query as _nested_relationships
            if isinstance(value, AppenderQuery):
                continue

            if not isinstance(value, list):
                value = [value]
            model_cls = value[0].__class__

            if nested_only:
                backref = prop.back_populates

                if backref and backref not in model_cls._nested_relationships:
                    continue

            yield (model_cls, value)

    def get_parent_documents(self, nested_only=False):
        backref_props = get_backref_props(self.__class__)

        non_indexable_fields = [field for field in self.get_non_indexable_fields()]

        for prop in backref_props:
            value = getattr(self, prop.key)

            if prop.key in non_indexable_fields:
                continue

            # Backref don't have _init_kwargs
            if hasattr(prop, "_init_kwargs"):
                continue

            # Backrefs don't have backrefs
            if prop.backref is not None:
                continue

            # Backrefs only have one related item
            if prop.uselist:
                continue

            # Do not index empty values
            if not value:
                continue

            model_cls = value.__class__

            if nested_only:
                backref = prop.back_populates

                if backref and backref not in model_cls._nested_relationships:
                    continue

            yield (value, prop.back_populates)

    def _is_modified(self):
        """ Determine if instance is modified.

        For instance to be marked as 'modified', it should:
          * Have state marked as modified
          * Have state marked as persistent
          * Any of modified fields have new value
        """
        state = attributes.instance_state(self)
        if state.persistent and state.modified:
            for field in state.committed_state.keys():
                history = state.get_history(field, self)
                if history.added or history.deleted:
                    return True

    def _is_created(self):
        state = attributes.instance_state(self)
        return not state.persistent

    @classmethod
    def get_readonly_item(cls, pkey_value=None, **params):
        columns = cls.get_columns()

        if pkey_value:
            return cls._wrap_read_only(
                cls.session_factory().query(cls).filter_by(**{cls.pk_field(): pkey_value}).values(*columns)
            )
        if params:
            return cls._wrap_read_only(
                cls.session_factory().query(cls).filter_by(**params).values(*columns)
            )
        raise TypeError('Missing one of required arguments: pkey_value or **params')

    @classmethod
    def get_columns(cls):
        inspected = inspect(cls)
        return [c_attr.key for c_attr in inspected.mapper.columns]

    @classmethod
    def _wrap_read_only(cls, gen):
        read_only_cls = cls._get_readonly_cls()
        single_value = next(gen, None)

        if single_value:
            return read_only_cls(item=single_value)
        return None

    @classmethod
    def get_readonly_collection(cls, **params):
        if params:
            query = cls.session_factory().query(cls).filter_by(**params)
        else:
            statement = text('SELECT * FROM public.%s' % cls.__tablename__)
            query = cls.session_factory().query(cls).from_statement(statement)
        return cls._wrap_read_only_collection(query.values(*cls.get_columns()))

    # proxies generator
    @classmethod
    def _wrap_read_only_collection(cls, gen):
        read_only_cls = cls._get_readonly_cls()
        for item in gen:
            yield read_only_cls(**{column: item[index] for index, column in enumerate(cls.get_columns())})

    @classmethod
    def _get_readonly_cls(cls):
        def _build(item=None, **kwargs):
            return ReadOnlyObject(cls.pk_field(), cls.pk_field_type(), item=item, **kwargs)
        return _build

    @classmethod
    def to_readonly_instance(cls, instance):
        readonly_cls = cls._get_readonly_cls()
        return readonly_cls(**instance.to_dict())


class ReadOnlyObject:
    def __init__(self, pk_field, pk_field_type, item=None, **kwargs):
        self._pk_field = pk_field
        self._pk_field_type = pk_field_type

        if item:
            keys = item.keys()
            for index, value in enumerate(item):
                self.__dict__[keys[index]] = value

        else:
            self.__dict__.update(kwargs)

    def pk_field(self):
        return self._pk_field

    def pk_field_type(self):
        return self._pk_field_type


class BaseDocument(BaseObject, BaseMixin):
    """ Base class for SQLA models.

    Subclasses of this class that do not define a model schema
    should be abstract as well (__abstract__ = True).
    """
    __abstract__ = True
    _watchers = defaultdict(list)
    _sort_methods = {}


    def save(self):
        session = object_session(self)
        session = session or self.session_factory()
        try:
            session.add(self)
            session.flush()
            session.expire(self)
            return self
        except (IntegrityError,) as e:
            if 'duplicate' not in e.args[0]:
                raise  # Other error, not duplicate

            raise JHTTPConflict(
                detail='Resource `{}` already exists.'.format(
                    self.__class__.__name__),
                extra={'data': e})

    def update(self, params):
        try:
            self._update(params)
            session = object_session(self)
            session.add(self)
            session.flush()
            self.watch_for_updates(params.keys())
            return self
        except (IntegrityError,) as e:
            if 'duplicate' not in e.args[0]:
                raise  # other error, not duplicate

            raise JHTTPConflict(
                detail='Resource `{}` already exists.'.format(
                    self.__class__.__name__),
                extra={'data': e})

    def delete(self):
        session = object_session(self)
        session.delete(self)

    def expire_parents(self, session=None):
        if session is None:
            session = object_session(self)
        for document, backref_name in self.get_parent_documents():
            session.expire(document, [backref_name])

    def watch_for_updates(self, fields):
        for field in fields:
            if field in self._watchers:
                try:
                    update_event = {'pk_value': getattr(self, self.pk_field()),
                                   'new_value': getattr(self, field)}

                    callbacks = self._watchers[field]

                    for callback in callbacks:
                        callback(update_event)

                except Exception as e:
                    log.error('Exception {} caused inside {}'.format(e, self._watchers[field]))
                    pass

    @classmethod
    def get_field_params(cls, field_name):
        """ Get init params of column named :field_name:. """
        columns = cls._mapped_columns()
        columns.update(cls._mapped_relationships())
        column = columns.get(field_name)
        return getattr(column, '_init_kwargs', None)

    @classmethod
    def add_field_watcher(cls, field_name, callback):
        cls._watchers[field_name].append(callback)

    @classmethod
    def add_sort_method(cls, sort_name, method):
        cls._sort_methods[sort_name] = method

    @classmethod
    def get_sort_method(cls, sort_name):
        return cls._sort_methods.get(sort_name)


class ESBaseDocument(six.with_metaclass(ESMetaclass, BaseDocument)):
    """ Base class for SQLA models that use Elasticsearch.

    Subclasses of this class that do not define a model schema
    should be abstract as well (__abstract__ = True).
    """
    __abstract__ = True


def reload_document(_type, _id):
    document_cls = get_document_cls(_type)
    try:
        document = document_cls.get_item(**{document_cls.pk_field(): _id})
        return document.to_indexable_dict()
    except NoResultFound:
        return None
