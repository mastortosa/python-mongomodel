import re
import urllib

from mongomodel import fields
from mongomodel.db import Client
from mongomodel.utils import get_sort_list, format_update


_connections = {}


def connect(db_name, **kwargs):
    global _connections
    if db_name in _connections:
        db = _connections[db_name][db_name]
    else:
        host = kwargs.get('host', 'localhost')
        if kwargs.get('port'):
            host = '%s:%s' % (host, kwargs['port'])
        if kwargs.get('user'):
            host = 'mongodb://%s:%s@%s' % (
                kwargs['user'],
                urllib.quote_plus(kwargs['password']),
                host)
        _connections[db_name] = Client(host)
        db = _connections[db_name][db_name]
    return db


class ModelMeta(type):

    def __new__(cls, name, bases, attrs):
        meta = attrs.pop('Meta', type('Meta', (object,), {}))
        super_meta_list = [i._meta for i in bases if hasattr(i, '_meta')]

        # All the fields as defined in the model attributes will be stored in
        # self._meta.fields{dict}. Each instance of the model will have a copy
        # of each field in instance._fields as well as a reference as an attr.
        # Parent fields.
        meta.fields = {}
        for i in super_meta_list:
            meta.fields.update(getattr(i, 'fields', {}))
        # Class fields.
        meta.fields.update(dict((k, v) for k, v in attrs.items()
                           if isinstance(v, fields.Field)))
        # Set fields names.
        for k, v in meta.fields.items():
            v.name = k

        # Set if is embedded (inherit). Any subclass of Model will be
        # _embedded = False and any subclass of Document but Model will be
        # _embedded = True.
        if not hasattr(meta, '_embedded'):
            for i in super_meta_list:
                if hasattr(i, '_embedded'):
                    meta._embedded = getattr(i, '_embedded')
                    break

        new_class = super(ModelMeta, cls).__new__(cls, name, bases, attrs)

        # Get database.
        if not meta._embedded and not getattr(meta, 'abstract', False):
            # TODO: shold be `database` inheritable?
            meta.database = next((i.database for i in super_meta_list
                                  if hasattr(i, 'database')),
                                 getattr(meta, 'database', None))
            if not meta.database:
                raise ValueError('database not found in model Meta.')

            meta.database_attrs = next((i.database_attrs
                                        for i in super_meta_list
                                        if hasattr(i, 'database_attrs')),
                                       {})

            # Get collection or create from model name.
            if not hasattr(meta, 'collection'):
                pattern, replace = r'([a-z0-9])([A-Z])', r'\1_\2'
                meta.collection = re.sub(pattern, replace, name + 's').lower()

            # Deal with collection connection in the meta to reduce attemps to
            # get the connection instance. Eg:
            # >>> doc1 = Model()
            # >>> doc1._meta.collection_connection = Connection()
            # >>> doc2 = Model()
            # >>> doc1._meta.collection_connection == \
            #     doc2._meta.collection_connection
            # True
            # >>> doc1 = Model.get(name='foo')
            # >>> doc2 = Model(name='bar')
            # >>> doc2._meta.collection_connection is None
            # False
            meta.collection_connection = None

            # Get ordering (inheritable).
            ordering = getattr(meta,
                               'ordering',
                               next((i.ordering for i in super_meta_list
                                     if hasattr(i, 'ordering')),
                                    None))
            if ordering:
                meta.ordering = ordering
                new_class._sort = get_sort_list(ordering)
            else:
                new_class._sort = None

        new_class._meta = meta
        return new_class


class Document(object):
    """
    Basic data model. Does not contain any database operation. It can be
    extended to use as a model in a EmbeddedDocumentField.
    """

    __metaclass__ = ModelMeta
    _changed = False

    class Meta:
        _embedded = True

    def __init__(self, _validate_required=True, **kwargs):
        self._validate_required = _validate_required
        field_names = self._meta.fields.keys()
        if not _validate_required:
            field_names = set(field_names).intersection(kwargs.keys())
        try:
            self._data = dict((i, self._meta.fields[i].default)
                              for i in field_names)
        except KeyError:
            raise ValueError('%s is not a valid field' % i)

        for k, v in kwargs.items():
            setattr(self, k, v)

    def __setitem__(self, attr, value):
        """
        Assign attribute assignment to item assignment to support cursor
        decoding (bson.decode_all).
        """
        if attr in self._meta.fields.keys():
            # Value will be loaded from mongodb, python conversion will be
            # needed and also to set as not changed since item assigment will
            # be used only as db reads.
            field = self._meta.fields[attr]
            setattr(self, attr, field.to_python(value))
            self._changed = False
        else:
            raise ValueError('Item assignment only available for fields.')

    def __unicode__(self):
        # Overwrite __unicode__ only.
        return '%s at %s' % (self.__class__, hex(id(self)))

    def __str__(self):
        return str(self.__unicode__())

    def __contains__(self, item):
        return item in self._meta.fields

    def __eq__(self, other):
        return self.to_mongo() == other.to_mongo()

    def __ne__(self, other):
        return not self.__eq__(other)

    def drop_none(self, data=None):
        data = data or self._data
        output = {}
        for k, v in data.items():
            if isinstance(v, dict):
                v = self.drop_none(v)
            if v is not None:
                output[k] = v
        return output

    def to_json(self, fields=None):
        pass  # TODO: use utils.encode_json(self.to_python())

    def to_mongo(self, drop_none=True):
        """
        Return dict data with the Model._data as a mongodb valid format and
        validated using all fields.to_mongo.
        Call to_mongo with self._data loaded from database, from constructor or
        set by attribute.
        """
        doc = {}
        for name in self._meta.fields.keys():
            if self._validate_required:
                value = self._data[name]
            else:
                if name in self._data:
                    value = self._data[name]
                else:
                    continue
            field = self._meta.fields[name]
            value = field.to_mongo(value)
            if value is None and drop_none:
                continue
            doc[name] = value
        return doc

    def to_python(self):
        """
        Return dict data with the Model._data as a python valid object and
        validated using all fields.to_python.
        Call to_python always when self._data was previously read from the
        database or converted using Model.as_mongo(), never from data
        introduced in the constructor or set by attribute.
        """
        doc = {}
        for name in self._meta.fields.keys():
            if self._validate_required:
                value = self._data[name]
            else:
                if name in self._data:
                    value = self._data[name]
                else:
                    continue
            field = self._meta.fields[name]
            value = field.to_python(value)
            doc[name] = value
        return doc

    def as_python(self):
        self._data = self.to_python()

    def as_mongo(self):
        self._data = self.to_mongo(drop_none=False)


class Model(Document):
    _id = fields.ObjectIdField(auto=True)

    class Meta:
        abstract = True
        _embedded = False

    def __unicode__(self):
        # Overwrite __unicode__ only.
        return self._id

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.__unicode__())

    def _get_collection(self):
        if self._meta.collection_connection is None:
            self._meta.collection_connection = self.__class__.get_collection()
        return self._meta.collection_connection

    @classmethod
    def get_collection(cls):
        if not cls._meta.collection_connection:
            db = connect(cls._meta.database, **cls._meta.database_attrs)
            cls._meta.collection_connection = db[cls._meta.collection]
            cls._meta.collection_connection.document_class = cls
        return cls._meta.collection_connection

    @classmethod
    def insert_one(cls, document=None, **kwargs):
        col = cls.get_collection()
        document = document or kwargs
        doc = cls(**document)
        doc.as_mongo()
        data = doc.drop_none()
        result = col.insert_one(data)
        doc._id = result.inserted_id
        doc._changed = False
        doc.as_python()
        return doc

    @classmethod
    def insert_many(cls, *args, **kwargs):
        col = cls.get_collection()
        doc_list = []
        data = []
        for i in args:
            doc = cls(**i)
            data.append(doc.to_mongo())
            doc_list.append(doc)
        result = col.insert_many(data, kwargs.get('ordered', True))
        for doc, _id in zip(doc_list, result.inserted_ids):
            doc._id = _id
            doc._changed
            doc.as_python()
        return doc_list

    @classmethod
    def find_one(cls, query=None, **kwargs):
        if query is None:
            query = kwargs
            kwargs = {}
            kwargs.update({
                'projection': query.pop('_projection', None),
                'skip': query.pop('_skip', 0),
                'limit': query.pop('_limit', 0),
                'no_cursor_timeout': query.pop('_no_cursor_timeout', False),
                'sort': query.pop('_sort', None),
                'allow_partial_results': query.pop('_allow_partial_results',
                                                   False),
                'oplog_replay': query.pop('_oplog_replay', False),
                'modifiers': query.pop('_modifiers', None),
                'manipulate': query.pop('_manipulate', True)})

        return cls.get_collection().find_one(query, **kwargs)

    @classmethod
    def find(cls, query=None, **kwargs):  # TODO: DRY.
        if query is None:
            query = kwargs
            kwargs = {}
            kwargs.update({
                'projection': query.pop('_projection', None),
                'skip': query.pop('_skip', 0),
                'limit': query.pop('_limit', 0),
                'no_cursor_timeout': query.pop('_no_cursor_timeout', False),
                'sort': query.pop('_sort', None),
                'allow_partial_results': query.pop('_allow_partial_results',
                                                   False),
                'oplog_replay': query.pop('_oplog_replay', False),
                'modifiers': query.pop('_modifiers', None),
                'manipulate': query.pop('_manipulate', True)})

        return cls.get_collection().find(query, **kwargs)

    @classmethod
    def delete_one(cls, query=None, **kwargs):
        query = query or kwargs
        col = cls.get_collection()
        return col.delete_one(query)

    @classmethod
    def delete_many(cls, query=None, **kwargs):
        query = query or kwargs
        col = cls.get_collection()
        return col.delete_many(query)

    @classmethod
    def replace(cls, query, update, projection=None, upsert=False, sort=None):
        col = cls.get_collection()
        doc = col.find_one_and_replace(query, update, projection=projection,
                                       upsert=upsert, sort=sort,
                                       return_document=True)
        if doc:
            doc = cls(_validate_required=False, **doc)
            doc._changed = False
            doc.as_python()
            return doc

    @classmethod
    def update(cls, query, update, projection=None, upsert=False, sort=None):
        if not update.keys()[0].startswith('$'):
            update = cls(_validate_required=False, **update).to_mongo()
            update = format_update(update)
        col = cls.get_collection()
        doc = col.find_one_and_update(query, update, projection=projection,
                                      upsert=upsert, sort=sort,
                                      return_document=True)
        if doc:
            doc = cls(_validate_required=False, **doc)
            doc._changed = False
            doc.as_python()
            return doc

    @classmethod
    def update_many(cls, query, update, upsert=False):
        col = cls.get_collection()
        return col.update_many(query, update, upsert=upsert)

    @classmethod
    def count(cls, query=None, **kwargs):
        return cls.get_collection().count(query or kwargs)

    def save(self):
        col = self._get_collection()
        # Save the cleaned and validated data as mongo values, no filter out.
        # Use filtered out date to the query
        self.as_mongo()
        data = self.drop_none()
        if self._id:  # Update.
            # TODO: check if is full document.
            data.pop('_id')
            col.find_one_and_replace(
                {'_id': self._id}, data, return_document=True)
        else:  # Create.
            result = col.insert_one(data)
            self._id = result.inserted_id
        self._changed = False
        self.as_python()

    def delete(self):
        if self._id:
            return self._get_collection().delete_one({'_id': self._id})
