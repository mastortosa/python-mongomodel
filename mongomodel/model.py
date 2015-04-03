import re
import urllib

from mongomodel import fields
from mongomodel.db import Client
from mongomodel.utils import get_sort_list


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

    # A subset of self._meta.fields.keys() or None (all fields).
    _projection = None

    class Meta:
        _embedded = True

    def __init__(self, **kwargs):
        # Data will contain the data from the mongo doc. If is set by the
        # user/app it will be stored as it till the model is saved, when the
        # model is saved _data will contain the values as declared in
        # Field.to_python(). When the model is created directly from data
        # retrieved from the database, _data will contain, again, the values as
        # declared in Field.to_python().
        projection = kwargs.get('_projection')
        if projection and not isinstance(projection, (list, tuple)):
            raise ValueError('_projection must be None|list|tuple')
            # TODO: validate subfields.
        self._projection = projection

        try:
            self._data = dict((i, self._meta.fields[i].default)
                              for i in self._get_field_names())
        except KeyError:
            raise ValueError('%s is not a valid field' % i)

        for k, v in kwargs.items():
            setattr(self, k, v)

        # if set(kwargs.keys()).issubset(self._meta.fields.keys()):
        #     for k, v in kwargs.items():
        #         setattr(self, k, v)
        # else:
        #     raise ValueError('One or more fields are not valid.')

    def __setitem__(self, attr, value):
        """
        Assign attribute assignment to item assignment to support cursor
        decoding (bson.decode_all).
        """
        if attr in self._get_field_names():
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

    def __repr__(self):
        return str(self.__unicode__())

    def __contains__(self, item):
        return item in self._meta.fields

    def __eq__(self, other):
        return self.to_mongo() == other.to_mongo()

    def __ne__(self, other):
        return not self.__eq__(other)

    def _get_field_names(self):
        if self._projection is None:
            return self._meta.fields.keys()
        else:
            return self._projection

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
        # doc = {}
        # for name, field in self._meta.fields.items():
        #     value = self._data[name]
        #     value = field.to_mongo(value)
        #     if value is None and drop_none:
        #         continue
        #     doc[name] = value
        # return doc
        doc = {}
        for name in self._get_field_names():
            field = self._meta.fields[name]
            value = field.to_mongo(self._data[name])
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
        # return dict((k, v.to_python(self._data[k]))
        #             for k, v in self._meta.fields.items())
        return dict((i, self._meta.fields[i].to_python(self._data[i]))
                    for i in self._get_field_names())

    def as_python(self):
        self._data = self.to_python()

    def as_mongo(self):
        self._data = self.to_mongo(drop_none=False)

    @classmethod
    def validate_update_query(cls, update):
        # Although Document has no db operation it must provide an update
        # validation since a document may be part of a model to update.
        data = {}
        for operator, kv in update.items():
            mongo_kv = {}
            for k, v in kv.items():
                k_split = k.split('.')
                if len(k_split) > 1:
                    # Convert from compacted to extended query. Eg:
                    # {k, v}; k ='key.subkey.num'; v = 2
                    # {k, v}; k ='key'; v={'subkey': {'num': 2}})}
                    k_split.reverse()
                    for k in k_split[:-1]:
                        v = {k: v}
                    k = k_split[-1]
                try:
                    field = cls._meta.fields[k]
                except KeyError:
                    raise ValueError('%s is not a field' % k)
                if isinstance(field, fields.ListField):
                    field.validate_update_operator(operator, v)
                    # TODO: call to_mongo with custom=False according to the
                    #       field.validate_update_operator comments.
                    # ??: convert from extended query to compacted query?
                elif isinstance(field, fields.EmbeddedDocumentField):
                    v = field.document.validate_update_query({operator: v})
                    if operator not in ('$unset', '$currentDate'):
                        v = v[operator]
                else:
                    field.validate_update_operator(operator, v)
                    # Provide a proper mongo value. Do not call custom to_mongo
                    # functions since it may have conflicts, eg:
                    # If a model has a field to store natural numbers:
                    # nat = fields.IntField(to_mongo=[validate_gt_zero])
                    # This operator would be wrong according to the custom
                    # validation:
                    # {'$inc': {'nat': -3}}.
                    # Sometimes the result in the document may be a valid or an
                    # invalid model, following the same example,
                    # doc: {'nat': 5} -> update -> doc: {'nat': 2}  # Valid.
                    # doc: {'nat': 2} -> update -> doc: {'nat': -1}  # Invalid.
                    # To avoid this, use Model.update() with replace=True or
                    # Model.save().
                    if operator not in ('$unset', '$currentDate'):
                        v = field.to_mongo(v, custom=False)
                if k in mongo_kv and isinstance(mongo_kv[k], dict):
                    mongo_kv[k].update(v)
                else:
                    mongo_kv[k] = v
            data[operator] = mongo_kv
        return data


class Model(Document):
    _id = fields.ObjectIdField(auto=True)

    class Meta:
        abstract = True
        _embedded = False

    def __init__(self, **kwargs):
        if '_projection' in kwargs and kwargs['_projection'] is not None and \
                '_id' not in '_projection':
            kwargs['_projection'] = ['_id'] + list(kwargs['_projection'])
        super(Model, self).__init__(**kwargs)

    def __unicode__(self):
        # Overwrite __unicode__ only.
        return self._id

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.__unicode__())

    @classmethod
    def clean_query(cls, **kwargs):
        query = {}
        projection = kwargs.pop('_projection', None)
        if projection and isinstance(projection, (list, set)):
            projection = dict((i, 1) for i in projection)
        for k, v in kwargs.items():
            query[k] = cls._meta.fields[k].to_mongo(v)
        return query, projection

    @classmethod
    def get_collection(cls):
        if not cls._meta.collection_connection:
            db = connect(cls._meta.database, **cls._meta.database_attrs)
            cls._meta.collection_connection = db[cls._meta.collection]
        return cls._meta.collection_connection

    @classmethod
    def create(cls, *args, **kwargs):
        """
        Insert one or more documents.
        Pass argument list as documents to be created, otherwise pass kwargs
        as attributes of the document to be created.
        """
        collection = cls.get_collection()
        if args:
            doc_list = []
            data = []
            for i in args:
                doc = cls(**i)
                data.append(doc.to_mongo())
                doc_list.append(doc)
            result = collection.insert_many(data)
            for doc, _id in zip(doc_list, result.inserted_ids):
                doc._id = _id
                doc._changed
                doc.as_python()
            return doc_list
        else:
            # Save and get data in the same way as self.save.
            doc = cls(**kwargs)
            doc.as_mongo()
            data = doc.drop_none()
            result = collection.insert_one(data)
            doc._id = result.inserted_id  # ??: for all Field.auto?
            doc._changed = False
            doc.as_python()
            return doc

    @classmethod
    def update(cls, query, update, multi=False, replace=False, projection=None,
               upsert=False, sort=None):
        """
        Update one or more documents.
        """
        collection = cls.get_collection()
        if multi:
            data = cls.validate_update_query(update)
            return collection.update_many(query, data, upsert)
        elif replace:
            # New document from update. Get data from a new model instance.
            doc = cls(**update)
            data = doc.to_mongo()
            method = collection.find_one_and_replace
        else:
            # Some attributes of the document will be updated.
            data = cls.validate_update_query(update)
            method = collection.find_one_and_update
        doc = method(query, data, projection=projection, upsert=upsert,
                     sort=sort, return_document=True)
        doc = cls(**doc)
        doc._changed = False
        doc.as_python()
        return doc

    @classmethod
    def get(cls, **kwargs):
        """
        Get one document.
        """
        query, projection = cls.clean_query(**kwargs)
        doc = cls.get_collection().find_one(query, projection)
        if doc:
            if projection:
                projection = projection.keys()
            doc['_projection'] = projection
            doc = cls(**doc)
            doc._changed = False
            doc.as_python()
            return doc

    @classmethod
    def list(cls, _projection=None, _skip=0, _limit=0,
             _no_cursor_timeout=False, _sort=None,
             _allow_partial_results=False, _oplog_replay=False,
             _modifiers=None, **filter):
        # TODO: create custom cursor for pymongo-3dev.
        """
        Get all documents matching the kwargs. Returns pymongo.cursor.Cursor
        with cls as document class.
        """
        _sort = _sort or cls._sort
        return cls.get_collection().find(
            filter=filter, projection=_projection, skip=_skip, limit=_limit,
            no_cursor_timeout=_no_cursor_timeout, sort=_sort,
            allow_partial_results=_allow_partial_results,
            oplog_replay=_oplog_replay)

    @classmethod
    def delete(cls, multi=True, **kwargs):
        """
        Delete one or more documents. Returns pymongo.results.DeleteResult
        instance.
        """
        collection = cls.get_collection()
        if multi:
            method = collection.delete_many
        else:
            method = collection.delete_one
        return method(kwargs)

    @classmethod
    def count(cls, **kwargs):
        return cls.get_collection().count(kwargs)

    def _get_collection(self):
        if self._meta.collection_connection is None:
            self._meta.collection_connection = self.__class__.get_collection()
        return self._meta.collection_connection

    def save(self):
        collection = self._get_collection()
        # Save the cleaned and validated data as mongo values, no filter out.
        # Use filtered out date to the query
        self.as_mongo()  # TODO: if not all the required fields are set, this will throw an error when trying to update a document.
        data = self.drop_none()
        if self._id:  # Update.
            data.pop('_id')
            collection.find_one_and_replace(
                {'_id': self._id}, data, return_document=True)
        else:  # Created.
            result = collection.insert_one(data)
            self._id = result.inserted_id
        self._changed = False
        self.as_python()

    def delete_instance(self):
        if self._id:
            return self._get_collection().delete_one({'_id': self._id})
