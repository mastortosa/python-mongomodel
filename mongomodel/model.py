from mongomodel import fields
from mongomodel.utils import validate_update_query
from mongomodel.db import Client


_connections = {}


def connect(db_name, **kwargs):
    global _connections
    if db_name in _connections:
        db = _connections[db_name][db_name]
    else:
        _connections[db_name] = Client(**kwargs)
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

        # Get database.
        if not meta._embedded and not getattr(meta, 'abstract', False):
            try:
                meta.database = next((i.database for i in super_meta_list
                                      if hasattr(i, 'database')),
                                     meta.database)
                meta.database_attrs = next((i.database_attrs
                                            for i in super_meta_list
                                            if hasattr(i, 'database_attrs')),
                                           {})
            except AttributeError:
                raise ValueError('database not found in model Meta.')

            # Get collection.
            meta.collection = getattr(
                meta, 'collection', ('%ss' % name).lower())

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

        new_class = super(ModelMeta, cls).__new__(cls, name, bases, attrs)
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

    def __init__(self, **kwargs):
        # Data will contain the data from the mongo doc. If is set by the
        # user/app it will be stored as it till the model is saved, when the
        # model is saved _data will contain the values as declared in
        # Field.to_python(). When the model is created directly from data
        # retrieved from the database, _data will contain, again, the values as
        # declared in Field.to_python().
        self._data = dict((k, v.default) for k, v in self._meta.fields.items())

        if set(kwargs.keys()).issubset(self._meta.fields.keys()):
            for k, v in kwargs.items():
                setattr(self, k, v)
        else:
            raise ValueError('One or more fields are not valid.')

    def __setitem__(self, attr, value):
        """
        Assign attribute assignment to item assignment to support cursor
        decoding (bson.decode_all).
        """
        if attr in self._meta.fields:
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
        return self.to_python() == other.to_python()

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_mongo(self, drop_none=True):
        doc = {}
        for name, field in self._meta.fields.items():
            value = self._data[name]
            if field.auto and value is None:
                continue
            value = field.to_mongo(value)
            if value is None and drop_none:
                continue
            doc[name] = value
        return doc

    def to_python(self):
        return dict((k, v.to_python(self._data[k]))
                    for k, v in self._meta.fields.items())

    def as_python(self):
        self._data = self.to_python()


class Model(Document):

    # TODO: check Cursor.__init__ and Cursor.find for pymongo 3.x
    # TODO: to_mongo() for fields in Model.create, Model.update, Model.delete
    # TODO: rethink Model._changed.
    # TODO: recursive embebbed document.
    # TODO: bulk write - api.mongodb.org/python/current/api/pymongo/bulk.html
    # TODO: support different _id fields, keepin Model._id as default.
    # TODO: Field.unique
    # ??: raise error when nothing is updated?

    _id = fields.ObjectIdField(auto=True)

    class Meta:
        abstract = True
        _embedded = False

    def __unicode__(self):
        # Overwrite __unicode__ only.
        return self._id

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.__unicode__())

    @classmethod
    def get_collection(cls):
        if not cls._meta.collection_connection:
            db = connect(cls._meta.database, **cls._meta.database_attrs)
            cls._meta.collection_connection = db[cls._meta.collection]
            cls._meta.collection_connection.model_cls = cls
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
            doc = cls(**kwargs)
            data = doc.to_mongo()
            result = collection.insert_one(data)
            doc._id = result.inserted_id  # ??: for all Field.auto?
            doc._changed = False
            doc.as_python()
            return doc

    @classmethod
    def update(cls, filter, update, upsert=False, multi=False, replace=False):
        """
        Update one or more documents.
        """
        collection = cls.get_collection()
        if multi:
            data = validate_update_query(update)
            return collection.update_many(filter, data, upsert)
        elif replace:
            # New document from update. Get data from a new model instance.
            doc = cls(**update)
            data = doc.to_mongo()
            method = collection.find_one_and_replace
        else:
            # Some attributes of the document will be updated.
            data = validate_update_query(update)
            method = collection.find_one_and_update
        doc = method(filter, data, upsert=upsert, return_document=True)
        doc = cls(**doc)
        doc._changed = False
        return doc

    @classmethod
    def get(cls, **kwargs):
        """
        Get one document.
        """
        doc = cls.get_collection().find_one(kwargs)
        if doc:
            doc = cls(**doc)
            doc._changed = False
            return doc

    @classmethod
    def list(cls, **kwargs):
        """
        Get all documents matching the kwargs. Returns pymongo.cursor.Cursor
        with cls as document class.
        """
        return cls.get_collection().find(kwargs)

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
        data = self.to_mongo()
        if self._id:
            data.pop('_id')
            collection.find_one_and_replace(
                {'_id': self._id}, data, return_document=True)
        else:
            result = collection.insert_one(data)
            self._id = result.inserted_id
        self._changed = False
        self.as_python()

    def delete_instance(self):
        if self._id:
            return self._get_collection().delete_one({'_id': self._id})
