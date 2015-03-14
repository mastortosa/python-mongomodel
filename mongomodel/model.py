from mongomodel import fields
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

        # Get database.
        if type(bases[0]) != type:  # Avoid checking for base model.
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
            meta.collection = getattr(meta, 'collection', name.lower())

            # Deal with collection connectio in the meta to reduce attemps to
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


class Model(object):
    __metaclass__ = ModelMeta

    _changed = False

    _id = fields.ObjectIdField(auto=True)

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
        """Overwrite just __unicode__ method."""
        return self._id

    def __str__(self):
        return str(self.__unicode__())

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
    def create(cls, **kwargs):
        obj = cls(**kwargs)
        obj._get_collection().save(obj.to_mongo())
        return obj

    @classmethod
    def update(cls, **kwargs):
        pass

    @classmethod
    def get(cls, **kwargs):
        return cls.get_collection().find_one(kwargs)

    @classmethod
    def list(cls, **kwargs):
        return cls.get_collection().find(kwargs)

    @classmethod
    def delete(cls, **kwargs):
        return cls.get_collection().remove(kwargs)

    def _get_collection(self):
        if self._meta.collection_connection is None:
            self._meta.collection_connection = self.__class__.get_collection()
        return self._meta.collection_connection

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

    def save(self):
        self._id = self._get_collection().save(self.to_mongo())
        self.as_python()

    def delete_instance(self):
        pass
