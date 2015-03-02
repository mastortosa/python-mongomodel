from mongomodel import fields


class BaseModelMeta(type):

    def __new__(cls, name, bases, attrs):
        meta = attrs.pop('Meta', type('Meta', (object,), {}))

        # Get superclasses fields.
        meta.fields = {}
        for super_class in bases:
            super_meta = getattr(super_class, '_meta', None)
            if super_meta:
                meta.fields.update(getattr(super_meta, 'fields', {}))

        # Get class fields.
        meta.fields.update(dict((k, v) for k, v in attrs.items()
                           if isinstance(v, fields.Field)))

        new_class = super(BaseModelMeta, cls).__new__(cls, name, bases, attrs)
        new_class._meta = meta
        return new_class


class BaseModel(object):
    __metaclass__ = BaseModelMeta

    def __init__(self, **kwargs):
        if set(kwargs.keys()).issubset(self._meta.fields.keys()):
            for k, v in kwargs.items():
                setattr(self, k, v)
        else:
            raise ValueError('One or more fields are not valid.')

    def to_python(self):
        return dict((k, v.to_python()) for k, v in self._meta.fields.items())

    def to_mongo(self):
        return dict((k, v.to_mongo()) for k, v in self._meta.fields.items())
