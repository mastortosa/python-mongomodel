from datetime import datetime, date
import re
import time
import uuid

from bson.objectid import ObjectId
import dateutil.parser
import pytz


from mongomodel import utils


class Field(object):
    name = None

    def __init__(self, default=None, required=True, auto=False, to_python=[],
                 to_mongo=[]):
        self.required = required
        self.auto = auto
        self.default = default
        self._to_python = list(to_python)
        self._to_mongo = list(to_mongo)

    def __set__(self, instance, value):
        if instance is not None:
            instance._data[self.name] = value
            instance._changed = True

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance._data.get(self.name)

    def _process(self, value, *args):
        for fn in args:
            if type(fn) == type:
                value = fn(value)
            else:
                value = fn(value, self)
        return value

    def to_mongo(self, value, *args):
        if value is None:
            if self.required and not self.auto:
                raise ValueError('value can\'t be none if required')
            else:
                return None
        else:
            args = list(args)
            args.reverse()
            return self._process(value, *(args + self._to_mongo))

    def to_python(self, value, *args):
        args = list(args)
        args.reverse()
        return self._process(value, *(args + self._to_python))


class TextField(Field):

    def to_mongo(self, value, *args):
        def validate(value, instance):
            if instance.required and not value.strip():
                raise ValueError('value can\'t be empty')
            return value
        return super(TextField, self).to_mongo(value, validate, unicode, *args)


class EmailField(TextField):

    def to_mongo(self, value, *args):
        def validate(value, instance):
            ixat = value.index('@')
            ixdot = value.rindex('.')
            assert(ixat > 1 and ixdot > ixat + 2 and ixdot + 2 < len(value))
            return value

        return super(EmailField, self).to_mongo(value, validate, *args)


class URLField(TextField):

    def __init__(self, https=False, **kwargs):
        self.https = https
        super(URLField, self).__init__(**kwargs)

    def to_mongo(self, value, *args):
        def clean(value, instance):
            if not (value.startswith('http://') or
                    value.startswith('https://')):
                return '%s://%s' % (('https' if instance.https else 'http'),
                                    value)
            return value

        def validate(value):
            regex = r'^(http|https)://(.*)?((\.\w{2})|(\.\w{3}))$'
            if not re.match(regex, value):
                raise ValueError
            return value

        return super(URLField, self).to_mongo(value, validate, clean, *args)


class BooleanField(Field):

    def to_mongo(self, value, *args):
        return super(BooleanField, self).to_mongo(value, bool, *args)


class IntegerField(Field):

    def to_mongo(self, value, *args):
        return super(IntegerField, self).to_mongo(value, int, float, *args)


class FloatField(Field):

    def to_mongo(self, value, *args):
        return super(FloatField, self).to_mongo(value, float, *args)


class ListField(Field):

    def to_mongo(self, value, *args):
        return super(ListField, self).to_mongo(value, list, *args)


class SetField(Field):

    def to_mongo(self, value, *args):
        return super(SetField, self).to_mongo(value, list, *args)

    def to_python(self, value, *args):
        return super(SetField, self).to_python(value, set, *args)


class DictField(Field):
    pass  # TODO: serialize? embebbed document?


class JSONField(Field):

    def to_mongo(self, value, *args):
        return super(JSONField, self).to_mongo(
            value, lambda x, _: utils.encode_json(x), *args)


class DateTimeField(Field):

    def __init__(self, timezone=None, **kwargs):
        if timezone is None or isinstance(timezone, pytz.tzinfo.DstTzInfo):
            self.timezone = timezone
        else:
            try:
                self.timezone = pytz.timezone(timezone)
            except:
                raise ValueError('%s is not a valid timezone' % timezone)
        super(DateTimeField, self).__init__(**kwargs)

    def to_mongo(self, value, *args):

        def load(value, instance):
            if not isinstance(value, datetime):
                value = dateutil.parser.parse(value)
            return value

        def validate_timezone(value, instance):
            if instance.timezone and value.tzinfo is None:
                raise ValueError('%s has no timezone while timezone is %s' %
                                 (value, instance.timezone))
            return value

        def set_timezone(value, instance):
            if instance.timezone:
                value = value.replace(tzinfo=instance.timezone)
            return value

        return super(DateTimeField, self).to_mongo(value,
                                                   lambda x, _: x.isoformat(),
                                                   set_timezone,
                                                   validate_timezone,
                                                   load,
                                                   *args)

    def to_python(self, value, *args):
        def load(value, instance):
            if isinstance(value, (str, unicode)):
                value = dateutil.parser.parse(value)
            return value
        return super(DateTimeField, self).to_python(value, load, *args)


class DateField(Field):

    def to_mongo(self, value, *args):
        def load(value, instance):
            if isinstance(value, (date, datetime)):
                value = value.date()
            else:
                value = dateutil.parser.parse(value).date()
            return value
        return super(DateField, self).to_mongo(
            value, lambda x, _: x.isoformat(), load, *args)

    def to_python(self, value, *args):
        def load(value, instance):
            if isinstance(value, (str, unicode)):
                value = dateutil.parser.parse(value)
            return value
        return super(DateField, self).to_python(value, load, *args)


class TimestampField(Field):

    def __init__(self, format=int, **kwargs):
        if format not in (float, int,):
            raise ValueError('timestamp format %s not valid' % format)
        self.format = format
        super(TimestampField, self).__init__(**kwargs)

    def to_mongo(self, value, *args):
        def load(value, instance):
            if isinstance(value, datetime):
                value = time.mktime(value.timetuple())
            return value
        return super(TimestampField, self).to_mongo(
            value, self.format, load, *args)

    def to_python(self, value, *args):
        return super(TimestampField, self).to_python(
            value, lambda x, _: datetime.fromtimestamp(x), *args)


class UUIDField(Field):

    def __init__(self, format='hex', unique=True, **kwargs):
        if format not in ('hex', 'int', 'urn', 'str',):
            raise ValueError('UUID format %s not valid' % format)
        self.format = format
        super(UUIDField, self).__init__(unique, **kwargs)

    def to_mongo(self, value, *args):
        def load(value, instance):
            if instance.format == 'str':
                value = value.__str__()
            return getattr(value, instance.format)
        return super(UUIDField, self).to_mongo(value, load, *args)

    def to_python(self, value, *args):
        def load(value, instance):
            if isinstance(value, (str, unicode)):
                value = uuid.UUID(value)
            else:
                value = uuid.UUID(int=value)
            return value
        return super(UUIDField, self).to_python(value, load, *args)


class ObjectIdField(Field):

    def to_mongo(self, value, *args):
        def validate(value, instance):
            if isinstance(value, (str, unicode)):
                value = ObjectId(value)
            return value
        return super(ObjectIdField, self).to_mongo(value, validate, *args)

    def to_python(self, value, *args):
        def validate(value, instance):
            if isinstance(value, (str, unicode)):
                value = ObjectId(value)
            return value
        return super(ObjectIdField, self).to_python(value, validate, *args)
