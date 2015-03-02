import time
import uuid
from datetime import datetime, date

import dateutil.parser
import pytz

from mongomodels import utils


class Field(object):
    value = None
    changed = False

    def __init__(self, default=None, required=True, unique=False,
                 primary_key=False, to_python=[], to_mongo=[]):
        self.required = required
        self.unique = unique
        self.value = default
        self.primary_key = primary_key
        self._to_python = list(to_python)
        self._to_mongo = list(to_mongo)

    def __set__(self, instance, value):
        self.value = value
        self.changed = True

    def __get__(self, instance, owner):
        return self.value

    def _process(self, value, *args):
        for fn in args:
            if type(fn) == type:
                value = fn(value)
            else:
                value = fn(value, self)
        return value

    def to_mongo(self, *args):
        args = list(args)
        args.reverse()
        return self._process(self.value, *(args + self._to_mongo))

    def to_python(self, *args):
        args = list(args)
        args.reverse()
        return self._process(self.value, *(args + self._to_python))


class TextField(Field):

    def to_mongo(self, *args):
        def validate(value, obj):
            if obj.required and not value.strip():
                raise ValueError('value can\'t be empty')
            return value
        return super(TextField, self).to_mongo(validate, unicode, *args)


class EmailField(TextField):

    def to_mongo(self, *args):
        def validate(value, obj):
            ixat = value.index('@')
            ixdot = value.rindex('.')
            assert(ixat > 1 and ixdot > ixat + 2 and ixdot + 2 < len(value))
            return value
        return super(EmailField, self).to_mongo(validate, *args)


class BooleanField(Field):

    def to_mongo(self, *args):
        return super(BooleanField, self).to_mongo(bool, *args)


class IntegerField(Field):

    def to_mongo(self, *args):
        return super(IntegerField, self).to_mongo(int, float, *args)


class FloatField(Field):

    def to_mongo(self, *args):
        return super(FloatField, self).to_mongo(float, *args)


class ListField(Field):

    def to_mongo(self, *args):
        return super(ListField, self).to_mongo(list, *args)


class SetField(Field):

    def to_mongo(self, *args):
        return super(SetField, self).to_mongo(list, *args)

    def to_python(self, *args):
        return super(SetField, self).to_python(set, *args)


class DictField(Field):
    pass  # TODO: serialize? EmbebbedDocumentField?


class JSONField(Field):

    def to_mongo(self, *args):
        return super(JSONField, self).to_mongo(
            lambda x, _: utils.encode_json(x), *args)


class DateTimeField(Field):

    def __init__(self, timezone=None, **kwargs):
        if timezone is None or isinstance(timezone, pytz.tzinfo.DstTzInfo):
            self.timezone = timezone
        else:
            try:
                self.timezone = pytz.timezone(timezone)
            except:
                raise ValueError('%s is not a valueid timezone' % timezone)
        super(DateTimeField, self).__init__(**kwargs)

    def to_mongo(self, *args):

        def load(value, obj):
            if not isinstance(value, datetime):
                value = dateutil.parser.parse(value)
            return value

        def validate_timezone(value, obj):
            if obj.timezone and value.tzinfo is None:
                raise ValueError('%s has no timezone while timezone is %s' %
                                 (value, obj.timezone))
            return value

        def set_timezone(value, obj):
            if obj.timezone:
                value = value.replace(tzinfo=obj.timezone)
            return value

        return super(DateTimeField, self).to_mongo(lambda x, _: x.isoformat(),
                                                   set_timezone,
                                                   validate_timezone,
                                                   load,
                                                   *args)

    def to_python(self, *args):
        return super(DateTimeField, self).to_python(
            lambda x, _: dateutil.parser.parse(x), *args)


class DateField(Field):

    def to_mongo(self, *args):
        def load(value, obj):
            if isinstance(value, (date, datetime)):
                value = value.date()
            else:
                value = dateutil.parser.parse(value).date()
            return value
        return super(DateField, self).to_mongo(
            lambda x, _: x.isoformat(), load, *args)

    def to_python(self, *args):
        return super(DateField, self).to_python(
            lambda x, _: dateutil.parser.parse(x), *args)


class TimestampField(Field):

    def __init__(self, format=int, **kwargs):
        if format not in (float, int,):
            raise ValueError('timestamp format %s not valueid' % format)
        self.format = format
        super(TimestampField, self).__init__(**kwargs)

    def to_mongo(self, *args):
        def load(value, obj):
            if isinstance(value, datetime):
                value = time.mktime(value.timetuple())
            return value
        return super(TimestampField, self).to_mongo(
            self.format, load, *args)

    def to_python(self, *args):
        return super(TimestampField, self).to_python(
            lambda x, _: datetime.fromtimestamp(x), *args)


class UUIDField(Field):

    def __init__(self, format='hex', unique=True, **kwargs):
        if format not in ('hex', 'int', 'urn', 'str',):
            raise ValueError('UUID format %s not valueid' % format)
        self.format = format
        super(UUIDField, self).__init__(unique, **kwargs)

    def to_mongo(self, *args):
        def load(value, obj):
            if obj.format == 'str':
                value = value.__str__()
            return getattr(value, obj.format)
        return super(UUIDField, self).to_mongo(load, *args)

    def to_python(self, *args):
        def load(value, obj):
            if isinstance(value, (str, unicode)):
                value = uuid.UUID(value)
            else:
                value = uuid.UUID(int=value)
            return value
        return super(UUIDField, self).to_python(load, *args)
