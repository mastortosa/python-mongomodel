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
        self._to_python = list(to_python)
        self._to_mongo = list(to_mongo)

    def _process(self, val, *args):
        for fn in args:
            if type(fn) == type:
                val = fn(val)
            else:
                val = fn(val, self)
        return val

    def to_mongo(self, *args):
        args = list(args)
        args.reverse()
        return self._process(self.value, *(args + self._to_mongo))

    def to_python(self, *args):
        args = list(args)
        args.reverse()
        return self._process(self.value, *(args + self._to_python))

    def set_value(self, val):
        self.value = val
        self.changed = True


class StringField(Field):

    def to_mongo(self, *args):
        def validate_str(val, obj):
            if obj.required and not val.strip():
                raise ValueError('value can\'t be empty')
            return val
        return super(StringField, self).to_mongo(validate_str, unicode, *args)


class EmailField(StringField):

    def to_mongo(self, *args):
        def validate_email(val, obj):
            try:
                atpos = val.index('@')
                dotpos = val.rindex('.')
            except ValueError:
                raise ValueError('%s is not a valid email' % val)
            if atpos < 1 or dotpos < atpos + 2 or dotpos + 2 >= len(val):
                raise ValueError('%s is not a valid email' % val)
            return val
        return super(EmailField, self).to_mongo(validate_email, *args)


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
    pass  # TODO: serialize


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
                raise ValueError('%s is not a valid timezone' % timezone)
        super(DateTimeField, self).__init__(**kwargs)

    def to_mongo(self, *args):

        def load_datetime(val, obj):
            if not isinstance(val, datetime):
                val = dateutil.parser.parse(val)
            return val

        def validate_timezone(val, obj):
            if obj.timezone and val.tzinfo is None:
                raise ValueError('%s has no timezone while timezone is %s' %
                                 (val, obj.timezone))
            return val

        def set_timezone(val, obj):
            if obj.timezone:
                val = val.replace(tzinfo=obj.timezone)
            return val

        return super(DateTimeField, self).to_mongo(lambda x, _: x.isoformat(),
                                                   set_timezone,
                                                   validate_timezone,
                                                   load_datetime,
                                                   *args)

    def to_python(self, *args):
        return super(DateTimeField, self).to_python(
            lambda x, _: dateutil.parser.parse(x), *args)


class DateField(Field):

    def to_mongo(self, *args):
        def load_date(val, obj):
            if isinstance(val, (date, datetime)):
                val = val.date()
            else:
                val = dateutil.parser.parse(val).date()
            return val
        return super(DateField, self).to_mongo(
            lambda x, _: x.isoformat(), load_date, *args)

    def to_python(self, *args):
        return super(DateField, self).to_python(
            lambda x, _: dateutil.parser.parse(x), *args)


class TimestampField(Field):

    def __init__(self, format=int, **kwargs):
        if format not in (float, int,):
            raise ValueError('timestamp format %s not valid' % format)
        self.format = format
        super(TimestampField, self).__init__(**kwargs)

    def to_mongo(self, *args):
        def load_timstamp(val, obj):
            if isinstance(val, datetime):
                val = time.mktime(val.timetuple())
            return val
        return super(TimestampField, self).to_mongo(
            self.format, load_timstamp, *args)

    def to_python(self, *args):
        return super(TimestampField, self).to_python(
            lambda x, _: datetime.fromtimestamp(x), *args)


class UUIDField(Field):

    def __init__(self, format='hex', unique=True, **kwargs):
        if format not in ('hex', 'int', 'urn', 'str',):
            raise ValueError('UUID format %s not valid' % format)
        self.format = format
        super(UUIDField, self).__init__(unique, **kwargs)

    def to_mongo(self, *args):
        def load_uuid(val, obj):
            if obj.format == 'str':
                val = val.__str__()
            return getattr(val, obj.format)
        return super(UUIDField, self).to_mongo(load_uuid, *args)

    def to_python(self, *args):
        def load_uuid(val, obj):
            if isinstance(val, (str, unicode)):
                val = uuid.UUID(val)
            else:
                val = uuid.UUID(int=val)
            return val
        return super(UUIDField, self).to_python(load_uuid, *args)
