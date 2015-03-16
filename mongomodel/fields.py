import pytz

from mongomodel import utils


class FieldValidationError(Exception):

    def __init__(self, message='', instance=None):
        self.field = instance.name
        if not message:
            message = 'Not a valid %s' % instance.__class__.__name__
        if self.field:
            message = '[%s] %s' % (self.field, message)
        super(FieldValidationError, self).__init__(message)


class FieldConfigurationError(Exception):
    pass


class Field(object):
    name = None

    ValidationError = FieldValidationError
    ConfigurationError = FieldConfigurationError

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
        try:
            for fn in args:
                if type(fn) == type:
                    value = fn(value)
                else:
                    value = fn(value, self)
        except:
            raise self.ValidationError(instance=self)
        return value

    def to_mongo(self, value, *args):
        if value is None:
            if self.required and not self.auto:
                raise self.ValidationError('Value can\'t be none if required',
                                           instance=self)
            else:
                return None
        else:
            args = list(args)
            # args.reverse()
            return self._process(value, *(args + self._to_mongo))

    def to_python(self, value, *args):
        args = list(args)
        # args.reverse()
        return self._process(value, *(args + self._to_python))


class TextField(Field):

    def to_mongo(self, value, *args):
        return super(TextField, self).to_mongo(
            value, unicode, utils.validate_text, *args)


class EmailField(TextField):

    def to_mongo(self, value, *args):
        return super(EmailField, self).to_mongo(
            value, utils.validate_email, *args)


class URLField(TextField):

    def __init__(self, https=False, **kwargs):
        self.https = https
        super(URLField, self).__init__(**kwargs)

    def to_mongo(self, value, *args):
        return super(URLField, self).to_mongo(
            value, utils.clean_url, utils.validate_url, *args)


class BooleanField(Field):

    def to_mongo(self, value, *args):
        return super(BooleanField, self).to_mongo(value, bool, *args)

    def to_python(self, value, *args):
        return super(BooleanField, self).to_python(value, bool, *args)


BoolField = BooleanField


class IntegerField(Field):

    def to_mongo(self, value, *args):
        return super(IntegerField, self).to_mongo(value, float, int, *args)

    def to_python(self, value, *args):
        return super(IntegerField, self).to_python(value, float, int, *args)


IntField = IntegerField


class FloatField(Field):

    def to_mongo(self, value, *args):
        return super(FloatField, self).to_mongo(value, float, *args)

    def to_python(self, value, *args):
        return super(FloatField, self).to_python(value, float, *args)


class ListField(Field):

    # TODO: add max_lenght property.

    def __init__(self, field, **kwargs):
        if field == 'self':
            field = ListField(field='??')  # TODO: think about this
        elif not field or not isinstance(field, Field):
            raise self.ConfigurationError('%s.field must be a Field instance.'
                                          % self.__class__.__name__)
        self.field = field
        super(ListField, self).__init__(**kwargs)

    def to_mongo(self, value, *args):
        return super(ListField, self).to_mongo(
            value, utils.list_to_mongo, *args)

    def to_python(self, value, *args):
        return super(ListField, self).to_python(
            value, utils.list_to_python, *args)


class SetField(ListField):

    def to_mongo(self, value, *args):
        return super(SetField, self).to_mongo(value, set, list, *args)

    def to_python(self, value, *args):
        return super(SetField, self).to_python(value, set, *args)


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
                raise self.ConfigurationError('%s is not a valid timezone'
                                              % timezone)
        super(DateTimeField, self).__init__(**kwargs)

    def to_mongo(self, value, *args):
        return super(DateTimeField, self).to_mongo(
            value, utils.load_datetime, utils.validate_timezone,
            utils.set_timezone, utils.isodate, *args)

    def to_python(self, value, *args):
        return super(DateTimeField, self).to_python(
            value, utils.load_datetime, *args)


class DateField(Field):

    def to_mongo(self, value, *args):
        return super(DateField, self).to_mongo(
            value, utils.load_date, utils.isodate, *args)

    def to_python(self, value, *args):
        return super(DateField, self).to_python(value, utils.load_date, *args)


class TimestampField(Field):

    def __init__(self, format=int, **kwargs):
        if format not in (float, int,):
            raise self.ConfigurationError(
                'timestamp format %s not valid' % format)
        self.format = format
        super(TimestampField, self).__init__(**kwargs)

    def to_mongo(self, value, *args):
        return super(TimestampField, self).to_mongo(
            value, self.load_timestamp, self.format, *args)

    def to_python(self, value, *args):
        return super(TimestampField, self).to_python(
            value, utils.timestamp_to_datetime, *args)


class UUIDField(Field):

    def __init__(self, format='hex', unique=True, **kwargs):
        if format not in ('hex', 'int', 'urn', 'str',):
            raise self.ConfigurationError('UUID format %s not valid' % format)
        self.format = format
        super(UUIDField, self).__init__(unique, **kwargs)

    def to_mongo(self, value, *args):
        return super(UUIDField, self).to_mongo(value, utils.format_uuid, *args)

    def to_python(self, value, *args):
        return super(UUIDField, self).to_python(value, utils.load_uuid, *args)


class ObjectIdField(Field):

    def to_mongo(self, value, *args):
        return super(ObjectIdField, self).to_mongo(
            value, utils.load_objectid, *args)

    def to_python(self, value, *args):
        return super(ObjectIdField, self).to_python(
            value, utils.load_objectid, *args)
