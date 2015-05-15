import pytz
from StringIO import StringIO

from mongomodel import utils


class FieldValidationError(Exception):

    def __init__(self, message='', instance=None):
        if not message:
            message = 'Not a valid %s' % instance.__class__.__name__
        self.field = getattr(instance, 'name', None)
        if self.field:
            message = '[%s] %s' % (self.field, message)
        super(FieldValidationError, self).__init__(message)


class FieldConfigurationError(Exception):
    pass


class Field(object):
    name = None

    _update_operators = ('$setOnInsert', '$set', '$unset',)

    ValidationError = FieldValidationError
    ConfigurationError = FieldConfigurationError

    def __init__(self, default=None, required=True, auto=False, unique=False,
                 to_python=[], to_mongo=[], **kwargs):
        self.required = required
        self.auto = auto
        self.default = default
        self.unique = unique
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
        except self.ValidationError as e:
            raise e
        except:
            raise self.ValidationError(instance=self)
        return value

    def to_mongo(self, value, *args, **kwargs):
        if value is None:
            if isinstance(self, BooleanField):
                return False
            elif self.required and not self.auto:
                raise self.ValidationError('Value can\'t be none if required',
                                           instance=self)
            else:
                return None
        else:
            if isinstance(self, (TextField, ObjectIdField)) and not value and \
                    not self.required:
                return None
            if kwargs.get('custom', True):
                args = list(args) + self._to_mongo
            return self._process(value, *args)

    def to_python(self, value, *args, **kwargs):
        if value is None:
            return None
        if isinstance(self, (TextField, ObjectIdField)) and not value:
            return None
        if kwargs.get('custom', True):
            args = list(args) + self._to_python
        return self._process(value, *args)

    def validate_update_operator(self, operator, value):
        """
        Checks if the update operator and value is correct, raise
        Field.ValidationError if not.
        """
        # error = self.ValidationError(
        #     'Invalid update operator %s with value %s.' % (operator, value),
        #     instance=self)
        if operator not in self._update_operators:
            raise self.ValidationError(
                'Update operator %s not allowed' % operator,
                instance=self)
        if operator == '$unset' and self.required:
            raise self.ValidationError(
                '$unset operator not allowed in required fields.',
                instance=self)


class ChoicesMixin(object):

    def __init__(self, *args, **kwargs):
        choices = kwargs.get('choices', ())
        if choices:
            if not isinstance(choices, (dict, list, tuple)):
                raise self.ConfigurationError(
                    'choices argument must be an instance of dict|list|tuple')
        self.choices = choices
        super(ChoicesMixin, self).__init__(*args, **kwargs)

    def to_mongo(self, value, *args, **kwargs):
        return super(ChoicesMixin, self).to_mongo(
            value, utils.load_choice, utils.validate_choices, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(ChoicesMixin, self).to_python(
            value, utils.validate_choices, *args, **kwargs)


class TextField(Field):

    def to_mongo(self, value, *args, **kwargs):
        return super(TextField, self).to_mongo(
            value, unicode, utils.validate_text, *args, **kwargs)


class EmailField(TextField):

    def to_mongo(self, value, *args, **kwargs):
        return super(EmailField, self).to_mongo(
            value, utils.validate_email, *args, **kwargs)


class URLField(TextField):

    def __init__(self, https=False, **kwargs):
        self.https = https
        super(URLField, self).__init__(**kwargs)

    def to_mongo(self, value, *args, **kwargs):
        return super(URLField, self).to_mongo(
            value, utils.clean_url, utils.validate_url, *args, **kwargs)


class BooleanField(Field):

    def to_mongo(self, value, *args, **kwargs):
        return super(BooleanField, self).to_mongo(
            value, utils.clean_bool, bool, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(BooleanField, self).to_python(
            value, bool, *args, **kwargs)


BoolField = BooleanField


class IntegerField(ChoicesMixin, Field):
    _update_operators = ('$inc', '$mul', '$setOnInsert', '$set', '$unset',
                         '$min', '$max', '$bit',)

    def to_mongo(self, value, *args, **kwargs):
        return super(IntegerField, self).to_mongo(
            value, float, int, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(IntegerField, self).to_python(
            value, float, int, *args, **kwargs)


IntField = IntegerField


class FloatField(Field):
    _update_operators = ('$inc', '$mul', '$setOnInsert', '$set', '$unset',
                         '$min', '$max',)

    def to_mongo(self, value, *args, **kwargs):
        return super(FloatField, self).to_mongo(value, float, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(FloatField, self).to_python(value, float, *args, **kwargs)


class JSONField(Field):

    def to_mongo(self, value, *args, **kwargs):
        return super(JSONField, self).to_mongo(
            value, lambda x, _: utils.encode_json(x), *args, **kwargs)


class DictField(Field):

    def to_mongo(self, value, *args, **kwargs):
        return super(DictField, self).to_mongo(value, dict, *args, **kwargs)


class DateTimeField(Field):
    _update_operators = ('$setOnInsert', '$set', '$min', '$max',
                         '$currentDate',)

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

    def to_mongo(self, value, *args, **kwargs):
        return super(DateTimeField, self).to_mongo(
            value, utils.load_datetime, utils.validate_timezone,
            utils.set_timezone, utils.isodate, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(DateTimeField, self).to_python(
            value, utils.load_datetime, *args, **kwargs)

    def validate_update_operator(self, operator, value):
        super(DateTimeField, self).validate_update_operator(operator, value)
        if operator == '$currentDate' and value not in (True,
                                                        {'$type': 'date'}):
            raise self.ValidationError(
                'DateTimeField $currentDate operator requires field value as '
                'True or {"$type": "date"}',
                instance=self)


class DateField(Field):

    def to_mongo(self, value, *args, **kwargs):
        return super(DateField, self).to_mongo(
            value, utils.load_date, utils.isodate, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(DateField, self).to_python(
            value, utils.load_date, *args, **kwargs)


class TimestampField(Field):
    _update_operators = ('$setOnInsert', '$set', '$min', '$max',
                         '$currentDate',)

    def __init__(self, format=int, **kwargs):
        if format not in (float, int,):
            raise self.ConfigurationError(
                'timestamp format %s not valid' % format)
        self.format = format
        super(TimestampField, self).__init__(**kwargs)

    def to_mongo(self, value, *args, **kwargs):
        return super(TimestampField, self).to_mongo(
            value, self.load_timestamp, self.format, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(TimestampField, self).to_python(
            value, utils.timestamp_to_datetime, *args, **kwargs)

    def validate_update_operator(self, operator, value):
        super(TimestampField, self).validate_update_operator(operator, value)
        if operator == '$currentDate' and value != {'$type': 'timestamp'}:
            raise self.ValidationError(
                'TimestampField $currentDate operator requires field value as '
                '{"$type": "timestamp"}',
                instance=self)


class UUIDField(Field):

    def __init__(self, format='hex', unique=True, **kwargs):
        if format not in ('hex', 'int', 'urn', 'str',):
            raise self.ConfigurationError('UUID format %s not valid' % format)
        self.format = format
        super(UUIDField, self).__init__(unique, **kwargs)

    def to_mongo(self, value, *args, **kwargs):
        return super(UUIDField, self).to_mongo(
            value, utils.format_uuid, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(UUIDField, self).to_python(
            value, utils.load_uuid, *args, **kwargs)


class ObjectIdField(Field):

    def to_mongo(self, value, *args, **kwargs):
        return super(ObjectIdField, self).to_mongo(
            value, utils.load_objectid, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(ObjectIdField, self).to_python(
            value, utils.load_objectid, *args, **kwargs)


class ListField(Field):
    _update_operators = ('$setOnInsert', '$set', '$unset', '$', '$addToSet',
                         '$pop', '$pullAll', '$pull', '$pushAll', '$push',)
    _update_modifiers = ('$each', '$slice', '$sort', '$position',)
    _projection = None

    def __init__(self, field, **kwargs):
        if not field or not isinstance(field, Field):
            raise self.ConfigurationError('%s.field must be a Field instance.'
                                          % self.__class__.__name__)
        self.field = field
        super(ListField, self).__init__(**kwargs)

    def to_mongo(self, value, *args, **kwargs):
        return super(ListField, self).to_mongo(
            value, utils.list_to_mongo, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(ListField, self).to_python(
            value, utils.list_to_python, *args, **kwargs)

    def validate_update_operator(self, operator, value):
        # Possible cases:
        # 1) { operator: { name : value | [value] } }
        # 2) { operator: { name.$ : value | [value] } }
        # 3) { operator: { name : { modifier : value | [value] } } }
        # 4) { operator: { name.$.xfield : xvalue } }
        #
        # After convert from short to extended query (in model):
        # 1) { operator: { name : value | [value] } }
        # 2) { operator: { name : { $ : value | [value] } }
        # 3) { operator: { name : { modifier : value | [value] } } }
        # 4) { operator: { name : { $ : { xfield : xvalue } } }
        #
        # Validation:
        # 1, 2, 3) value must be an instance of self.field
        # 3) modifier must be in self._update_modifiers
        # 4) self.field must be an instance of EmbeddedDocumentField
        # 4) xfield must be in self.field._meta.fields
        # 4) xvalue must be an instance of self.field
        # 1) value | [value]
        # 2) { $ : value | [value] }
        # 3) { modifier : value | [value] }
        # 4) { $ : { xfield : xvalue } }

        # print
        # print 'ListField.validate_update_operator'
        # print operator
        # print value

        super(ListField, self).validate_update_operator(operator, value)
        # Is a dict with self.field data (EmbeddedDoc) or a dict like 2, 3, 4).
        if isinstance(value, dict) and \
                value.keys()[0] not in ('$',) + self._update_modifiers:
            value = self.field.document_class(**value)
        if isinstance(value, self.field.document_class):
            # 1) Validate value.
            # TODO: not happy with this.
            # TODO: don't validate? value must be a valid self.fiend.document_class
            # self.field.validate_update_operator(operator, value)
            pass
        elif isinstance(value, list):
            # 1) Validate each value in list.
            for i in value:
                self.field.validate_update_operator(operator, i)
        elif isinstance(value, dict):
            # 2, 3, 4)
            for modifier, value in value.items():
                # TODO: value may be a list?
                if isinstance(self.field, EmbeddedDocumentField):
                    # print
                    # print 'validate EmbeddedDocumentField of a ListField'
                    document = self.field.document_class()
                    document.validate_update_query({operator: value})


class SetField(ListField):

    def to_mongo(self, value, *args, **kwargs):
        return super(SetField, self).to_mongo(
            value, set, list, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(SetField, self).to_python(value, set, *args, **kwargs)


class EmbeddedDocumentField(Field):
    _projection = None

    def __init__(self, document_class, **kwargs):
        self.document_class = document_class  # TODO: validate
        super(EmbeddedDocumentField, self).__init__(**kwargs)

    def __set__(self, instance, value):
        if isinstance(value, self.document_class):
            value = value._data
        super(EmbeddedDocumentField, self).__set__(instance, value)

    def to_mongo(self, value, *args, **kwargs):
        if value is None:
            if self.required:
                raise self.ValidationError('Value can\'t be none if required',
                                           instance=self)
        else:
            if value != {}:
                value = self.document_class(**value).to_mongo()
            return value

    def to_python(self, value, *args, **kwargs):
        if value is not None:
            if value != {}:
                value = self.document_class(**value).to_python()
            return value


class BinaryFileField(Field):

    def to_mongo(self, value, *args, **kwargs):
        return super(BinaryFileField, self).to_mongo(
            value, utils.load_binary_file, *args, **kwargs)

    def to_python(self, value, *args, **kwargs):
        return super(BinaryFileField, self).to_python(
            value, utils.decode_json, *args, **kwargs)


class LocalFileField(Field):

    def __init__(self, media_root, media_url='/media/', **kwargs):
        self.media_root = media_root
        self.media_url = media_url
        super(LocalFileField, self).__init__(**kwargs)

    def to_mongo(self, value, *args, **kwargs):
        return super(LocalFileField, self).to_mongo(
            value, utils.load_local_file, *args, **kwargs)
