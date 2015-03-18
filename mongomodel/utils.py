import json
import re
import time
import uuid
from datetime import datetime, date

from bson.objectid import ObjectId
import dateutil.parser


def json_default_encode(x):
    """Allow date and datetime JSON encoding."""
    if isinstance(x, (datetime, date)):
        return x.isoformat()
    if isinstance(x, ObjectId):
        return str(x)


def json_encode(x):
    return json.dumps(x, default=json_default_encode)


def json_decode(x):
    return json.loads(x)  # TODO


def validate_update_query(model, update):
    # TODO: check if is required to $unset.
    # TODO: recursive in EmbeddedDocumentField.
    # TODO: check for key.subkey
    # operation = { name : { ( key | key.sub) : ( val | operation ) }, ... }
    doc = model()
    data = {}
    for operator, kv in update.items():
        mongo_kv = {}
        for k, v in kv.items():
            if k not in doc:
                raise ValueError('%s is not a field' % k)
            mongo_kv[k] = doc._meta.fields[k].to_mongo(v)
        data[operator] = mongo_kv
    return data


# Functions to use in Field.to_mongo and Field.to_python

def validate_text(value, instance):
    if instance.required and not value.strip():
        raise instance.ValidationError('Value can\'t be empty',
                                       instance=instance)
    return value


def validate_email(value, instance):
    ixat = value.index('@')
    ixdot = value.rindex('.')
    if not (ixat > 1 and ixdot > ixat + 2 and ixdot + 2 < len(value)):
        raise instance.ValidationError(instance=instance)
    return value


def clean_url(value, instance):
    if not (value.startswith('http://') or
            value.startswith('https://')):
        return '%s://%s' % (('https' if instance.https else 'http'), value)
    return value


def validate_url(value, instance):
    regex = r'^(http|https)://(.*)?((\.\w{2})|(\.\w{3}))$'
    if not re.match(regex, value):
        raise instance.ValidationError(instance=instance)
    return value


def list_to_mongo(value, instance):
    return [instance.field.to_mongo(i) for i in value]


def list_to_python(value, instance):
    return [instance.field.to_python(i) for i in value]


def load_datetime(value, instance):
    if isinstance(value, (str, unicode)):
        value = dateutil.parser.parse(value)
    elif isinstance(value, date):
        value = datetime.combine(value, datetime.min.time())
    return value


def validate_timezone(value, instance):
    if instance.timezone and value.tzinfo is None:
        raise instance.ValidationError(instance=instance)
    return value


def set_timezone(value, instance):
    if instance.timezone:
        value = value.replace(tzinfo=instance.timezone)
    return value


def isodate(value, instance):
    return value.isoformat()


def load_date(value, instance):
    if isinstance(value, (str, unicode)):
        value = dateutil.parser.parse(value).date()
    elif isinstance(value, datetime):
        value = value.date()
    return value


def load_timestamp(value, instance):
    if isinstance(value, datetime):
        value = time.mktime(value.timetuple())
    return value


def timestamp_to_datetime(value, instance):
    return datetime.fromtimestamp(value)


def format_uuid(value, instance):
    if instance.format == 'str':
        value = value.__str__()
    else:
        value = getattr(value, instance.format)
    return value


def load_uuid(value, instance):
    if isinstance(value, (str, unicode)):
        value = uuid.UUID(value)
    else:
        value = uuid.UUID(int=value)
    return value


def load_objectid(value, instance):
    if isinstance(value, (str, unicode)):
        value = ObjectId(value)
    return value
