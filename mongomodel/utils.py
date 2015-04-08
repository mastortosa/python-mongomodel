import os
import json
import re
import time
import uuid
from datetime import datetime, date
from StringIO import StringIO

from bson.objectid import ObjectId
from bson.binary import Binary
import dateutil.parser


def json_default_encode(x):
    if isinstance(x, (datetime, date)):
        return x.isoformat()
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, (set, tuple)):
        return list(x)
    # TODO: Model and Document


def json_encode(x):
    return json.dumps(x, default=json_default_encode)


def json_decode(x):
    return json.loads(x)  # TODO


# Functions to use in Field.to_mongo and Field.to_python

def validate_choices(value, instance):
    if instance.choices:
        choices = instance.choices
        if isinstance(choices, (tuple, list)):
            choices = dict(choices)
        if value not in choices.keys():
            raise instance.ValidationError('Value not in field choices',
                                           instance=instance)
    return value


def load_choice(value, instance):
    if instance.choices:
        choices = instance.choices
        if isinstance(choices, (tuple, list)):
            choices = dict(choices)
        # Get the value in the proper field format.
        if value in choices.values():
            value = next(k for k, v in choices.items() if v == value)
    return value


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


def clean_bool(value, instance):
    if value == 'on':
        value = True
    elif value == 'off':
        value = False
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


def load_binary_file(value, instance):
    """
    Returns a json encoded dict with file content_type, filename and body.
    Encode file in base64 before serialize the dict. `body` must be a string
    from file|StringIO.read() or tornado|django request file.
    """
    # First time the file is uploaded the body must be base64-encoded.
    try:
        return Binary(json.dumps(value))
    except UnicodeDecodeError:
        value['body'] = value['body'].encode('base64')
        return Binary(json.dumps(value))


def decode_json(value, instance):
    return json.loads(value)


def load_local_file(value, instance):
    # Create file from {filename, body, content_type} and return
    # {url, path, content_type}.
    if 'body' in value:
        file_ = open(os.path.join(instance.media_root, value['filename']), 'w')
        file_.write(value['body'])
        file_.close()
        value = {'url': os.path.join(instance.media_url, value['filename']),
                 'path': os.path.join(instance.media_root, value['filename']),
                 'content_type': value['content_type']}
    return value


# Model and document utils.

def get_sort_list(ordering):
    """
    Returns sort options valid for a mongodb query.
    ordering: list of key (`key` for direction:1, `-key` for direction:-1)
    sort: a list of (key, direction) pairs specifying the sort order.
    """
    sort = []
    for i in ordering:
        if i.startswith('-'):
            kd = (i[1:], -1)
        else:
            kd = (i, 1)
        sort.append(kd)
    return sort
