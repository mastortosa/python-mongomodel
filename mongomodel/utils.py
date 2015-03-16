import json
from datetime import datetime, date

from bson.objectid import ObjectId


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


def validate_update_query(model, update):  # TEMP
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
