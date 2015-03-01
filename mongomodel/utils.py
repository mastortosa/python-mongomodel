import json
from datetime import datetime, date


def json_default_encode(x):
    """Allow date and datetime JSON encoding."""
    if isinstance(x, (datetime, date)):
        return x.isoformat()


def json_encode(x):
    return json.dumps(x, default=json_default_encode)


def json_decode(x):
    return json.loads(x)  # TODO
