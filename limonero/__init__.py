from datetime import date, datetime
import decimal

from flask.json import JSONEncoder


class CustomJSONEncoder(JSONEncoder):
    def __init__(self, **kwargs):
        kwargs['indent'] = None
        super().__init__(**kwargs)
    def default(self, o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        elif isinstance(o, (bytes, decimal.Decimal)):
            return str(o)
        return super(CustomJSONEncoder, self).default(o)
