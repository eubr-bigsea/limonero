from datetime import date, datetime
import decimal

from flask.json import JSONEncoder


class CustomJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        elif isinstance(o, (bytes, decimal.Decimal)):
            return str(o)
        return super(CustomJSONEncoder, self).default(o)
