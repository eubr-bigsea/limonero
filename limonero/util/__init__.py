# -*- coding: utf-8 -*-
import decimal
from datetime import datetime
from json import JSONEncoder


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        return JSONEncoder.default(self, obj)
