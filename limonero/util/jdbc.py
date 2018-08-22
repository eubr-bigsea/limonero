# -*- coding: utf-8 -*-
from limonero.models import DataType


def get_mysql_data_type(d, dtype):
    if d[dtype] in ('TINY', 'SHORT', 'INT24', 'YEAR', 'BIT'):
        final_type = DataType.INTEGER
    elif d[dtype] in ('LONGLONG',):
        final_type = DataType.LONG
    elif d[dtype] in ('NEWDATE',):
        final_type = DataType.DATETIME
    elif d[dtype] in ('NEWDECIMAL',):
        final_type = DataType.DECIMAL
    elif d[dtype] in ('DECIMAL', 'FLOAT', 'DOUBLE', 'DATE', 'TIME', 'DATETIME',
                      'LONG', 'TIMESTAMP'):
        final_type = d[dtype]
    else:
        final_type = DataType.CHARACTER
    return final_type
