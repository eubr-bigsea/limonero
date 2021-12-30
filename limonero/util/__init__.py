# -*- coding: utf-8 -*-
import decimal
import json
import os
import unicodedata
import collections
from datetime import datetime
from json import JSONEncoder

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        return JSONEncoder.default(self, obj)


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')

HdfsExtraParameters = collections.namedtuple('HdfsExtraParameters', 
    ['user', 'use_hostname', 'resources'])
# Python 3.6
HdfsExtraParameters.__new__.__defaults__ = (None, None)

def parse_hdfs_extra_params(data):
    if data is not None:
        return json.loads(data, 
            object_hook=lambda d: HdfsExtraParameters(**d))
    return None

def get_hdfs_conf(jvm, extra_params, config):
    conf = jvm.org.apache.hadoop.conf.Configuration()
    use_hostname = ((extra_params is not None and 
        extra_params.use_hostname) or 
        config.get('dfs.client.use.datanode.hostname', True))

    if extra_params is not None:
        if extra_params.user:
            # This is the only way to set HDFS user name
            os.environ["HADOOP_USER_NAME"] = extra_params.user
            jvm.java.lang.System.setProperty("HADOOP_USER_NAME", extra_params.user)

        Path = jvm.org.apache.hadoop.fs.Path
        if extra_params.resources:
            for resource in extra_params.resources:
                conf.addResource(Path(resource))

    conf.set('dfs.client.use.datanode.hostname',
                         "true" if use_hostname else "false")
    return conf
                  
