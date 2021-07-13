# -*- coding: utf-8 -*-
import logging
import uuid
from gettext import gettext

from py4j.compat import bytearray2
from urllib.parse import urlparse
from limonero.py4j_init import create_gateway

WRONG_HDFS_CONFIG = gettext(
    "Limonero HDFS access not correctly configured (see "
    "config 'dfs.client.use.datanode.hostname')")

log = logging.getLogger(__name__)


def get_tmp_path(jvm, hdfs, parsed, filename):
    """
    Temporary directory used to upload files to HDFS
    """
    tmp_dir = '{}/tmp/upload/{}'.format(parsed.path.replace('//', '/'),
                                        filename)
    tmp_path = jvm.org.apache.hadoop.fs.Path(tmp_dir)
    if not hdfs.exists(tmp_path):
        hdfs.mkdirs(tmp_path)
    return tmp_path


def create_hdfs_chunk(chunk_number, filename, storage, use_hostname,
                      gateway_port):
    parsed = urlparse(storage.url)

    conf, jvm = create_gateway_and_hdfs_conf(use_hostname, gateway_port)

    str_uri = '{proto}://{host}:{port}'.format(
        proto=parsed.scheme, host=parsed.hostname, port=parsed.port)
    uri = jvm.java.net.URI(str_uri)

    hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    tmp_path = get_tmp_path(jvm, hdfs, parsed, filename)
    chunk_filename = "{tmp}/{file}.part{part:09d}".format(
        tmp=tmp_path.toString(), file=filename, part=chunk_number)

    # time.sleep(1)
    chunk_path = jvm.org.apache.hadoop.fs.Path(chunk_filename)
    return chunk_path, hdfs


def write_chunk(jvm, chunk_number, filename, storage, file_data,
                conf):
    """
    Writes a single chunk in HDFS. Chunks are provided by the interface and
    are blocks of data (binary)
    """
    storage_url = storage.url if storage.url[-1] != '/' \
        else storage.url[:-1]
    parsed = urlparse(storage_url)

    if parsed.scheme == 'file':
        str_uri = '{proto}://{path}'.format(
            proto=parsed.scheme, path=parsed.path)
    else:
        str_uri = '{proto}://{host}:{port}'.format(
            proto=parsed.scheme, host=parsed.hostname,
            port=parsed.port)

    uri = jvm.java.net.URI(str_uri)

    hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    log.info('================== %s', uri)
    tmp_path = get_tmp_path(jvm, hdfs, parsed, filename)

    chunk_filename = "{tmp}/{file}.part{part:09d}".format(
        tmp=tmp_path.toString(), file=filename, part=chunk_number)
    chunk_path = jvm.org.apache.hadoop.fs.Path(chunk_filename)

    output_stream = hdfs.create(chunk_path)
    block = bytearray2(file_data)

    output_stream.write(block, 0, len(block))
    output_stream.close()

    # Checks if all file's parts are present
    full_path = tmp_path
    list_iter = hdfs.listFiles(full_path, False)
    counter = 0
    while list_iter.hasNext():
        counter += 1
        list_iter.next()

    return file_data, hdfs, str_uri, tmp_path, counter


def create_gateway_and_hdfs_conf(use_datanode, gateway_port):
    """
    Stats JVM and define HDFS configuration used to upload data.
    """
    gateway = create_gateway(log, gateway_port)
    jvm = gateway.jvm
    conf = jvm.org.apache.hadoop.conf.Configuration()
    conf.set('dfs.client.use.datanode.hostname',
             "true" if use_datanode else "false")
    return conf, jvm


def merge_chunks(conf, filename, full_path, hdfs, jvm, str_uri,
                 instance_name):
    """
    Merge already uploaded chunks in a single file using HDFS API.
    """
    final_filename = '{}_{}'.format(uuid.uuid4().hex, filename)
    # time to merge all files
    target_path = jvm.org.apache.hadoop.fs.Path('{}/{}/{}/{}'.format(
        str_uri, '/limonero/data', instance_name, final_filename))

    result_code = 200
    result = None
    if hdfs.exists(target_path):
        result = {'status': 'error', 'message': gettext('File already exists')}
        result_code = 500
    jvm.org.apache.hadoop.fs.FileUtil.copyMerge(
        hdfs, full_path, hdfs, target_path, True, conf, None)
    return result_code, result, target_path
