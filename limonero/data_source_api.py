# -*- coding: utf-8 -*-}
import codecs
import decimal
import logging
import math
import operator
import re
import uuid
import zipfile
from ast import literal_eval
from collections import defaultdict
from io import BytesIO
from io import StringIO

import pymysql
from backports import csv
from dbfpy import dbf
from flask import g as flask_g
from flask import request, Response, current_app
from flask import stream_with_context
from flask.views import MethodView
from flask_babel import gettext
from flask_restful import Resource
from py4j.compat import bytearray2
from py4j.protocol import Py4JJavaError
from requests import compat as req_compat
from sqlalchemy import inspect
from sqlalchemy.orm import subqueryload, joinedload
from sqlalchemy.sql.elements import or_
from werkzeug.exceptions import NotFound

from app_auth import requires_auth, User
from limonero.py4j_init import create_gateway
from limonero.util import strip_accents
from limonero.util.jdbc import get_mysql_data_type
from schema import *

_ = gettext
log = logging.getLogger(__name__)

WRONG_HDFS_CONFIG = gettext(
    "Limonero HDFS access not correctly configured (see "
    "config 'dfs.client.use.datanode.hostname')")

INVALID_FORMAT_ERROR = gettext(
    "At least one value for attribute '{attr}' is incompatible "
    "with the type '{type}': {v}"
)

DATE_FORMATS = {
    '%m/%d/%Y': 'MM/dd/yyyy', '%m-%d-%Y': 'MM-dd-yyyy',
    '%m.%d.%Y': 'MM.dd.yyyy',
    '%Y/%m/%d': 'yyyy/MM/dd', '%Y-%m-%d': 'yyyy-MM-dd',
    '%Y.%m.%d': 'yyyy.MM.dd',
    '%d/%m/%Y': 'dd/MM/yyyy', '%d-%m-%Y': 'dd-MM-yyyy ',
    '%d.%m.%Y': 'dd.MM.yyyy',
}
TIME_FORMATS = {
    '': '',
    'T%H:%M:%S.%fZ': 'Thh:mm:ss.Z',
    ' %H:%M:%S': 'hh:mm:ss',
}


def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != '':
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


def _filter_by_permissions(data_sources, permissions):
    if flask_g.user.id not in (0, 1):  # It is not a inter service call
        sub_query = DataSourcePermission.query.with_entities(
            DataSourcePermission.id).filter(
            DataSourcePermission.permission.in_(permissions),
            DataSourcePermission.user_id == flask_g.user.id)
        conditions = or_(
            DataSource.is_public,
            DataSource.user_id == flask_g.user.id,
            DataSource.id.in_(sub_query)
        )
        data_sources = data_sources.filter(conditions)
    return data_sources


class DataSourceListApi(Resource):
    """ REST API for listing class DataSource """

    @staticmethod
    @requires_auth
    def get():
        result, result_code = {'status': 'ERROR',
                               'message': gettext('Internal error')}, 500
        # noinspection PyBroadException

        try:
            simple = False
            if request.args.get('simple') != 'true':
                only = None
            else:
                simple = True
                only = ('id', 'name', 'description', 'created', 'tags',
                        'format', 'user_name', 'permissions', 'user_id',
                        'privacy_aware')

            if request.args.get('fields'):
                only = tuple(
                    [x.strip() for x in request.args.get('fields').split(',')])

            possible_filters = {'enabled': bool, 'format': None, 'user_id': int}
            data_sources = DataSource.query
            for f, transform in possible_filters.items():
                data_sources = apply_filter(data_sources, request.args, f,
                                            transform, lambda field: field)

            query = request.args.get('query') or request.args.get('name')
            if query:
                data_sources = data_sources.filter(or_(
                    DataSource.name.ilike('%%{}%%'.format(query)),
                    DataSource.description.ilike('%%{}%%'.format(query)),
                    DataSource.tags.ilike('%%{}%%'.format(query))
                ))
            if not simple:
                data_sources = data_sources.options(
                    joinedload(DataSource.attributes))
            data_sources = _filter_by_permissions(
                data_sources, PermissionType.values())

            formats = [f for f in request.args.get('formats', '').split(',')
                       if f in DataSourceFormat.values()]
            if formats:
                data_sources = data_sources.filter(DataSource.format.in_(
                    formats))
            sort = request.args.get('sort', 'name')
            if sort not in ['name', 'id', 'user_id', 'user_name']:
                sort = 'id'

            sort_option = getattr(DataSource, sort)
            if request.args.get('asc', 'true') == 'false':
                sort_option = sort_option.desc()

            data_sources = data_sources.order_by(sort_option)

            page = request.args.get('page') or '1'

            if request.args.get('list') is None:
                if page is not None and page.isdigit():
                    page_size = int(request.args.get('size', 20))
                    page = int(page)
                    if page > -1:
                        pagination = data_sources.paginate(page, page_size,
                                                           False)
                        if pagination.total < page_size and page > 1:
                            pagination = data_sources.paginate(1, page_size,
                                                               False)
                    else:
                        # No pagination
                        pagination = data_sources
                    result = {
                        'data': DataSourceListResponseSchema(
                            many=True, only=only,
                            exclude=('permissions',)).dump(
                            pagination.items).data,
                        'pagination': {
                            'page': page, 'size': page_size,
                            'total': pagination.total,
                            'pages': int(
                                math.ceil(1.0 * pagination.total / page_size))}
                    }
            else:
                only = ('id', 'name', 'tags')
                result = DataSourceListResponseSchema(
                    many=True, only=only).dump(data_sources).data
            db.session.commit()
            result_code = 200

        except NotFound:
            result_code = 404
            result = {'data': []}
        except Exception as ex:
            log.exception(ex.message)

        return result, result_code

    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR",
            message=gettext("Missing json in the request body")), 400
        if request.json is not None:
            request_schema = DataSourceCreateRequestSchema()
            response_schema = DataSourceItemResponseSchema(
                exclude=('url', 'storage.url'))
            json_data = request.json

            json_data['user_id'] = json_data.get('user_id', flask_g.user.id)
            json_data['user_login'] = json_data.get('user_login',
                                                    flask_g.user.login)
            json_data['user_name'] = json_data.get('user_name',
                                                   flask_g.user.name)

            form = request_schema.load(json_data)

            if form.errors:
                result, result_code = dict(
                    status="ERROR", message=gettext("Validation error"),
                    errors=form.errors), 400
            else:
                try:
                    data_source_id = None
                    data_source = None
                    if request.args.get('mode') == 'overwrite':
                        # Try to retrieve existing data source
                        data_source = DataSource.query.filter(
                            DataSource.url == form.data.url).first()
                        if data_source:
                            data_source_id = data_source.id
                            data_source = form.data
                            data_source.id = data_source_id
                            db.session.merge(data_source)

                    if form.data.format == DataSourceFormat.JDBC:
                        storage = Storage.query.get(form.data.storage_id)
                        form.data.url = storage.url
                    elif form.data.format in [DataSourceFormat.TEXT]:
                        # Attributes are not supported
                        form.data.attributes = []

                    if data_source_id is None:
                        data_source = form.data
                        db.session.add(data_source)

                    db.session.commit()
                    result, result_code = {'data': response_schema.dump(
                        data_source).data}, 200
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message=gettext(
                                                   "Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()

        return result, result_code


class DataSourceDetailApi(Resource):
    """ REST API for a single instance of class DataSource """

    @staticmethod
    @requires_auth
    def get(data_source_id):

        names_only = request.args.get('attributes_name') == 'true'

        data_sources = DataSource.query
        data_source = _filter_by_permissions(data_sources,
                                             PermissionType.values())

        data_source = data_source.filter(DataSource.id == data_source_id)
        data_source = data_source.first()

        if data_source is not None:
            if names_only:
                attributes = {'attributes': [{'name': attr.name} for attr in
                                             data_source.attributes]}
                return attributes
            else:
                return DataSourceItemResponseSchema().dump(data_source).data
        else:
            return dict(status="ERROR",
                        message=gettext("%(type)s not found.",
                                        type=gettext('Data source'))), 404

    @staticmethod
    @requires_auth
    def delete(data_source_id):
        result, result_code = dict(
            status="ERROR",
            message=gettext("%(type)s not found.",
                            type=gettext('Data source'))), 404

        filtered = _filter_by_permissions(
            DataSource.query, [PermissionType.MANAGE, PermissionType.WRITE])
        data_source = filtered.filter(DataSource.id == data_source_id).first()
        if data_source is not None:
            try:
                data_source.enabled = False
                db.session.add(data_source)
                db.session.commit()
                result, result_code = dict(
                    status="OK",
                    message=gettext("%(what)s was successfuly deleted",
                                    what=gettext('Data source'))), 200
            except Exception as e:
                log.exception('Error in DELETE')
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               "Internal error")), 500
                if current_app.debug:
                    result['debug_detail'] = e.message
                db.session.rollback()
        return result, result_code

    @staticmethod
    @requires_auth
    def patch(data_source_id):
        result = dict(status="ERROR", message=gettext('Insufficient data'))
        result_code = 400
        json_data = request.json or json.loads(request.data)
        if json_data:
            request_schema = partial_schema_factory(
                DataSourceCreateRequestSchema)

            # FIXME: Remove this code, ignore attribute_privacy
            for attr in json_data.get('attributes', ''):
                del attr['attribute_privacy']

            # Ignore missing fields to allow partial updates
            form = request_schema.load(json_data, partial=True)
            response_schema = DataSourceItemResponseSchema()

            if not form.errors:
                try:
                    form.data.id = data_source_id
                    filtered = _filter_by_permissions(
                        DataSource.query,
                        [PermissionType.MANAGE, PermissionType.WRITE])
                    data_source = filtered.filter(
                        DataSource.id == data_source_id).first()

                    if form.data.format in [DataSourceFormat.TEXT]:
                        # Attributes are not supported
                        form.data.attributes = []
                    if data_source is not None:
                        data_source = db.session.merge(form.data)
                        db.session.commit()
                        result, result_code = dict(
                            status="OK",
                            message=gettext("%(what)s was successfuly updated",
                                            what=gettext('Data source')),
                            data=response_schema.dump(data_source).data), 200
                    else:
                        result = dict(status="ERROR",
                                      message=gettext("%(type)s not found.",
                                                      type=gettext(
                                                          'Data source')))
                except Exception as e:
                    current_app.logger.exception(e)
                    log.exception('Error in PATCH')
                    result, result_code = dict(status="ERROR",
                                               message=gettext(
                                                   "Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()
            else:
                result = dict(status="ERROR", message=gettext('Invalid data'),
                              errors=form.errors)
        return result, result_code


class DataSourcePermissionApi(Resource):
    """ REST API for sharing a DataSource """

    @staticmethod
    @requires_auth
    def post(data_source_id, user_id):
        result, result_code = dict(
            status="ERROR",
            message=gettext('Missing json in the request body')), 400

        if request.json is not None:
            form = request.json
            to_validate = ['permission', 'user_name', 'user_login']
            error = False
            for check in to_validate:
                if check not in form or form.get(check, '').strip() == '':
                    result, result_code = dict(
                        status="ERROR", message=gettext('Validation error'),
                        errors={'Missing': check}), 400
                    error = True
                    break
                if check == 'permission' and form.get(
                        'permission') not in PermissionType.values():
                    result, result_code = dict(
                        status="ERROR", message=gettext('Validation error'),
                        errors={'Invalid': check}), 400
                    error = True
                    break
            if not error:
                try:
                    filtered = _filter_by_permissions(
                        DataSource.query, [PermissionType.MANAGE])
                    data_source = filtered.filter(
                        DataSource.id == data_source_id).first()

                    if data_source is not None:
                        conditions = [DataSourcePermission.data_source_id ==
                                      data_source_id,
                                      DataSourcePermission.user_id == user_id]
                        permission = DataSourcePermission.query.filter(
                            *conditions).first()

                        action_performed = 'Added'
                        if permission is not None:
                            action_performed = 'Updated'
                            permission.permission = form['permission']
                        else:
                            permission = DataSourcePermission(
                                data_source=data_source, user_id=user_id,
                                user_name=form['user_name'],
                                user_login=form['user_login'],
                                permission=form['permission'])

                        db.session.add(permission)
                        db.session.commit()
                        result, result_code = {'message': action_performed,
                                               'status': 'OK'}, 200
                    else:
                        result, result_code = dict(
                            status="ERROR",
                            message=gettext("%(type)s not found.",
                                            type=gettext('Data source'))), 404
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message=gettext(
                                                   "Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()

        return result, result_code

    @staticmethod
    @requires_auth
    def delete(data_source_id, user_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext("%(type)s not found.",
                                                   type=gettext(
                                                       'Data source'))), 404

        filtered = _filter_by_permissions(DataSource.query,
                                          [PermissionType.MANAGE])
        data_source = filtered.filter(DataSource.id == data_source_id).first()
        if data_source is not None:
            permission = DataSourcePermission.query.filter(
                DataSourcePermission.data_source_id == data_source_id,
                DataSourcePermission.user_id == user_id).first()
            if permission is not None:
                try:
                    db.session.delete(permission)
                    db.session.commit()
                    result, result_code = dict(
                        status="OK",
                        message=gettext("%(what)s was successfuly deleted",
                                        what=gettext('Data source'))), 200
                except Exception as e:
                    log.exception('Error in DELETE')
                    result, result_code = dict(status="ERROR",
                                               message=gettext(
                                                   "Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()
        return result, result_code


class DataSourceUploadApi(Resource):
    """ REST API for upload a DataSource """

    @staticmethod
    def _get_tmp_path(jvm, hdfs, parsed, filename):
        tmp_dir = u'{}/tmp/upload/{}'.format(parsed.path.replace('//', '/'),
                                             filename)
        tmp_path = jvm.org.apache.hadoop.fs.Path(tmp_dir)
        if not hdfs.exists(tmp_path):
            hdfs.mkdirs(tmp_path)
        return tmp_path

    @staticmethod
    @requires_auth
    def get():
        # noinspection PyBroadException
        try:
            identifier = request.args.get('resumableIdentifier', type=str)
            filename = request.args.get('resumableFilename', type=str)
            chunk_number = request.args.get('resumableChunkNumber', type=int)

            result, result_code = 'OK', 200

            storage_id = request.args.get('storage_id', type=int)
            if not all([storage_id, identifier, filename, chunk_number]):
                # Parameters are missing or invalid
                result, result_code = {'status': 'ERROR', 'message': gettext(
                    'Missing required parameters')}, 400
            else:
                storage = Storage.query.get(storage_id)
                parsed = req_compat.urlparse(storage.url)

                gateway = create_gateway(log, current_app.gateway_port)
                jvm = gateway.jvm

                str_uri = '{proto}://{host}:{port}'.format(
                    proto=parsed.scheme, host=parsed.hostname, port=parsed.port)
                uri = jvm.java.net.URI(str_uri)

                conf = jvm.org.apache.hadoop.conf.Configuration()
                conf.set('dfs.client.use.datanode.hostname',
                         "true" if current_app.config.get(
                             'dfs.client.use.datanode.hostname',
                             True) else "false")

                log.error('=======> %s', uri)
                hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

                tmp_path = DataSourceUploadApi._get_tmp_path(
                    jvm, hdfs, parsed, filename)

                chunk_filename = "{tmp}/{file}.part{part:09d}".format(
                    tmp=tmp_path.toString(), file=filename, part=chunk_number)
                current_app.logger.debug('Creating chunk: %s', chunk_filename)

                # time.sleep(1)
                chunk_path = jvm.org.apache.hadoop.fs.Path(chunk_filename)
                if not hdfs.exists(chunk_path):
                    # Let resumable.js know this chunk does not exists
                    #  and needs to be uploaded
                    result, result_code = {'status': 'OK',
                                           'message': gettext('Not found')}, 404

            return result, result_code
        except Py4JJavaError as java_ex:
            log.exception('Java error')
            if 'Could not obtain block' in java_ex.java_exception.getMessage():
                return {'status': 'ERROR',
                        'message': WRONG_HDFS_CONFIG}, 400
            else:
                return {'status': 'ERROR',
                        'message': gettext('Internal error')}, 400
        except:
            raise

    @staticmethod
    @requires_auth
    def post():
        try:
            identifier = request.args.get('resumableIdentifier', type=str)
            filename = request.args.get('resumableFilename', type=unicode)
            chunk_number = request.args.get('resumableChunkNumber', type=int)
            total_chunks = request.args.get('resumableTotalChunks', type=int)
            total_size = request.args.get('resumableTotalSize', type=int)
            storage_id = request.args.get('storage_id', type=int)

            result, result_code = 'OK', 200
            if not all([identifier, filename, chunk_number]):
                # Parameters are missing or invalid
                result, result_code = {'status': 'ERROR', 'message': gettext(
                    'Missing required parameters')}, 400
            else:
                storage = Storage.query.get(storage_id)
                storage_url = storage.url if storage.url[-1] != '/' \
                    else storage.url[:-1]
                parsed = req_compat.urlparse(storage_url)

                gateway = create_gateway(log, current_app.gateway_port)
                jvm = gateway.jvm

                if parsed.scheme == 'file':
                    str_uri = '{proto}://{path}'.format(
                        proto=parsed.scheme, path=parsed.path)
                else:
                    str_uri = '{proto}://{host}:{port}'.format(
                        proto=parsed.scheme, host=parsed.hostname,
                        port=parsed.port)
                uri = jvm.java.net.URI(str_uri)

                conf = jvm.org.apache.hadoop.conf.Configuration()
                conf.set('dfs.client.use.datanode.hostname',
                         "true" if current_app.config.get(
                             'dfs.client.use.datanode.hostname',
                             True) else "false")

                hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
                log.info('================== %s', uri)

                tmp_path = DataSourceUploadApi._get_tmp_path(
                    jvm, hdfs, parsed, filename)

                chunk_filename = u"{tmp}/{file}.part{part:09d}".format(
                    tmp=tmp_path.toString(), file=filename, part=chunk_number)
                current_app.logger.debug('Wrote chunk: %s', chunk_filename)

                chunk_path = jvm.org.apache.hadoop.fs.Path(chunk_filename)

                file_data = request.get_data()
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

                if counter == total_chunks:
                    final_filename = '{}_{}'.format(uuid.uuid4().hex, filename)

                    # time to merge all files
                    instance = current_app.config.get('instance', 'unnamed')

                    target_path = jvm.org.apache.hadoop.fs.Path(
                        u'{}/{}/{}/{}'.format(str_uri,
                                              u'/limonero/data', instance,
                                              final_filename))
                    if hdfs.exists(target_path):
                        result = {'status': 'error',
                                  'message': gettext('File already exists')}
                        result_code = 500

                    jvm.org.apache.hadoop.fs.FileUtil.copyMerge(
                        hdfs, full_path, hdfs, target_path, True, conf,
                        None)

                    # noinspection PyBroadException
                    try:
                        user = getattr(flask_g, 'user')
                    except:
                        user = User(id=1, login='admin',
                                    email='admin@lemonade',
                                    first_name='admin',
                                    last_name='admin',
                                    locale='en')

                    extension = filename[-3:].lower()
                    if extension == 'csv':
                        ds_format = DataSourceFormat.CSV
                    elif extension == 'json':
                        ds_format = DataSourceFormat.JSON
                    elif extension == 'xml':
                        ds_format = DataSourceFormat.XML_FILE
                    elif extension == 'txt':
                        ds_format = DataSourceFormat.TEXT
                    else:
                        ds_format = DataSourceFormat.UNKNOWN

                    ds = DataSource(
                        format=ds_format,
                        name=filename,
                        storage_id=storage.id,
                        description=_('Imported in Limonero'),
                        enabled=True,
                        url=target_path.toString(),
                        estimated_size_in_mega_bytes=total_size / 1024.0 ** 2,
                        user_id=user.id,
                        user_login=user.login,
                        user_name='{} {}'.format(
                            user.first_name.encode('utf8'),
                            user.last_name.encode('utf8')).strip())

                    # gateway.shutdown()
                    db.session.add(ds)

                    if filename[-4:] in ['.csv', '.CSV', '.tsv', '.TSV']:
                        # noinspection PyBroadException
                        try:
                            # try to infer the field delimiter
                            count_delimiters = defaultdict(int)
                            for ch in file_data:
                                if ch in [',', ';', '\t']:
                                    count_delimiters[ch] += 1
                            sorted_delim = sorted(count_delimiters.items(),
                                                  key=operator.itemgetter(1),
                                                  reverse=True)
                            delim = sorted_delim[0][0] if sorted_delim else ','
                            ds.is_first_line_header = True
                            ds.attribute_delimiter = delim
                            ds.format = DataSourceFormat.CSV
                            DataSourceUploadApi._try_infer_schema(ds, delim)
                        except:
                            # in case of error, save the upload information
                            db.session.commit()
                    else:
                        db.session.commit()
                    response_schema = DataSourceItemResponseSchema()
                    result = {'status': 'OK',
                              'data': response_schema.dump(ds).data}

            return result, result_code, {
                'Content-Type': 'application/json; charset=utf-8'}
        except Py4JJavaError as java_ex:
            if 'Could not obtain block' in java_ex.java_exception.getMessage():
                return {'status': 'ERROR',
                        'message': WRONG_HDFS_CONFIG}, 400
            log.exception('Java error')
        except:
            raise

    @staticmethod
    def _try_infer_schema(ds, delim):
        options = {
            'use_header': True,
            'delimiter': delim.decode('utf-8')
        }
        DataSourceInferSchemaApi.infer_schema(ds, options)


class DataSourceDownload(MethodView):
    """ Entry point for downloading a DataSource """

    # noinspection PyUnresolvedReferences
    @staticmethod
    @requires_auth
    def get(data_source_id):

        result = json.dumps(
            {'status': 'ERROR', 'message': gettext('Internal error')})
        result_code = 500

        data_source = DataSource.query.get_or_404(ident=data_source_id)

        parsed = req_compat.urlparse(data_source.url)

        gateway = create_gateway(log, current_app.gateway_port)
        jvm = gateway.jvm

        if parsed.scheme == 'file':
            def do_download():
                total = 0
                done = False
                with open(parsed.path) as f:
                    while not done:
                        lido = f.read(4096)
                        total += len(lido)
                        if len(lido) != 4096:
                            done = True
                        yield lido

            name = '{}.{}'.format(data_source.name.replace(' ', '-'),
                                  data_source.format.lower())
            result = Response(stream_with_context(
                do_download()), mimetype='text/csv')

            result.headers[
                'Cache-Control'] = 'no-cache, no-store, must-revalidate'
            result.headers['Pragma'] = 'no-cache'
            result.headers["Content-Disposition"] = \
                "attachment; filename={}".format(name)
            result_code = 200
        else:
            str_uri = '{proto}://{host}:{port}'.format(
                proto=parsed.scheme, host=parsed.hostname, port=parsed.port)
            try:
                uri = jvm.java.net.URI(str_uri)

                conf = jvm.org.apache.hadoop.conf.Configuration()
                conf.set('dfs.client.use.datanode.hostname',
                         "true" if current_app.config.get(
                             'dfs.client.use.datanode.hostname',
                             True) else "false")

                hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

                chunk_path = jvm.org.apache.hadoop.fs.Path(parsed.path)
                if not hdfs.exists(chunk_path):
                    result, result_code = gettext("%(type)s not found.",
                                                  type=gettext(
                                                      'Data source')), 404
                else:
                    buf = jvm.java.nio.ByteBuffer.allocate(4096)
                    input_in = hdfs.open(chunk_path)

                    def do_download():
                        total = 0
                        done = False
                        while not done:
                            lido = input_in.read(buf)
                            total += lido
                            buf.position(0)
                            if lido != 4096:
                                done = True
                                yield bytes(buf.array())[:lido]
                            else:
                                yield bytes(buf.array())

                    name = '{}.{}'.format(data_source.name.replace(' ', '-'),
                                          data_source.format.lower())
                    result = Response(stream_with_context(
                        do_download()), mimetype='text/csv')

                    result.headers[
                        'Cache-Control'] = 'no-cache, no-store, must-revalidate'
                    result.headers['Pragma'] = 'no-cache'
                    result.headers["Content-Disposition"] = \
                        "attachment; filename={}".format(name)
                    result_code = 200
            except Py4JJavaError as java_ex:
                if 'Could not obtain block' in \
                        java_ex.java_exception.getMessage():
                    return {'status': 'ERROR',
                            'message': WRONG_HDFS_CONFIG}, 400
                log.exception('Java error')
            except Exception as e:
                result = json.dumps(
                    {'status': 'ERROR', 'message': gettext('Internal error')})
                result_code = 500
                log.exception(e.message)

        return result, result_code


class DataSourceInferSchemaApi(Resource):
    @staticmethod
    def _infer_schema_from_db(ds, options):
        pass

    @staticmethod
    def infer_schema(ds, options):
        parsed = req_compat.urlparse(ds.url)

        if ds.format in (DataSourceFormat.JDBC,):
            parsed = req_compat.urlparse(ds.url)
            qs = dict(x.split('=') for x in parsed.query.split('&'))
            # Supported DB: mysql

            if parsed.scheme == 'mysql':
                from pymysql.constants import FIELD_TYPE
                ft = pymysql.constants.FIELD_TYPE
                d = {getattr(ft, k): k for k in dir(ft) if
                     not k.startswith('_')}
                try:
                    fix_limit = re.compile(r'\sLIMIT\s+(\d+)')

                    if ds.command is None or ds.command.strip() == '':
                        raise ValueError(gettext(
                            'Data source does not have a command specified'))
                    cmd = fix_limit.sub('', ds.command)
                    with pymysql.connect(
                            host=parsed.hostname,
                            port=parsed.port or '3306',
                            user=qs.get('user'),
                            passwd=qs.get('password'),
                            db=parsed.path[1:]) as cursor:
                        cursor.execute('{} LIMIT 0'.format(cmd))

                        old_attrs = Attribute.query.filter(
                            Attribute.data_source_id == ds.id)
                        old_attrs.delete(synchronize_session=False)
                        for (name, dtype, size, to_ignore, precision, scale,
                             nullable) in cursor.description:
                            final_type = get_mysql_data_type(d, dtype)
                            if d[dtype] == 'BLOB':
                                precision = None
                            attr = Attribute(name=name, type=final_type,
                                             size=size, precision=precision,
                                             scale=scale, nullable=nullable)
                            attr.data_source = ds
                            attr.feature = False
                            attr.label = False
                            db.session.add(attr)
                        db.session.commit()
                except Exception:
                    raise ValueError(gettext('Could not connect to database'))
            else:
                raise ValueError(
                    gettext('Unsupported database: %(what)s',
                            what=parsed.scheme))

        elif ds.format in (DataSourceFormat.PARQUET,):
            str_uri = '{proto}://{host}:{port}'.format(
                proto=parsed.scheme, host=parsed.hostname, port=parsed.port)

            gateway = create_gateway(log, current_app.gateway_port)
            jvm = gateway.jvm

            hadoop_pkg = jvm.org.apache.hadoop
            parquet_pkg = jvm.org.apache.parquet
            uri = jvm.java.net.URI(str_uri)

            conf = hadoop_pkg.conf.Configuration()
            conf.set('dfs.client.use.datanode.hostname',
                     "true" if current_app.config.get(
                         'dfs.client.use.datanode.hostname',
                         True) else "false")

            path = hadoop_pkg.fs.Path(ds.url)
            no_filter = \
                parquet_pkg.format.converter.ParquetMetadataConverter.NO_FILTER
            parquet_reader = parquet_pkg.hadoop.ParquetFileReader
            meta_data = parquet_reader.readFooter(conf, path, no_filter)
            schema = meta_data.getFileMetaData().getSchema()
            final_schema = json.loads(meta_data.toJSON(meta_data))

            types = {
                'UTF8': DataType.CHARACTER,
                'BINARY': DataType.CHARACTER,
                'DOUBLE': DataType.DOUBLE,
                'TIMESTAMP_MILLIS': DataType.DATETIME,
                'DATE': DataType.DATE,
                'FLOAT': DataType.FLOAT,
                'INT32': DataType.INTEGER,
                'INT64': DataType.LONG,
                'CHARACTER': DataType.TEXT,
                'DECIMAL': DataType.DECIMAL
            }
            old_attrs = Attribute.query.filter(
                Attribute.data_source_id == ds.id)
            old_attrs.delete(synchronize_session=False)

            for definition in final_schema['fileMetaData']['schema']['columns']:
                primitive_type = definition['primitiveType']
                if '__index_level' not in primitive_type['name']:
                    data_type = types[primitive_type['originalType'] or
                                      primitive_type['primitiveTypeName']]
                    attr = Attribute(name=primitive_type['name'],
                                     nullable=True,
                                     enumeration=False,
                                     type=data_type,
                                     feature=False, label=False, )
                    attr.data_source = ds
                    attr.feature = False
                    attr.label = False
                    db.session.add(attr)
            db.session.commit()

        elif ds.format in (DataSourceFormat.CSV, DataSourceFormat.SHAPEFILE):

            str_uri = '{proto}://{host}:{port}'.format(
                proto=parsed.scheme, host=parsed.hostname, port=parsed.port)
            conf, hadoop_pkg, hdfs, jvm, path, buffered_reader = [None] * 6
            if parsed.scheme == 'hdfs':
                # noinspection PyUnresolvedReferences
                gateway = create_gateway(log, current_app.gateway_port)
                jvm = gateway.jvm

                hadoop_pkg = jvm.org.apache.hadoop
                uri = jvm.java.net.URI(str_uri)

                conf = hadoop_pkg.conf.Configuration()
                conf.set('dfs.client.use.datanode.hostname',
                         "true" if current_app.config.get(
                             'dfs.client.use.datanode.hostname',
                             True) else "false")

                hdfs = hadoop_pkg.fs.FileSystem.get(uri, conf)
                path = hadoop_pkg.fs.Path(ds.url)

            elif parsed.scheme == 'file':
                str_uri = '{proto}://{path}'.format(
                    proto=parsed.scheme, path=parsed.path)

            if ds.format == DataSourceFormat.CSV:
                try:
                    use_header = options.get('use_header',
                                             False) or ds.is_first_line_header

                    delimiter = (options.get('delimiter', ',') or ',').encode(
                        'latin1')
                    # If there is a delimiter set in data_source, use it instead
                    if ds.attribute_delimiter:
                        delimiter = ds.attribute_delimiter

                    special_delimiters = {'{tab}': u'\t',
                                          '{new_line \\n}': u'\n',
                                          '{new_line \\r\\n}': u'\r\n'
                                          }
                    delimiter = special_delimiters.get(delimiter, delimiter)

                    if isinstance(delimiter, str):
                        delimiter = delimiter.decode('utf8')

                    if ds.treat_as_missing:
                        missing_values = ds.treat_as_missing.split(',')
                    else:
                        missing_values = []

                    encoding = 'utf8'
                    if parsed.scheme == 'file':
                        encoding = ds.encoding or 'utf8'
                        buffered_reader = codecs.open(parsed.path, 'rb',
                                                      encoding=encoding)
                    elif parsed.scheme == 'hdfs':
                        buffered_reader, encoding = \
                            DataSourceInferSchemaApi._get_reader(
                                conf, ds, hadoop_pkg, hdfs, jvm, path)

                    quote_char = options.get('quote_char', None)
                    quote_char = quote_char.encode(
                        encoding) if quote_char else None

                    # Read 100 lines, may be enough to infer schema
                    lines = StringIO()
                    for i in range(1000):
                        if parsed.scheme == 'file':
                            line = buffered_reader.readline()
                        else:
                            line = buffered_reader.readLine()
                        if line is None:
                            break

                        line = line.encode(encoding).decode('utf8')
                        line = line.replace('\0', '')
                        lines.write(line)
                        lines.write(u'\n')

                    buffered_reader.close()
                    lines.seek(0)
                    if quote_char:
                        csv_reader = csv.reader(lines,
                                                delimiter=delimiter,
                                                quotechar=quote_char.decode(
                                                    'utf8'))
                    else:
                        csv_reader = csv.reader(lines, delimiter=delimiter)

                    attrs = []
                    # noinspection PyBroadException
                    attrs = DataSourceInferSchemaApi._get_csv_attributes(
                        attrs, csv_reader, use_header, missing_values)

                    old_attrs = Attribute.query.filter(
                        Attribute.data_source_id == ds.id)
                    old_attrs.delete(synchronize_session=False)

                    for attr in attrs:
                        if attr.type is None:
                            attr.type = DataType.CHARACTER
                        if attr.type == DataType.CHARACTER:
                            if attr.size > 1000:
                                attr.type = DataType.TEXT
                        attr.data_source = ds
                        attr.feature = False
                        attr.label = False
                        db.session.add(attr)

                    db.session.commit()
                except Exception as ex:
                    raise ValueError(
                        gettext('Cannot infer the schema: %(what)s', what=ex))
            elif ds.format == DataSourceFormat.SHAPEFILE:
                old_attrs = Attribute.query.filter(
                    Attribute.data_source_id == ds.id)
                old_attrs.delete(synchronize_session=False)

                if ds.url.endswith('.zip'):
                    zip_input_stream = jvm.java.io.BufferedInputStream(
                        hdfs.open(path))
                    z = zipfile.ZipFile(BytesIO(
                        jvm.org.apache.commons.io.IOUtils.toByteArray(
                            zip_input_stream)))
                    dbf_name = next(
                        (f for f in z.namelist() if f.endswith('dbf')), None)
                    if dbf_name is None:
                        raise ValueError(
                            gettext('Cannot infer the schema for shapefile '
                                    '(invalid Zip file content).'))
                    else:
                        dbf_io = BytesIO(z.read(dbf_name))
                else:
                    path2 = hadoop_pkg.fs.Path(re.sub('.shp$', '.dbf', ds.url))
                    input_stream_dbf = jvm.java.io.BufferedInputStream(
                        hdfs.open(path2))

                    dbf_content = jvm.org.apache.commons.io.IOUtils.toByteArray(
                        input_stream_dbf)
                    dbf_io = BytesIO(dbf_content)

                handler = dbf.Dbf(dbf_io)

                types = {
                    'C': DataType.CHARACTER,
                    'B': DataType.DOUBLE,
                    'D': DataType.DATETIME,
                    'F': DataType.FLOAT,
                    'I': DataType.INTEGER,
                    'L': DataType.LONG,
                    'M': DataType.TEXT,
                    'N': DataType.FLOAT,
                    'O': DataType.DOUBLE,
                    'V': DataType.CHARACTER,
                    'Y': DataType.DECIMAL
                }
                for definition in handler.fieldDefs:
                    data_type = types[definition.typeCode]
                    attr = Attribute(name=definition.name, nullable=True,
                                     enumeration=False, type=data_type,
                                     feature=False, label=False,
                                     size=definition.length,
                                     scale=definition.decimalCount)
                    attr.data_source = ds
                    attr.feature = False
                    attr.label = False
                    db.session.add(attr)
                db.session.commit()
        else:
            # gateway.shutdown()
            raise ValueError(
                gettext('Cannot infer the schema for format %(format)s',
                        format=ds.format))
            # gateway.shutdown()

    @staticmethod
    @requires_auth
    def post(data_source_id):

        result = {'status': 'OK'}
        ds = DataSource.query.get_or_404(data_source_id)
        request_body = {}
        if request.data:
            request_body = json.loads(request.data)

        # noinspection PyBroadException
        try:
            DataSourceInferSchemaApi.infer_schema(ds, request_body)
        except UnicodeEncodeError:
            log.exception('Invalid CSV encoding')
            result = {'status': 'ERROR',
                      'message': gettext('Invalid CSV encoding')}, 400
        except ValueError as ve:
            result = {'status': 'ERROR', 'message': ve.message}, 400
        except Py4JJavaError as java_ex:
            if 'Could not obtain block' in java_ex.java_exception.getMessage():
                return {'status': 'ERROR',
                        'message': WRONG_HDFS_CONFIG}, 400
            log.exception('Java error')
            result = {'status': 'ERROR',
                      'message': gettext('Internal error, try later')}, 400
        except Exception:
            log.exception('Invalid CSV format')
            db.session.rollback()
            return {'status': 'ERROR',
                    'message': 'Internal error, try later'}, 400
        return result

    @staticmethod
    def _get_reader(conf, ds, hadoop_pkg, hdfs, jvm, path):
        # Support to Gzip'ed files. Spark also supports gzip transparently.
        codec_factory = hadoop_pkg.io.compress.CompressionCodecFactory(conf)
        codec = codec_factory.getCodec(path)
        if codec is None:
            input_stream = hdfs.open(path)
        else:
            input_stream = codec.createInputStream(hdfs.open(path))

        # Handle UTF-8 with BOM
        # See https://stackoverflow.com/a/44862536/1646932
        bom_input_stream = jvm.org.apache.commons.io.input.BOMInputStream(
            input_stream)
        if bom_input_stream.getBOM() is not None:
            encoding = bom_input_stream.getBOM().getCharsetName()
        else:
            encoding = ds.encoding or 'UTF-8'
        buffered_reader = jvm.java.io.BufferedReader(
            jvm.java.io.InputStreamReader(bom_input_stream, encoding))
        return buffered_reader, encoding

    @staticmethod
    def _get_csv_attributes(attrs, csv_reader, use_header, missing_values):
        for row in csv_reader:
            if use_header and len(attrs) == 0:
                attrs = DataSourceInferSchemaApi._get_header(row)
            else:
                if len(attrs) == 0:
                    attrs = DataSourceInferSchemaApi._get_default_header(
                        row)
                for i, value in enumerate(row):
                    if i < len(attrs):
                        if value is None or value == '' \
                                or value in missing_values:
                            attrs[i].nullable = True
                        else:
                            if attrs[i].type != DataType.CHARACTER:
                                DataSourceInferSchemaApi._infer_attr(
                                    attrs, i, value)
                            else:
                                attrs[i].size = max(
                                    attrs[i].size, len(value))
        return attrs

    @staticmethod
    def _try_parse(d):
        for df, java_df in DATE_FORMATS.items():
            for hf, java_hf in TIME_FORMATS.items():
                # noinspection PyBroadException
                try:
                    f = ''.join([df, hf])
                    parsed = datetime.datetime.strptime(d, f)
                    return parsed, ''.join([java_df, java_hf])
                except:
                    pass
        raise ValueError("Invalid date")

    @staticmethod
    def _infer_attr(attrs, i, value):
        try:
            v = literal_eval(value)
        except ValueError:
            v = value
        except SyntaxError:
            v = value

        # test if first char is zero to avoid python
        # convertion of octal
        if any([(value[0] == '0' and len(value) > 1 and value[1] != '.'),
                type(v) in [str, unicode]]):
            if type(v) not in [int, float, long]:
                # noinspection PyBroadException
                try:
                    (d, f) = DataSourceInferSchemaApi._try_parse(v)
                    attrs[i].type = DataType.DATETIME
                    attrs[i].format = f
                except:
                    attrs[i].type = DataType.CHARACTER
                    attrs[i].size = len(value)
                    attrs[i].precision = None
                    attrs[i].scale = None
            else:
                attrs[i].type = DataType.CHARACTER
                attrs[i].size = len(value)
                attrs[i].precision = None
                attrs[i].scale = None
        elif type(v) in [int] and -2147483648 < v < 2147483647:
            if attrs[i].type not in [DataType.DECIMAL, DataType.FLOAT,
                                     DataType.LONG]:
                attrs[i].type = DataType.INTEGER
        elif type(v) in [long]:
            if attrs[i].type not in [DataType.DECIMAL, DataType.FLOAT]:
                attrs[i].type = DataType.LONG
        elif type(v) in [float]:
            change_to_str = False
            parts = value.split('.')
            left, right = None, None
            if len(parts) == 2:
                left, right = parts
            elif len(parts) == 1 and parts[0].isdigit():
                left, right = parts[0], ''
            else:
                change_to_str = True
            if not change_to_str:
                attrs[i].type = DataType.DECIMAL
                attrs[i].precision = max(attrs[i].precision,
                                         len(left) + len(right))
                attrs[i].scale = max(attrs[i].scale, len(right))
            else:
                attrs[i].type = DataType.TEXT

    @staticmethod
    def _get_default_header(row):
        attrs = []
        for i, attr in enumerate(row):
            # Default name is attrX
            attrs.append(
                Attribute(name='attr{}'.format(i),
                          nullable=False,
                          enumeration=False))
        return attrs

    @staticmethod
    def _get_header(row):
        attrs = []
        for attr in row:
            attr = strip_accents(attr.strip().replace(' ', '_'))[:100]
            attrs.append(
                Attribute(name=attr, nullable=False, enumeration=False))
        return attrs


class DataSourcePrivacyApi(Resource):
    """ REST API for a managing data source privacy """

    @staticmethod
    @requires_auth
    def get(data_source_id):

        options = subqueryload(DataSource.attributes).subqueryload(
            Attribute.attribute_privacy)

        attr_privacy = DataSource.query \
            .outerjoin(DataSource.attributes) \
            .outerjoin(Attribute.attribute_privacy) \
            .join(DataSource.attributes) \
            .join(Attribute.attribute_privacy) \
            .filter(DataSource.id == data_source_id) \
            .filter(DataSource.enabled) \
            .filter(DataSource.privacy_aware) \
            .options(options) \
            .all()
        if attr_privacy:
            return {'data': DataSourcePrivacyResponseSchema().dump(
                attr_privacy[0], many=False).data}
        else:
            return dict(status="ERROR",
                        message=gettext("%(type)s not found.",
                                        type=gettext('Data source'))), 404

    @staticmethod
    @requires_auth
    def patch(data_source_id):
        result = dict(status="ERROR", message=gettext('Insufficient data'))
        result_code = 400
        json_data = request.json or json.loads(request.data)
        if json_data:
            request_schema = partial_schema_factory(
                DataSourceCreateRequestSchema)
            # Ignore missing fields to allow partial updates
            form = request_schema.load(json_data, partial=True)
            response_schema = DataSourceItemResponseSchema()
            if not form.errors:
                try:
                    form.data.id = data_source_id
                    data_source = db.session.merge(form.data)
                    db.session.commit()

                    if data_source is not None:
                        result, result_code = dict(
                            status="OK",
                            message=gettext("%(what)s was successfuly updated",
                                            what=gettext('Data source')),
                            data=response_schema.dump(data_source).data), 200
                    else:
                        result = dict(
                            status="ERROR",
                            message=gettext("%(type)s not found.",
                                            type=gettext('Data source')))
                except Exception as e:
                    current_app.logger.exception(e)
                    log.exception('Error in PATCH')
                    result, result_code = dict(
                        status="ERROR", message=gettext("Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()
            else:
                result = dict(status="ERROR", message=gettext('Invalid data'),
                              errors=form.errors)
        return result, result_code


class DataSourceSampleApi(Resource):
    @staticmethod
    @requires_auth
    def get(data_source_id):

        data_sources = DataSource.query
        data_source = _filter_by_permissions(data_sources,
                                             PermissionType.values())

        data_source = data_source.filter(
            DataSource.id == data_source_id).first()

        limit = int(request.args.get('limit', 100))
        result, status_code = dict(status='ERROR', message='Not found'), 404

        warnings = []
        if limit > 1000:
            result, status_code = dict(
                status='ERROR',
                message='The maximum number of records allowed is 1000'), 400
        elif data_source is not None:
            treat_as_missing = (data_source.treat_as_missing or '').split(',')
            parsed = req_compat.urlparse(data_source.url)
            if parsed.scheme == 'mysql':
                qs = dict(x.split('=') for x in parsed.query.split('&'))
                fix_limit = re.compile(r'\sLIMIT\s+(\d+)')
                cmd = fix_limit.sub('', data_source.command)

                with pymysql.connect(
                        host=parsed.hostname,
                        port=parsed.port or '3306',
                        user=qs.get('user'),
                        passwd=qs.get('password'),
                        db=parsed.path[1:],
                        charset='UTF8',
                        cursorclass=pymysql.cursors.DictCursor) as cursor:
                    cursor.execute('{} LIMIT {}'.format(cmd, limit))
                    result, status_code = dict(status='OK',
                                               data=cursor.fetchall()), 200
            elif parsed.scheme == 'file':
                # Support JSON and CSV
                if data_source.format == DataSourceFormat.CSV:
                    encoding = data_source.encoding or 'utf8'
                    with codecs.open(parsed.path, 'rb',
                                     encoding=encoding) as csvfile:

                        header = []
                        converters = []
                        if data_source.attributes:
                            csv_params = {
                                'fileobj': csvfile,
                                'delimiter': data_source.attribute_delimiter or ','}
                            if data_source.text_delimiter:
                                csv_params['quoting'] = csv.QUOTE_MINIMAL
                                csv_params[
                                    'quotechar'] = data_source.text_delimiter
                            for attr in data_source.attributes:
                                header.append(attr.name)
                                if attr.type in [DataType.DECIMAL]:
                                    converters.append(decimal.Decimal)
                                elif attr.type in [DataType.DOUBLE,
                                                   DataType.FLOAT]:
                                    converters.append(float)
                                elif attr.type in [DataType.INTEGER,
                                                   DataType.LONG]:
                                    converters.append(int)
                                else:
                                    converters.append(unicode.strip)
                            reader = csv.reader(**csv_params)
                        else:
                            header.append(_('row'))
                            converters.append(unicode.strip)
                            reader = csv.reader(
                                fileobj=csvfile,
                                delimiter=u';')

                        if data_source.is_first_line_header:
                            reader.next()
                        data = []
                        for i, line in enumerate(reader):
                            if i >= limit:
                                break
                            row = {}
                            for h, v, conv in zip(header, line, converters):
                                try:
                                    row[h] = conv(v) if v != '' else ''
                                except decimal.InvalidOperation:
                                    row[h] = gettext(
                                        "<Invalid data>: `{}`".format(v))
                                    warnings.append(h)
                            data.append(row)
                            result, status_code = dict(
                                status='OK', warnings=list(set(warnings)),
                                data=data), 200
                elif data_source.format == DataSourceFormat.JSON:
                    data = []
                    with open(parsed.path) as f:
                        for i, line in enumerate(f):
                            if i >= limit:
                                break
                            data.append(json.loads(line))
                    result, status_code = dict(status='OK',
                                               data=data), 200
                else:
                    result, status_code = dict(
                        status='ERROR',
                        message='Format {} is not supported'.format(
                            data_source.format)), 400
            elif parsed.scheme == 'hdfs':
                gateway = create_gateway(log, current_app.gateway_port)
                jvm = gateway.jvm
                str_uri = '{proto}://{host}:{port}'.format(
                    proto=parsed.scheme, host=parsed.hostname, port=parsed.port)
                try:
                    uri = jvm.java.net.URI(str_uri)

                    conf = jvm.org.apache.hadoop.conf.Configuration()
                    conf.set('dfs.client.use.datanode.hostname',
                             "true" if current_app.config.get(
                                 'dfs.client.use.datanode.hostname',
                                 True) else "false")

                    hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

                    chunk_path = jvm.org.apache.hadoop.fs.Path(parsed.path)
                    if not hdfs.exists(chunk_path):
                        result = ({
                                      'status': 'ERROR',
                                      'message': 'Not found'}, 404)
                    else:
                        input_in = hdfs.open(chunk_path)

                        bom_input_stream = \
                            jvm.org.apache.commons.io.input.BOMInputStream(
                                input_in)

                        if bom_input_stream.getBOM() is not None:
                            encoding = \
                                bom_input_stream.getBOM().getCharsetName()
                        else:
                            encoding = data_source.encoding or 'UTF-8'
                        if parsed.path.endswith('.gz'):
                            buffered_reader = jvm.java.io.BufferedReader(
                                jvm.java.io.InputStreamReader(
                                    jvm.java.util.zip.GZIPInputStream(
                                        bom_input_stream), encoding))
                        else:
                            buffered_reader = jvm.java.io.BufferedReader(
                                jvm.java.io.InputStreamReader(
                                    bom_input_stream, encoding))

                        header = []
                        converters = []
                        csv_buf = StringIO()
                        if data_source.attributes:
                            for attr in data_source.attributes:
                                header.append(attr.name)
                                if attr.type in [DataType.DECIMAL]:
                                    converters.append(decimal.Decimal)
                                elif attr.type in [DataType.DOUBLE,
                                                   DataType.FLOAT]:
                                    converters.append(float)
                                elif attr.type in [DataType.INTEGER,
                                                   DataType.LONG]:
                                    converters.append(int)
                                else:
                                    converters.append(unicode.strip)
                            d = data_source.attribute_delimiter or unicode(',')
                            csv_params = {
                                'fileobj': csv_buf,
                                'delimiter': d}
                            reader = csv.reader(**csv_params)
                        else:
                            header.append(_('row'))
                            converters.append(unicode.strip)
                            reader = csv.reader(
                                fileobj=csv_buf,
                                delimiter=u';')
                        data = []
                        i = 0
                        if data_source.is_first_line_header:
                            buffered_reader.readLine()

                        limit = 40
                        while i < limit:
                            tmp_line = buffered_reader.readLine()
                            if tmp_line:
                                csv_buf.write(tmp_line)
                                csv_buf.write('\n'.decode('utf8'))
                            i += 1

                        csv_buf.seek(0)
                        for line in reader:
                            row = {}
                            for h, v, conv in zip(header, line, converters):
                                # noinspection PyBroadException
                                try:
                                    if v not in treat_as_missing:
                                        row[h] = conv(v)
                                    else:
                                        row[h] = None
                                except:
                                    status_code = 422
                                    return dict(
                                        status='ERROR',
                                        message=INVALID_FORMAT_ERROR.format(
                                            type=str(conv), attr=h,
                                            v=str(v)
                                        )), status_code
                            data.append(row)

                        result, status_code = dict(status='OK',
                                                   data=data), 200
                except Py4JJavaError as java_ex:
                    if 'Could not obtain block' in \
                            java_ex.java_exception.getMessage():
                        return {'status': 'ERROR',
                                'message': WRONG_HDFS_CONFIG}, 400
                    log.exception('Java error')
                except Exception as e:
                    result = {'status': 'ERROR',
                              'message': 'Internal error:',
                              'details': str(e)}
                    status_code = 500
                    log.exception(e.message)
            else:
                return dict(status="ERROR",
                            message="Unsupported protocol {}".format(
                                parsed.scheme)), 400
        else:
            return dict(status="ERROR", message="Not found"), 404
        return result, status_code


# Events
# noinspection PyUnusedLocal
@event.listens_for(inspect(DataSource).relationships['attributes'], 'append')
@event.listens_for(inspect(DataSource).relationships['attributes'], 'remove')
def receive_append_or_remove(target, value, initiator):
    target.updated = datetime.datetime.utcnow()


# noinspection PyUnusedLocal
@event.listens_for(Attribute, 'after_update')
def receive_attribute_change(mapper, connection, target):
    if target.data_source:
        target.data_source.updated = datetime.datetime.utcnow()
