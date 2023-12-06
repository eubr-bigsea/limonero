# -*- coding: utf-8 -*-
from sqlalchemy.event import listens_for
import codecs
import collections
import csv
import datetime
import decimal
import gzip
import io
import json
import logging
import math
import operator
import pyarrow.parquet as pq
import re
import uuid
import zipfile
from io import BytesIO, StringIO
from urllib.parse import urlparse
from pathlib import Path


import pymysql
from flask import Response, current_app
from flask import g as flask_g
from flask import request, stream_with_context
from flask.views import MethodView
from flask_babel import gettext
from flask_restful import Resource
from marshmallow.exceptions import ValidationError
from py4j.protocol import Py4JJavaError
from pyarrow import fs
from sqlalchemy import inspect
from sqlalchemy.orm import joinedload, subqueryload
from sqlalchemy.sql.elements import or_
from werkzeug.exceptions import NotFound

import limonero.hdfs_util as hu
from limonero.util import get_hdfs_conf, parse_hdfs_extra_params, strip_accents
from limonero.util.jdbc import get_hive_data_type, get_mysql_data_type

from .app_auth import User, requires_auth
from .schema import (DataSourceListResponseSchema, DataSourceItemResponseSchema, 
                     DataSourceCreateRequestSchema, DataSourcePrivacyResponseSchema, partial_schema_factory)
from .models import (Attribute, AttributePrivacy, DataType, db, DataSource, DataSourcePermission, DataSourceFormat, 
                     DataSourceInitialization, 
                     PermissionType, Storage)

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

SPECIAL_DELIMITERS = {'{tab}': '\t',
                      '{new_line \\n}': '\n',
                      '{new_line \\r\\n}': '\r\n'
                     }

def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != '':
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


def is_logged_user_owner_or_admin(data_source):
    return (int(data_source.user_id) == int(flask_g.user.id) or
            'ADMINISTRATOR' in flask_g.user.permissions)


def _filter_by_permissions(data_sources, permissions, consider_public=True):
    if flask_g.user.id not in (0, 1):  # It is not a inter service call
        sub_query = DataSourcePermission.query.with_entities(
            DataSourcePermission.id).filter(
            DataSourcePermission.permission.in_(permissions),
            DataSourcePermission.user_id == flask_g.user.id)
        conditions = [
            DataSource.user_id == flask_g.user.id,
            DataSource.id.in_(sub_query)
        ]
        if consider_public:
            conditions.append(DataSource.is_public)
        data_sources = data_sources.filter(or_(*conditions))
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
                        'privacy_aware', 'download_token')

            if request.args.get('fields'):
                only = tuple(
                    [x.strip() for x in request.args.get('fields').split(',')])

            possible_filters = {'enabled': bool, 'format': None, 
                                'user_id': int, 'workflow_id': int}
            data_sources = DataSource.query
            for f, transform in list(possible_filters.items()):
                data_sources = apply_filter(data_sources, request.args, f,
                                            transform, lambda field: field)
            ds_id = request.args.get('id')
            if ds_id:
                data_sources = data_sources.filter(
                        DataSource.id == int(request.args.get('id')))

            lookup = request.args.get('lookup')
            if lookup and lookup in ['1', 'true', True, 1]:
                data_sources = data_sources.filter(DataSource.is_lookup)

            use_in_workflow = request.args.get('uiw')
            if use_in_workflow and use_in_workflow in ['1', 'true', True, 1]:
                data_sources = data_sources.filter(DataSource.use_in_workflow)

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
                data_sources, list(PermissionType.values()))

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
                            pagination.items),
                        'pagination': {
                            'page': page, 'size': page_size,
                            'total': pagination.total,
                            'pages': int(
                                math.ceil(1.0 * pagination.total // page_size))}
                    }
            else:
                only = ('id', 'name', 'tags')
                result = DataSourceListResponseSchema(
                    many=True, only=only).dump(data_sources)
            db.session.commit()
            result_code = 200

        except NotFound:
            result_code = 404
            result = {'data': []}
        except Exception as ex:
            log.exception(str(ex))

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

            try:
                entity = request_schema.load(json_data)
                data_source_id = None
                data_source = None
                if request.args.get('mode') == 'overwrite':
                    # Try to retrieve existing data source
                    data_source = DataSource.query.filter(
                        DataSource.url == entity.url).first()
                    if data_source:
                        data_source_id = data_source.id
                        data_source = entity
                        data_source.id = data_source_id
                        db.session.merge(data_source)

                if entity.format == DataSourceFormat.JDBC:
                    storage = Storage.query.get(entity.storage_id)
                    entity.url = storage.url
                elif entity.format in [DataSourceFormat.TEXT]:
                    # Attributes are not supported
                    entity.attributes = []

                if data_source_id is None:
                    data_source = entity
                    data_source.initialization = \
                        DataSourceInitialization.NO_INITIALIZED
                    db.session.add(data_source)

                db.session.commit()
                result, result_code = {'data': response_schema.dump(
                    data_source)}, 200
            except ValidationError as e:
                result = dict(status="ERROR", message=gettext('Invalid data'),
                          errors=e.messages)
                result_code = 400
            except Exception as e:
                log.exception('Error in POST')
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               "Internal error")), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
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
                                             list(PermissionType.values()))

        data_source = data_source.filter(DataSource.id == data_source_id)
        data_source = data_source.first()

        if data_source is not None:
            if names_only:
                attributes = {'attributes': [{'name': attr.name} for attr in
                                             data_source.attributes]}
                return attributes
            else:
                if not is_logged_user_owner_or_admin(data_source):
                    exclude = ['storage.url', 'storage.client_url',
                            'storage.extra_params', 'url' ]
                else:
                    exclude = []
                return DataSourceItemResponseSchema(exclude=exclude).dump(
                        data_source)
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
            if not is_logged_user_owner_or_admin(data_source):
                result, result_code = dict(
                    status="ERROR",
                    message=gettext(
                        "You are not authorized to perform this action")), 401
            else:
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
                        result['debug_detail'] = str(e)
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
            # for attr in json_data.get('attributes', ''):
            #    del attr['attribute_privacy']

            response_schema = DataSourceItemResponseSchema()

            try:
                # Ignore missing fields to allow partial updates
                entity = request_schema.load(json_data, partial=True)

                entity.id = data_source_id
                filtered = _filter_by_permissions(
                    DataSource.query,
                    [PermissionType.MANAGE, PermissionType.WRITE])
                data_source = filtered.filter(
                    DataSource.id == data_source_id).first()

                if entity.format in [DataSourceFormat.TEXT]:
                    # Attributes are not supported
                    entity.attributes = []
                if data_source is not None:
                    if not is_logged_user_owner_or_admin(data_source):
                        result, result_code = dict(
                            status="ERROR",
                            message=gettext(
                                "You are not authorized "
                                "to perform this action")), 400
                    elif entity.is_lookup and len(entity.attributes) != 2:
                        result, result_code = dict(
                            status="ERROR",
                            message=gettext(
                                "Lookup tables can only "
                                "have 2 attributes (id and description)")), 401
                    else:
                        data_source = db.session.merge(entity)
                        db.session.commit()
                        result = {
                                'status': 'OK',
                                'message': 
                                gettext("%(what)s was successfuly updated",
                                                   what=gettext('Data source')),
                                'data': response_schema.dump(data_source)
                        }
                        result_code = 200
                else:
                    result = {
                        'status': 'ERROR',
                        'message': gettext("%(type)s not found.",
                                           type=gettext('Data source'))}
            except ValidationError as e:
                result = dict(status="ERROR", message=gettext('Invalid data'),
                          errors=e.messages)
                result_code = 400
            except Exception as e:
                current_app.logger.exception(e)
                log.exception('Error in PATCH')
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               "Internal error")), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
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
                        'permission') not in list(PermissionType.values()):
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
                        result['debug_detail'] = str(e)
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
                        result['debug_detail'] = str(e)
                    db.session.rollback()
        return result, result_code

class DataSourceUploadApi(Resource):
    """ REST API for upload a DataSource """

    @staticmethod
    def _get_tmp_path(hdfs, parsed, filename):
        final_path = parsed.path.replace('//', '/')
        tmp_dir = f'{final_path}/tmp/upload/{filename}'

        str_uri = f'{parsed.scheme}://{parsed.hostname}'
        # if parsed.port:
        #    hdfs = fs.HadoopFileSystem(str_uri, port=int(parsed.port))
        #else:
        #    hdfs = fs.HadoopFileSystem(str_uri)

        if not hu.exists(hdfs, tmp_dir):
            hu.mkdirs(hdfs, tmp_dir)
        return tmp_dir

    @staticmethod
    def _get_ids(request):
        identifier = request.args.get('resumableIdentifier', type=str)
        filename = request.args.get('resumableFilename', type=str)
        chunk_number = request.args.get('resumableChunkNumber', type=int)
        return identifier, filename, chunk_number

    @staticmethod
    @requires_auth
    def get():
        # noinspection PyBroadException
        try:
            result, result_code = 'OK', 200
            identifier, filename, chunk_number = (
                DataSourceUploadApi._get_ids(request))

            storage_id = request.args.get('storage_id', type=int)
            if not all([storage_id, identifier, filename, chunk_number]):
                # Parameters are missing or invalid
                result, result_code = {'status': 'ERROR', 'message': gettext(
                    'Missing required parameters')}, 400
            else:
                storage = Storage.query.get(storage_id)
                if storage.type != 'HDFS':
                    raise ValueError(
                        'Usupported storage type: {}'.format(storage.type))
                parsed = urlparse(storage.url)

                if parsed.scheme == 'file':
                    chunk_filename = f'/tmp/{filename}.part{chunk_number:09d}'
                    chunk_path = Path(chunk_filename)
                    if not chunk_path.exists():
                        result, result_code = {'status': 'OK',
                                           'message': gettext('Not found')}, 404
                elif parsed.scheme == 'hdfs':
                    str_uri = hu.get_parsed_uri(parsed, False)
                    extra_params = parse_hdfs_extra_params(storage.extra_params)
                    if parsed.port:                
                        hdfs = fs.HadoopFileSystem(str_uri, user=extra_params.user or 'hadoop', 
                            port=int(parsed.port))
                    else:
                        hdfs = fs.HadoopFileSystem(str_uri, user=extra_params.user or 'hadoop',)
                    tmp_path = DataSourceUploadApi._get_tmp_path(
                        hdfs, parsed, filename)

                    chunk_filename = f'{tmp_path}/{filename}.part{chunk_number:09d}'
                    current_app.logger.debug(f'Creating chunk: {chunk_filename}')

                    # time.sleep(1)
                    if not hu.exists(hdfs, chunk_filename):
                        # Let resumable.js know this chunk does not exists
                        #  and needs to be uploaded
                        result, result_code = {'status': 'OK',
                                           'message': gettext('Not found')}, 404

            return result, result_code
        except Exception as e:
            log.exception(e)
            return {'status': 'ERROR',
                        'message': gettext('Internal error')}, 400

    @staticmethod
    @requires_auth
    def post():
        try:
            identifier, filename, chunk_number = (
                DataSourceUploadApi._get_ids(request))

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
                parsed = urlparse(storage_url)

                if parsed.scheme == 'file':
                    str_uri = '{proto}://{path}'.format(
                        proto=parsed.scheme, path=parsed.path)
                else:
                    str_uri = hu.get_parsed_uri(parsed, False)

                extra_params = parse_hdfs_extra_params(storage.extra_params)
                # conf = get_hdfs_conf(jvm, extra_params, current_app.config)
                hadoop_user = extra_params.user or 'hadoop'
                if parsed.port:                
                    hdfs = fs.HadoopFileSystem(str_uri, user=hadoop_user, 
                        port=int(parsed.port))
                else:
                    hdfs = fs.HadoopFileSystem(str_uri, user=hadoop_user,)

                tmp_path = DataSourceUploadApi._get_tmp_path(
                        hdfs, parsed, filename)

                chunk_filename = f'{tmp_path}/{filename}.part{chunk_number:09d}'
                current_app.logger.debug('Writing chunk: %s', chunk_filename)

                file_data = request.get_data()
                hu.write(hdfs, chunk_filename, file_data)

                # Checks if all file's parts are present
                if chunk_number == total_chunks:
                    final_filename = f'{uuid.uuid4().hex}_{filename}'

                    # time to merge all files
                    parsed_path = parsed.path or ''
                    if parsed_path.endswith('/'):
                        parsed_path = parsed_path[:-1]
                    if parsed_path:
                        target_path = (
                            f'/{parsed_path.strip("/")}/limonero/data/{final_filename}')
                    else:
                        target_path = (f'/limonero/data/{final_filename}')
                    # target_path = (f'/limonero/data/{final_filename}')

                    if hu.exists(hdfs, target_path):
                        result = {'status': 'error',
                                  'message': gettext(
                                  'A file with same name already exists. Try to upload the file again.')}
                        result_code = 500
                    hu.copy_merge(hdfs, tmp_path, target_path, filename, 
                                  total_chunks)

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
                        description=gettext('Imported in Limonero'),
                        enabled=True,
                        url=target_path if parsed.scheme == 'file' 
                            else f'{storage_url.strip("/")}/limonero/data/{final_filename}',
                        estimated_size_in_mega_bytes=total_size / 1024.0 ** 2,
                        user_id=user.id,
                        user_login=user.login,
                        user_name='{} {}'.format(
                            user.first_name,
                            user.last_name).strip())

                    db.session.add(ds)

                    if filename[-4:] in ['.csv', '.CSV', '.tsv', '.TSV']:
                        # noinspection PyBroadException
                        try:
                            # try to infer the field delimiter
                            count_delimiters = collections.defaultdict(int)
                            for ch in file_data:
                                if ch in [',', ';', '\t']:
                                    count_delimiters[ch] += 1
                            sorted_delim = sorted(
                                list(count_delimiters.items()),
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
                              'data': response_schema.dump(ds)}

            return result, result_code, {
                'Content-Type': 'application/json; charset=utf-8'}
        except Exception as e:
            log.exception(e)
            result = {'status': 'ERROR',
                     'data': str(e)}
            result_code = 500
            

    @staticmethod
    def _try_infer_schema(ds, delim):
        options = {
            'use_header': True,
            'delimiter': delim
        }
        DataSourceInferSchemaApi.infer_schema(ds, options)

class DataSourceDownload(MethodView):
    """ Entry point for downloading a DataSource """

    # noinspection PyUnresolvedReferences
    @staticmethod
    def get(data_source_id):
        # Uses a token to download
        download_token = {}
        try:
            fernet_key = current_app.fernet
            token = request.args.get('token', '').encode('utf8')
            if token is None:
                return json.dumps(
                    {'status': 'ERROR', 'message': gettext('Missing token')}), 401
            download_token = json.loads(fernet_key.decrypt(token))
        except Exception:
            return json.dumps(
                {'status': 'ERROR', 'message': 
                 gettext('Invalid or expired token. Refresh the listing page.'
                         )}), 500
        if download_token['id'] != data_source_id:
            return json.dumps(
                {'status': 'ERROR', 'message': 
                 gettext('Invalid data source.')}), 401

        data_source = DataSource.query.get_or_404(ident=data_source_id)

        parsed = urlparse(data_source.url)
        convert_to_csv = request.args.get('to_csv') in ('1', 'true')

        if parsed.scheme == 'file':
            name = '{}.{}'.format(data_source.name.replace(' ', '-'),
                                  data_source.format.lower())
                                 
            if data_source.format == 'PARQUET' and convert_to_csv:
                def do_download(path):
                    BUFFER_SIZE = 4096
                    ds = pq.ParquetDataset(path).read()
                    buf = io.BytesIO()
                    csv.write_csv(ds, buf, csv.WriteOptions(include_header=True))
                    buf.seek(0)

                    done = False
                    while not done:
                        data = buf.read(BUFFER_SIZE)
                        amount = len(data)
                        if amount != BUFFER_SIZE:
                            done = True
                        yield data

                name = name + '.csv'
                result = Response(stream_with_context(
                    do_download(parsed.path)))
            else:
                def do_download(path: str):

                   total = 0
                   done = False
                   with open(path, 'rb') as f:
                       while not done:
                           read_data = f.read(4096)
                           total += len(read_data)
                           if len(read_data) != 4096:
                               done = True
                           yield read_data

                result = Response(stream_with_context(
                    do_download(parsed.path)))
            result.headers[
                'Cache-Control'] = 'no-cache, no-store, must-revalidate'
            result.headers['Pragma'] = 'no-cache'
            result.headers["Content-Disposition"] = f"attachment; filename={name}"
            result_code = 200
        else:
            str_uri = hu.get_parsed_uri(parsed, False)
            try:
                extra_params = parse_hdfs_extra_params(
                        data_source.storage.extra_params)
                hadoop_user = 'hadoop'
                if extra_params and extra_params.user:
                    hadoop_user = extra_params.user
                # conf = get_hdfs_conf(jvm, extra_params, current_app.config)
                if parsed.port:
                    hdfs = fs.HadoopFileSystem(str_uri, port=int(parsed.port), 
                        user=hadoop_user)
                else:
                    hdfs = fs.HadoopFileSystem(str_uri, user=hadoop_user)

                if not hu.exists(hdfs, parsed.path):
                    message = gettext("%(type)s not found.",
                            type=gettext('Data source'))
                    result, result_code = {
                        'status': 'ERROR',
                        'message': message }, 404
                else:
                    name = '{}.{}'.format(data_source.name.replace(' ', '-'),
                                          data_source.format.lower())

                    if data_source.format == 'PARQUET':
                        if convert_to_csv:
                            result = Response(stream_with_context(
                                hu.download_parquet_as_csv(hdfs, parsed.path)),
                                mimetype='text/csv')
                            name = name + '.csv'
                        elif hu.is_directory(hdfs, parsed.path):
                            result = Response(stream_with_context(
                                hu.download_parquet(hdfs, parsed.path)), 
                                mimetype='application/octet-stream')
                        else:
                            result = Response(stream_with_context(
                                hu.download_file(hdfs, parsed.path)))
                    else:
                        result = Response(stream_with_context(
                            hu.download_file(hdfs, parsed.path)))

                    result.headers[
                        'Cache-Control'] = 'no-cache, no-store, must-revalidate'
                    result.headers['Pragma'] = 'no-cache'
                    result.headers["Content-Disposition"] = \
                        f"attachment; filename={name}"
                    result_code = 200
            except Exception as e:
                result = json.dumps(
                    {'status': 'ERROR', 'message': gettext('Internal error')})
                result_code = 500
                log.exception(str(e))

        return result, result_code


class DataSourceInferSchemaApi(Resource):
    @staticmethod
    def _infer_schema_from_db(ds, options):
        pass

    @staticmethod
    def infer_schema(ds, options):
        parsed = urlparse(
            next((a for a in [ds.url, ds.storage.client_url, ds.storage.url]
                if a), None))

        if ds.format in (DataSourceFormat.JDBC,):
            parsed = urlparse(ds.url)
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
                            db=parsed.path[1:]) as cn:
                        cursor = cn.cursor()
                        cursor.execute('{} LIMIT 0'.format(cmd))

                        DataSourceInferSchemaApi._delete_old_attributes(ds)

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

        elif ds.format in (DataSourceFormat.HIVE):
            parsed = urlparse(ds.storage.client_url)
            from pyhive import hive
            from TCLIService.ttypes import TOperationState
            if ds.storage.extra_params:
                extra = json.loads(ds.storage.extra_params)
            else:
                extra = {}
            print('=' * 20)
            print(parsed)
            print('=' * 20)
            cursor = hive.connect(
                   host=parsed.hostname,
                   port=int(parsed.port or 10000),
                   username=parsed.username,
                   password=parsed.password,
                   database=(parsed.path or 'default').replace('/', ''),
                   auth=extra.get('auth', 'CUSTOM') if parsed.password else None,
                   configuration={
                       'hive.cli.print.header': 'true'},
                   kerberos_service_name=extra.get('kerberos_service_name'),
                   thrift_transport=None).cursor()

            if ds.command is None or ds.command.strip() == '':
                raise ValueError(gettext(
                    'Data source does not have a command specified'))
            fix_limit = re.compile(r'\sLIMIT\s+(\d+)')
            cmd = fix_limit.sub('', ds.command)
            cursor.execute('{} LIMIT 0'.format(cmd), async_=True)
            status = cursor.poll().operationState
            while status in (TOperationState.INITIALIZED_STATE,
                    TOperationState.RUNNING_STATE):
                status = cursor.poll().operationState

            tables = set()
            attrs = []
            attrs_name = []
            types = []
            for t in cursor.description:
                attr, attr_type = t[0:2]
                types.append(attr_type)
                table, name = attr.split('.', 2)
                attrs.append(attr)
                attrs_name.append(name)
                tables.add(table)
            if len(tables) == 1:
                final_names = attrs_name
            else:
                final_names = attrs

            DataSourceInferSchemaApi._delete_old_attributes(ds)
            for (name, d) in zip(final_names, types):
                final_type = get_hive_data_type(d)
                attr = Attribute(name=name, type=final_type,
                                 size=None, precision=None,
                                 scale=None, nullable=None)
                attr.data_source = ds
                attr.feature = False
                attr.label = False
                db.session.add(attr)
            db.session.commit()


        elif ds.format in (DataSourceFormat.PARQUET,):

            #extra_params = parse_hdfs_extra_params(ds.storage.extra_params)
            #conf = get_hdfs_conf(jvm, extra_params, current_app.config)
            path = parsed.path
            if parsed.scheme == 'hdfs':
                str_uri = hu.get_parsed_uri(parsed, False)
                if parsed.port:
                    use_fs = fs.HadoopFileSystem(str_uri, port=int(parsed.port))
                else:
                    use_fs = fs.HadoopFileSystem(str_uri)
                schema = hu.infer_parquet(use_fs, path)
            elif parsed.schema == 'file':
                str_uri = hu.get_parsed_uri(parsed, False)
                use_fs = fs.LocalFileSystem()
                schema = hu.infer_parquet(use_fs, path)
            else:
                raise ValueError(gettext('Usupported filesystem: {fs}', 
                    fs=parsed.schema))

            DataSourceInferSchemaApi._delete_old_attributes(ds)
            for name, (dtype, precision, scale) in schema:
                attr = Attribute(name=name,
                                 nullable=True,
                                 enumeration=False,
                                 type=dtype,
                                 feature=False, label=False,
                                 precision=precision if precision != 0 else None,
                                 scale=scale,
                                 )
                attr.data_source = ds
                db.session.add(attr)
            db.session.commit()

        elif ds.format in (DataSourceFormat.CSV, DataSourceFormat.SHAPEFILE):
            str_uri = hu.get_parsed_uri(parsed, False)

            #conf, hadoop_pkg, hdfs, jvm, path, buffered_reader = [None] * 6
            if parsed.scheme == 'hdfs':
                # noinspection PyUnresolvedReferences
                extra_params = parse_hdfs_extra_params(ds.storage.extra_params)
                hadoop_user = extra_params.user or 'hadoop'
                if parsed.port:
                    use_fs = fs.HadoopFileSystem(str_uri, user=hadoop_user,
                        port=int(parsed.port))
                else:
                    use_fs = fs.HadoopFileSystem(str_uri, user=hadoop_user)
            elif parsed.scheme == 'file':
                str_uri = f'{parsed.scheme}://{parsed.path}'
                use_fs = fs.LocalFileSystem()
            else:
                raise ValueError(gettext('Usupported filesystem: ') +  
                    parsed.scheme)
            if ds.format == DataSourceFormat.CSV:
                try:
                    use_header = options.get('use_header',
                                             False) or ds.is_first_line_header

                    delimiter = (options.get('delimiter', ',') or ',')
                    # If there is a delimiter set in data_source, use it instead
                    if ds.attribute_delimiter:
                        delimiter = ds.attribute_delimiter

                    delimiter = SPECIAL_DELIMITERS.get(delimiter, delimiter)

                    if ds.treat_as_missing:
                        missing_values = ds.treat_as_missing.split(',')
                    else:
                        missing_values = []

                    encoding = 'utf8'

                    is_gzip = parsed.path.endswith('.gz')
                    
                    if parsed.scheme == 'file':
                        encoding = ds.encoding or 'utf8'
                        buffered_reader = codecs.open(parsed.path, 
                                                    'rb', encoding=encoding)
                    elif parsed.scheme == 'hdfs':
                        buffered_reader = io.BufferedReader(
                            use_fs.open_input_stream(parsed.path))

                    if is_gzip and parsed.scheme != 'hdfs':
                        reader = gzip.open(buffered_reader, mode='rt')
                    else:
                        reader = buffered_reader

                    quote_char = options.get('quote_char', None)
                    quote_char = quote_char.encode(
                        encoding) if quote_char else None

                    # Read 1000 lines, may be enough to infer schema
                    lines = StringIO()

                    for i in range(1000):
                        line = reader.readline()
                        if line is None:
                            break

                        if isinstance(line, bytes):
                            line = line.decode(encoding)
                        else:
                            line = line.encode(encoding).decode('utf8')
                        line = line.replace('\0', '')
                        lines.write(line)
                        lines.write('\n')
                    reader.close()
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

                    DataSourceInferSchemaApi._delete_old_attributes(ds)
                    for attr in attrs:
                        if attr.type is None:
                            attr.type = DataType.CHARACTER
                        if attr.type == DataType.CHARACTER:
                            if attr.size is not None and attr.size > 1000:
                                attr.type = DataType.TEXT
                        attr.data_source = ds
                        attr.feature = False
                        attr.label = False
                        db.session.add(attr)

                    db.session.commit()
                except Exception as ex:
                    log.exception(ex)
                    raise ValueError(
                        gettext('Cannot infer the schema: %(what)s', what=ex))
            elif ds.format == DataSourceFormat.SHAPEFILE:
                if True:
                    raise ValueError(gettext('Usupported'))
                from dbfpy import dbf
                DataSourceInferSchemaApi._delete_old_attributes(ds)

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
            raise ValueError(
                gettext('Cannot infer the schema for format %(format)s',
                        format=ds.format))

    @staticmethod
    def _delete_old_attributes(ds):
        old_attrs = Attribute.query.filter(
            Attribute.data_source_id == ds.id)
        old_privacy_attrs = AttributePrivacy.query.filter(
            AttributePrivacy.attribute_id.in_(old_attrs.with_entities(
                AttributePrivacy.attribute_id)))
        old_privacy_attrs.delete(synchronize_session=False)
        old_attrs.delete(synchronize_session=False)

    @staticmethod
    @requires_auth
    def post(data_source_id):

        result = {'status': 'OK'}
        ds = DataSource.query.get_or_404(data_source_id)
        request_body = request.json

        # noinspection PyBroadException
        try:
            DataSourceInferSchemaApi.infer_schema(ds, request_body)
        except UnicodeEncodeError:
            log.exception('Invalid CSV encoding')
            result = {'status': 'ERROR',
                      'message': gettext('Invalid CSV encoding')}, 400
        except ValueError as ve:
            result = {'status': 'ERROR', 'message': str(ve)}, 400
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
        for df, java_df in list(DATE_FORMATS.items()):
            for hf, java_hf in list(TIME_FORMATS.items()):
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
            v = float(value)
            if int(v) == v:
                v = int(v)
        except ValueError:
            v = value

        # test if first char is zero to avoid python
        # convertion of octal
        if any([(value[0] == '0' and len(value) > 1 and value[1] != '.'),
                type(v) in [str, str]]):
            if type(v) not in [int, float, int]:
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
        elif type(v) in [int]:
            if attrs[i].type not in [DataType.DECIMAL, DataType.FLOAT]:
                attrs[i].type = DataType.LONG
        elif type(v) in [float]:
            change_to_str = False
            parts = value.split('.')
            left, right = None, None
            if len(parts) == 2:
                left = len(parts[0])
                right = len(parts[1])
            elif len(parts) == 1 and parts[0].isdigit():
                left = len(parts[0])
                right = 0
            else:
                change_to_str = True
            if not change_to_str:
                attrs[i].type = DataType.DECIMAL
                attrs[i].precision = min(max((attrs[i].precision or 0),
                                             int(left or '0') + int(
                                                 right or 0)),
                                         18)
                attrs[i].scale = min(
                    max((attrs[i].scale or 0), int(right or 0)), 18)
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
            name = strip_accents(attr.strip())[:100].lower()
            attr = ''.join([c if c.isalnum() else "_" for c in name])
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
                attr_privacy[0], many=False)}
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
            response_schema = DataSourceItemResponseSchema()
            if not form.errors:
                try:
                    # Ignore missing fields to allow partial updates
                    data = request_schema.load(json_data, partial=True)
                    data.id = data_source_id
                    data_source = db.session.merge(data)
                    db.session.commit()

                    if data_source is not None:
                        result, result_code = dict(
                            status="OK",
                            message=gettext("%(what)s was successfuly updated",
                                            what=gettext('Data source')),
                            data=response_schema.dump(data_source)), 200
                    else:
                        result = dict(
                            status="ERROR",
                            message=gettext("%(type)s not found.",
                                            type=gettext('Data source')))
                except ValidationError as e:
                    result = dict(status="ERROR", message=gettext('Invalid data'),
                          errors=e.messages)
                    result_code = 400
                except Exception as e:
                    current_app.logger.exception(e)
                    log.exception('Error in PATCH')
                    result, result_code = dict(
                        status="ERROR", message=gettext("Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
        return result, result_code


class DataSourceInitializationApi(Resource):
    @staticmethod
    @requires_auth
    def post(data_source_id, status):
        data_sources = DataSource.query
        data_source = _filter_by_permissions(data_sources,
                                             list(PermissionType.values()))

        data_source = data_source.filter(DataSource.id == data_source_id)
        data_source = data_source.first()

        if data_source is not None:
            data_source.initialization = status
            db.session.add(data_source)
            db.session.commit()
            return {'status': 'OK'}, 200
        else:
            return {'status': 'Not found'}, 404



class DataSourceSampleApi(Resource):
    @staticmethod
    @requires_auth
    def get(data_source_id: int):
        return DataSourceSampleApi._get_sample(data_source_id)

    @staticmethod
    def _get_sample(data_source_id: int):

        data_sources = DataSource.query
        data_source = _filter_by_permissions(data_sources,
                                             list(PermissionType.values()))

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
            parsed = urlparse(
                next((a for a in [data_source.url, data_source.storage.client_url,
                    data_source.storage.url]
                    if a), None))

            if parsed.scheme == 'mysql':
                qs = {}
                if parsed.query and parsed.query.strip():
                    qs_args = parsed.query.split('&')
                    if qs_args:
                        qs = dict(x.split('=') for x in qs_args)
                fix_limit = re.compile(r'\sLIMIT\s+(\d+)')
                cmd = fix_limit.sub('', data_source.command)

                with pymysql.connect(
                        host=parsed.hostname,
                        port=parsed.port or '3306',
                        user=parsed.username or qs.get('user'),
                        passwd=parsed.password or qs.get('password'),
                        db=parsed.path[1:],
                        charset='UTF8',
                        cursorclass=pymysql.cursors.DictCursor) as cn:
                    cursor = cn.cursor()
                    cursor.execute('{} LIMIT {}'.format(cmd, limit))
                    result, status_code = dict(status='OK',
                                           data=cursor.fetchall()), 200
            elif data_source.format == 'HIVE':
                from pyhive import hive
                from TCLIService.ttypes import TOperationState
                parsed = urlparse(data_source.storage.client_url)

                if data_source.command is None or \
                        data_source.command.strip() == '':
                    raise ValueError(gettext(
                        'Data source does not have a command specified'))
                if data_source.storage.extra_params:
                    extra = json.loads(data_source.storage.extra_params)
                else:
                    extra = {}
                if parsed.password is not None:
                    cursor = hive.connect(
                          host=parsed.hostname,
                          port=int(parsed.port or 10000),
                          username=parsed.username,
                          password=parsed.password,
                          database=(parsed.path or 'default').replace('/', ''),
                          auth=extra.get('auth', 'CUSTOM'),
                          configuration={
                              'hive.cli.print.header': 'true'},
                          kerberos_service_name=extra.get('kerberos_service_name'),
                          thrift_transport=None).cursor()
                else:
                    cursor = hive.connect(
                          host=parsed.hostname,
                          port=int(parsed.port or 10000),
                          database=(parsed.path or 'default').replace('/', ''),
                          configuration={
                              'hive.cli.print.header': 'true'},
                          kerberos_service_name=extra.get('kerberos_service_name'),
                          thrift_transport=None).cursor()


                fix_limit = re.compile(r'\sLIMIT\s+(\d+)')
                cmd = fix_limit.sub('', data_source.command)
                cursor.execute('{} LIMIT {}'.format(cmd, limit))
                status = cursor.poll().operationState
                while status in (TOperationState.INITIALIZED_STATE,
                        TOperationState.RUNNING_STATE):
                    status = cursor.poll().operationState

                #col_names = [desc[0] for desc in cursor.description]
                col_names = [attr.name for attr in data_source.attributes]
                result = []
                for row in cursor:
                    result.append(dict(zip(col_names, row)))
                result, status_code = dict(status='OK',
                                           data=result), 200
            elif parsed.scheme == 'file':
                # Support JSON and CSV
                if data_source.format == DataSourceFormat.CSV:
                    encoding = data_source.encoding or 'utf8'
                    csvfile = gzip.open(parsed.path, mode='rt') \
                        if parsed.path.endswith('.gz') else \
                            codecs.open(parsed.path, 'rb', encoding=encoding)

                    header = []
                    converters = []
                    if data_source.attributes:
                        csv_params = {
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
                                converters.append(str.strip)
                        reader = csv.reader(csvfile, **csv_params)
                    else:
                        header.append(_('row'))
                        converters.append(str.strip)
                        reader = csv.reader(
                            csvfile,
                            delimiter=';')

                    if data_source.is_first_line_header:
                        next(reader)
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
                    csvfile.close()
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
            elif parsed.scheme == 'hdfs' and data_source.format in('CSV', 'PARQUET'):
                import pyarrow
                from pyarrow import fs
                try:
                    str_uri = f'{parsed.scheme}://{parsed.hostname}'
                    extra_params = parse_hdfs_extra_params(data_source.storage.extra_params)
                    hadoop_user = extra_params.user or 'hadoop'
                    if parsed.port:
                        hdfs = fs.HadoopFileSystem(str_uri, port=int(parsed.port), 
                            user=hadoop_user)
                    else:
                        hdfs = fs.HadoopFileSystem(str_uri, user=hadoop_user)

                    if not hu.exists(hdfs, parsed.path):
                        result = ({ 'status': 'ERROR',
                                      'message': 'Not found'}, 404)
                    else:
                        if data_source.format == 'CSV':
                            data = hu.sample_csv(hdfs, parsed.path, limit, data_source)
                        elif data_source.attributes:
                            schema = hu.get_parquet_schema(data_source)
                            data = hu.sample_parquet(hdfs, parsed.path, limit, schema)
                        else:
                            data = hu.sample_parquet(hdfs, parsed.path, limit)
                        result, status_code = dict(status='OK',
                                               data=data), 200
                except pyarrow.lib.ArrowInvalid:
                    log.exception(gettext('Internal error'))
                    result, status_code = dict(
                        status='ERROR',
                        message=gettext('Data type are not correctly defined. Please, revise them.')), 400
                except Exception as e:
                    log.exception(gettext('Internal error'))
                    result, status_code = dict(
                        status='ERROR',
                        message=gettext('Internal error')), 400
            else:
                return dict(status="ERROR",
                            message="Unsupported protocol {}".format(
                                parsed.scheme)), 400
        else:
            return dict(status="ERROR", message="Not found"), 400
        return result, status_code

# class DataSourceConvertApi(Resource):
#     @staticmethod
#     @requires_auth
#     def post(data_source_id: int, target_type: str):

#         data_sources = DataSource.query
#         data_source = _filter_by_permissions(data_sources,
#                                              list(PermissionType.values()))

#         data_source = data_source.filter(
#             DataSource.id == data_source_id).first()

#         result, status_code = dict(status='ERROR', message='Not found'), 404

#         warnings = []
#         if data_source is not None:
#             treat_as_missing = (data_source.treat_as_missing or '').split(',')
#             parsed = urlparse(
#                 next((a for a in [data_source.url, data_source.storage.client_url,
#                     data_source.storage.url]
#                     if a), None))

#             if parsed.scheme in( 'mysql', 'hive'):
#                 result = dict(status='Error',
#                     message='Invalid source type. Only file-based '
#                         'types can be converted.')
#                 status_code = 400
#             elif (data_source.format != DataSourceFormat.CSV and
#                     data_source.format != DataSourceFormat.JSON):
#                 result= dict(
#                     status='ERROR',
#                     message=f'Format {data_source.format} is not supported')
#                 status_code = 400

#             elif parsed.scheme == 'file':
#                 # Support JSON and CSV
#                 if data_source.format == DataSourceFormat.CSV:
#                     encoding = data_source.encoding or 'utf8'
#                     csvfile = gzip.open(parsed.path, mode='rt') \
#                         if parsed.path.endswith('.gz') else \
#                             codecs.open(parsed.path, 'rb', encoding=encoding)

#                     header = []
#                     converters = []
#                     if data_source.attributes:
#                         csv_params = {
#                             'delimiter': data_source.attribute_delimiter or ','}
#                         if data_source.text_delimiter:
#                             csv_params['quoting'] = csv.QUOTE_MINIMAL
#                             csv_params[
#                                 'quotechar'] = data_source.text_delimiter
#                         for attr in data_source.attributes:
#                             header.append(attr.name)
#                             if attr.type in [DataType.DECIMAL]:
#                                 converters.append(decimal.Decimal)
#                             elif attr.type in [DataType.DOUBLE,
#                                                DataType.FLOAT]:
#                                 converters.append(float)
#                             elif attr.type in [DataType.INTEGER,
#                                                DataType.LONG]:
#                                 converters.append(int)
#                             else:
#                                 converters.append(str.strip)
#                         reader = csv.reader(csvfile, **csv_params)
#                     else:
#                         header.append(_('row'))
#                         converters.append(str.strip)
#                         reader = csv.reader(
#                             csvfile,
#                             delimiter=';')

#                     if data_source.is_first_line_header:
#                         next(reader)
#                     data = []
#                     for i, line in enumerate(reader):
#                         if i >= limit:
#                             break
#                         row = {}
#                         for h, v, conv in zip(header, line, converters):
#                             try:
#                                 row[h] = conv(v) if v != '' else ''
#                             except decimal.InvalidOperation:
#                                 row[h] = gettext(
#                                     "<Invalid data>: `{}`".format(v))
#                                 warnings.append(h)
#                         data.append(row)
#                         result, status_code = dict(
#                             status='OK', warnings=list(set(warnings)),
#                             data=data), 200
#                     csvfile.close()
#                 elif data_source.format == DataSourceFormat.JSON:
#                     data = []
#                     with open(parsed.path) as f:
#                         for i, line in enumerate(f):
#                             if i >= limit:
#                                 break
#                             data.append(json.loads(line))
#                     result, status_code = dict(status='OK',
#                                                data=data), 200
#             elif parsed.scheme == 'hdfs':
#                 gateway = create_gateway(log, current_app.gateway_port)
#                 jvm = gateway.jvm
#                 str_uri = hu.get_parsed_uri(parsed)

#                 try:
#                     uri = jvm.java.net.URI(str_uri)

#                     extra_params = parse_hdfs_extra_params(
#                             data_source.storage.extra_params)
#                     conf = get_hdfs_conf(jvm, extra_params, current_app.config)

#                     hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

#                     chunk_path = jvm.org.apache.hadoop.fs.Path(parsed.path)
#                     if not hdfs.exists(chunk_path):
#                         result = ({
#                                       'status': 'ERROR',
#                                       'message': 'Not found'}, 404)
#                     else:
#                         input_in = hdfs.open(chunk_path)

#                         bom_input_stream = \
#                             jvm.org.apache.commons.io.input.BOMInputStream(
#                                 input_in)

#                         if bom_input_stream.getBOM() is not None:
#                             encoding = \
#                                 bom_input_stream.getBOM().getCharsetName()
#                         else:
#                             encoding = data_source.encoding or 'UTF-8'
#                         if parsed.path.endswith('.gz'):
#                             buffered_reader = jvm.java.io.BufferedReader(
#                                 jvm.java.io.InputStreamReader(
#                                     jvm.java.util.zip.GZIPInputStream(
#                                         bom_input_stream), encoding))
#                         else:
#                             buffered_reader = jvm.java.io.BufferedReader(
#                                 jvm.java.io.InputStreamReader(
#                                     bom_input_stream, encoding))

#                         header = []
#                         converters = []
#                         csv_buf = StringIO()

#                         if data_source.attributes:
#                             for attr in data_source.attributes:
#                                 header.append(attr.name)
#                                 if attr.type in [DataType.DECIMAL]:
#                                     converters.append(decimal.Decimal)
#                                 elif attr.type in [DataType.DOUBLE,
#                                                    DataType.FLOAT]:
#                                     converters.append(float)
#                                 elif attr.type in [DataType.INTEGER,
#                                                    DataType.LONG]:
#                                     converters.append(int)
#                                 else:
#                                     converters.append(str.strip)
#                             d = data_source.attribute_delimiter or str(',')
#                             csv_params = {'delimiter': SPECIAL_DELIMITERS.get(d, d)}
#                             reader = csv.reader(csv_buf, **csv_params)
#                         else:
#                             header.append(_('row'))
#                             converters.append(str.strip)
#                             reader = csv.reader(
#                                 csv_buf,
#                                 delimiter=';')
#                         data = []
#                         i = 0
#                         if data_source.is_first_line_header:
#                             buffered_reader.readLine()

#                         limit = 40
#                         while i < limit:
#                             tmp_line = buffered_reader.readLine()
#                             if tmp_line:
#                                 csv_buf.write(tmp_line)
#                                 csv_buf.write('\n')
#                             i += 1

#                         csv_buf.seek(0)
#                         from collections import OrderedDict
#                         for line in reader:
#                             row = OrderedDict()
#                             for h, v, conv in zip(header, line, converters):
#                                 # noinspection PyBroadException
#                                 try:
#                                     if v not in treat_as_missing:
#                                         row[h] = conv(v)
#                                     else:
#                                         row[h] = None
#                                 except:
#                                     status_code = 422
#                                     return dict(
#                                         status='ERROR',
#                                         message=INVALID_FORMAT_ERROR.format(
#                                             type=str(conv), attr=h,
#                                             v=str(v)
#                                         )), status_code
#                             data.append(row)

#                         result, status_code = dict(status='OK',
#                                                    data=data), 200
#                 except Py4JJavaError as java_ex:
#                     if 'Could not obtain block' in \
#                             java_ex.java_exception.getMessage():
#                         return {'status': 'ERROR',
#                                 'message': WRONG_HDFS_CONFIG}, 400
#                     log.exception('Java error')
#                 except Exception as e:
#                     result = {'status': 'ERROR',
#                               'message': 'Internal error:',
#                               'details': str(e)}
#                     status_code = 500
#                     log.exception(str(e))
#             else:
#                 return dict(status="ERROR",
#                             message=f"Unsupported protocol {parsed.scheme}"), 400
#         else:
#             return dict(status="ERROR", message="Not found"), 404
#         return result, status_code



# Events
# noinspection PyUnusedLocal
@listens_for(inspect(DataSource).relationships['attributes'], 'append')
@listens_for(inspect(DataSource).relationships['attributes'], 'remove')
def receive_append_or_remove(target, value, initiator):
    target.updated = datetime.datetime.utcnow()


# noinspection PyUnusedLocal
@listens_for(Attribute, 'after_update')
def receive_attribute_change(mapper, connection, target):
    if target.data_source:
        target.data_source.updated = datetime.datetime.utcnow()
