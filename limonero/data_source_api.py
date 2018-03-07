# -*- coding: utf-8 -*-}
import codecs
import csv
import logging
import math
import re
import unicodedata
import StringIO
from ast import literal_eval
from io import BytesIO
from urlparse import urlparse

from dateutil import parser as date_parser
from dbfpy import dbf
from werkzeug.exceptions import NotFound
from flask import g as flask_g
from flask import request, Response, current_app
from flask import stream_with_context
from flask.views import MethodView
from flask_restful import Resource
from sqlalchemy import or_, and_
from sqlalchemy.orm import subqueryload, joinedload

from app_auth import requires_auth, User
from schema import *

from hdfs3 import HDFileSystem

log = logging.getLogger(__name__)

def create_hdfs_from_url(url, user = "root"):
    parsed = urlparse(url)

    str_uri = '{proto}://{host}'.format(
        proto=parsed.scheme, host=parsed.hostname)

    return HDFileSystem(host=str_uri, port=parsed.port, user=user)

def create_hdfs(storage, user = "root"):
    return create_hdfs_from_url(storage.url, user=user)

def create_tmp_path(hdfs, identifier):
    tmp_path = '/tmp/upload/{}'.format(identifier)
    if not hdfs.exists(tmp_path):
        hdfs.mkdir(tmp_path)
    return tmp_path

def create_chunk_name(hdfs, identifier, filename, chunk_number):
    tmp_path = create_tmp_path(hdfs, identifier)
    return "{tmp}/{file}.part{part:09d}".format(
        tmp=tmp_path, file=filename, part=chunk_number)

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')

def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != '':
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


def _filter_by_permissions(data_sources, permissions):
    if flask_g.user.id not in (0, 1):  # It is not a inter service call
        conditions = or_(
            DataSource.is_public,
            DataSource.user_id == flask_g.user.id,
            and_(
                DataSourcePermission.user_id == flask_g.user.id,
                DataSourcePermission.permission.in_(permissions)
            )
        )
        data_sources = data_sources.filter(conditions)
    return data_sources


class DataSourceListApi(Resource):
    """ REST API for listing class DataSource """

    @staticmethod
    @requires_auth
    def get():
        result, result_code = 'Internal error', 500
        # noinspection PyBroadException
        try:
            simple = False
            if request.args.get('simple') != 'true':
                only = None
            else:
                simple = True
                only = ('id', 'name', 'description', 'created',
                        'user_name', 'permissions', 'user_id', 'privacy_aware')

            if request.args.get('fields'):
                only = tuple(
                    [x.strip() for x in request.args.get('fields').split(',')])

            possible_filters = {'enabled': bool, 'format': None, 'user_id': int}
            data_sources = DataSource.query
            for f, transform in possible_filters.items():
                data_sources = apply_filter(data_sources, request.args, f,
                                            transform, lambda field: field)

            data_sources = data_sources.join(DataSourcePermission, isouter=True)
            if not simple:
                data_sources = data_sources.options(
                    joinedload(DataSource.attributes))

            data_sources = _filter_by_permissions(
                data_sources, PermissionType.values())

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
                                                           True)
                    else:
                        # No pagination
                        pagination = data_sources
                    result = {
                        'data': DataSourceListResponseSchema(
                            many=True, only=only).dump(pagination.items).data,
                        'pagination': {
                            'page': page, 'size': page_size,
                            'total': pagination.total,
                            'pages': int(
                                math.ceil(1.0 * pagination.total / page_size))}
                    }
                else:
                    result = {
                        'data': DataSourceListResponseSchema(
                            many=True, only=only).dump(data_sources).data}
            else:
                only = ('id', 'name')
                result = DataSourceListResponseSchema(
                    many=True, only=only).dump(data_sources).data
            db.session.commit()
            result_code = 200
        except NotFound as ex:
            print ex
            log.exception(ex.message)
            raise(ex)
        except Exception as ex:
            print ex
            log.exception(ex.message)

        return result, result_code

    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR", message="Missing json in the request body"), 400
        if request.json is not None:
            request_schema = DataSourceCreateRequestSchema()
            response_schema = DataSourceItemResponseSchema()
            form = request_schema.load(request.json)
            if form.errors:
                result, result_code = dict(
                    status="ERROR", message="Validation error",
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

                    if data_source_id is None:
                        data_source = form.data
                        db.session.add(data_source)

                    db.session.commit()
                    result, result_code = response_schema.dump(
                        data_source).data, 200
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message="Internal error"), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()

        return result, result_code


class DataSourceDetailApi(Resource):
    """ REST API for a single instance of class DataSource """

    @staticmethod
    @requires_auth
    def get(data_source_id):

        # data_source = DataSource.query.join(DataSource.storage).join(
        #     DataSource.attributes, isouter=True).options(
        #     joinedload(DataSource.attributes),
        #     joinedload(DataSource.permissions),
        #     joinedload(DataSource.storage)
        # )

        data_source = DataSource.query.join(DataSource.storage).options(
            joinedload(DataSource.attributes),
            joinedload(DataSource.permissions),
        )

        data_source = _filter_by_permissions(data_source,
                                             PermissionType.values())

        data_source = data_source.order_by(DataSource.name)
        data_source = data_source.filter(
            DataSource.id == data_source_id).first()

        if data_source is not None:
            return DataSourceItemResponseSchema().dump(data_source).data
        else:
            return dict(status="ERROR", message="Not found"), 404

    @staticmethod
    @requires_auth
    def delete(data_source_id):
        result, result_code = dict(status="ERROR", message="Not found"), 404

        filtered = _filter_by_permissions(
            DataSource.query, [PermissionType.MANAGE, PermissionType.WRITE])
        data_source = filtered.filter(DataSource.id == data_source_id).first()
        if data_source is not None:
            try:
                data_source.enabled = False
                db.session.add(data_source)
                db.session.commit()
                result, result_code = dict(status="OK", message="Deleted"), 200
            except Exception as e:
                log.exception('Error in DELETE')
                result, result_code = dict(status="ERROR",
                                           message="Internal error"), 500
                if current_app.debug:
                    result['debug_detail'] = e.message
                db.session.rollback()
        return result, result_code

    @staticmethod
    @requires_auth
    def patch(data_source_id):
        result = dict(status="ERROR", message="Insufficient data")
        result_code = 400
        json_data = request.json or json.loads(request.data)
        if json_data:
            request_schema = partial_schema_factory(
                DataSourceCreateRequestSchema)

            # FIXME: Remove this code, ignore attribute_privacy
            for attr in json_data['attributes']:
                del attr['attribute_privacy']

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
                            status="OK", message="Updated",
                            data=response_schema.dump(data_source).data), 200
                    else:
                        result = dict(status="ERROR", message="Not found")
                except Exception as e:
                    current_app.logger.exception(e)
                    log.exception('Error in PATCH')
                    result, result_code = dict(status="ERROR",
                                               message="Internal error"), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()
            else:
                result = dict(status="ERROR", message="Invalid data",
                              errors=form.errors)
        return result, result_code


class DataSourcePermissionApi(Resource):
    """ REST API for sharing a DataSource """

    @staticmethod
    @requires_auth
    def post(data_source_id, user_id):
        result, result_code = dict(
            status="ERROR", message="Missing json in the request body"), 400

        if request.json is not None:
            form = request.json
            to_validate = ['permission', 'user_name', 'user_login']
            error = False
            for check in to_validate:
                if check not in form or form.get(check, '').strip() == '':
                    result, result_code = dict(
                        status="ERROR", message="Validation error",
                        errors={'Missing': check}), 400
                    error = True
                    break
                if check == 'permission' and form.get(
                        'permission') not in PermissionType.values():
                    result, result_code = dict(
                        status="ERROR", message="Validation error",
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
                        result, result_code = dict(status="ERROR",
                                                   message="Not found"), 404
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message="Internal error"), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()

        return result, result_code

    @staticmethod
    @requires_auth
    def delete(data_source_id, user_id):
        result, result_code = dict(status="ERROR", message="Not found"), 404

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
                    result, result_code = dict(status="OK",
                                               message="Deleted"), 200
                except Exception as e:
                    log.exception('Error in DELETE')
                    result, result_code = dict(status="ERROR",
                                               message="Internal error"), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()
        return result, result_code


class DataSourceUploadApi(Resource):
    """ REST API for upload a DataSource """

    @staticmethod
    @requires_auth
    def get():
        # noinspection PyBroadException
        try:
            identifier = request.args.get('resumableIdentifier', type=str)
            filename = request.args.get('resumableFilename', type=str)
            chunk_number = request.args.get('resumableChunkNumber', type=int)
            storage_id = request.args.get('storage_id', type=int)

            result, result_code = 'OK', 200

            if not identifier or not filename or not chunk_number:
                # Parameters are missing or invalid
                result, result_code = 'Missing arguments', 500
            else:
                storage = Storage.query.get(storage_id)
                hdfs = create_hdfs(storage)

                chunk_filename = create_chunk_name(hdfs, identifier, filename, chunk_number)
                current_app.logger.debug('Checking for chunk: %s', chunk_filename)

                if not hdfs.exists(chunk_filename):
                    # Let resumable.js know this chunk does not exists
                    #  and needs to be uploaded
                    result, result_code = 'Not found', 404

            return result, result_code
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
            if not identifier or not filename or not chunk_number:
                # Parameters are missing or invalid
                result, result_code = 'Missing arguments', 500
            else:
                storage = Storage.query.get(storage_id)
                hdfs = create_hdfs(storage)

                chunk_filename = create_chunk_name(hdfs, identifier, filename, chunk_number)

                with hdfs.open(chunk_filename, 'wb') as f:
                    f.write(request.get_data())

                current_app.logger.debug('Wrote chunk: %s', chunk_filename)

                # Checks if all file's parts are present
                full_path = create_tmp_path(hdfs, identifier)

                counter = len(hdfs.ls(full_path))

                if counter == total_chunks:
                    final_filename = '{}_{}'.format(identifier, filename)

                    if not hdfs.exists("/limonero/data"):
                        hdfs.mkdir("/limonero/data")
                    # time to merge all files
                    target_path = u'{}/{}'.format(u'/limonero/data',
                        final_filename)

                    if hdfs.exists(target_path):
                        result = {"status": "error",
                                  "message": "File already exists"}
                        result_code = 500
                        return result, result_code, {
                            'Content-Type': 'application/json; charset=utf-8'}
                    else:
                        files = hdfs.ls(full_path)
                        with hdfs.open(target_path, 'wb') as f2:
                            for apath in files:
                                with hdfs.open(apath, 'rb') as f:
                                    out = 1
                                    while out:
                                        out = f.read(2**16)
                                        f2.write(out)
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
                    infer = False
                    if extension == 'csv':
                        ds_format = DataSourceFormat.CSV
                        infer = True
                    elif extension == 'json':
                        ds_format = DataSourceFormat.JSON
                    elif extension == 'xml':
                        ds_format = DataSourceFormat.XML_FILE
                    else:
                        ds_format = DataSourceFormat.TEXT

                    ds = DataSource(
                        format=ds_format,
                        name=filename,
                        storage_id=storage.id,
                        description='Imported in Limonero',
                        enabled=True,
                        url='{}{}'.format(storage.url, target_path),
                        estimated_size_in_mega_bytes=total_size / 1024.0 ** 2,
                        user_id=user.id,
                        user_login=user.login,
                        user_name='{} {}'.format(user.first_name,
                                                 user.last_name).strip())

                    db.session.add(ds)
                    db.session.commit()

            return result, result_code, {
                'Content-Type': 'application/json; charset=utf-8'}
        except:
            raise


class DataSourceDownload(MethodView):
    """ Entry point for downloading a DataSource """

    # noinspection PyUnresolvedReferences
    @staticmethod
    @requires_auth
    def get(data_source_id):
        data_source = DataSource.query.get_or_404(ident=data_source_id)

        try:
            hdfs = create_hdfs_from_url(data_source.url)
            file_path = urlparse(data_source.url).path

            if not hdfs.exists(file_path):
                result, result_code = 'Not found', 404
            else:
                input_in = hdfs.open(file_path)

                def do_download():
                    total = 0
                    done = False
                    chunk_size = 4096
                    while not done:
                        buf = input_in.read(chunk_size)
                        read = len(buf)
                        total += read
                        if read != chunk_size:
                            done = True
                        yield buf

                name = '{}.{}'.format(data_source.name.replace(' ', '-'),
                                      data_source.format.lower())
                result = Response(stream_with_context(
                    do_download()), mimetype='text/csv')

                result.headers[
                    'Cache-Control'] = 'no-cache, no-store, must-revalidate'
                result.headers['Pragma'] = 'no-cache'
                result.headers[
                    "Content-Disposition"] = "attachment; filename={}".format(
                    name)
                result_code = 200
        except Exception as e:
            print e
            result = json.dumps(
                {'status': 'ERROR', 'message': 'Internal error'})
            result_code = 500
            log.exception(e.message)

        return result, result_code


class DataSourceInferSchemaApi(Resource):
    tests = {
        DataType.DATETIME: lambda x: date_parser.parse(x),
        DataType.DECIMAL: '',
        DataType.DOUBLE: '',
    }

    @staticmethod
    def infer_schema(ds, options):
        parsed = urlparse(ds.storage.url)

        # noinspection PyUnresolvedReferences
        hdfs = create_hdfs_from_url(ds.storage.url)

        path = urlparse(ds.url).path

        if ds.format == DataSourceFormat.CSV:
            use_header = options.get('use_header',
                                     False) or ds.is_first_line_header
            delimiter = (options.get('delimiter', ',') or ',').encode('latin1')
            # If there is a delimiter set in data_source, use it instead
            if ds.attribute_delimiter:
                delimiter = ds.attribute_delimiter

            special_delimiters = {'{tab}': '\t', '{new_line}': '\n'}
            delimiter = special_delimiters.get(delimiter, delimiter).encode(
                'utf8')

            buffered_reader, encoding = DataSourceInferSchemaApi._get_reader(
                hdfs, ds, path)

            quote_char = options.get('quote_char', None)
            quote_char = quote_char.encode(encoding) if quote_char else None

            i = 1
            lines = StringIO.StringIO()
            # Read 100 lines, may be enough to infer schema
            for line in buffered_reader:
                if i == 100:
                    break
                line = line.replace('\0', '')
                lines.write(line)
                lines.write(u'\n')
                i += 1

            lines.seek(0)

            csv_reader = csv.reader(lines, delimiter=delimiter,
                    quotechar=quote_char.decode("utf8"))

            attrs = []
            # noinspection PyBroadException
            try:
                attrs = DataSourceInferSchemaApi._get_csv_attributes(
                    attrs, csv_reader, use_header)

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
            except Exception as e:
                db.session.rollback()
                log.exception('Invalid CSV format')
                return {'status': 'ERROR', 'message': 'Invalid CSV format'}, 400
        elif ds.format == DataSourceFormat.SHAPEFILE:
            old_attrs = Attribute.query.filter(
                Attribute.data_source_id == ds.id)
            old_attrs.delete(synchronize_session=False)

            path2 = urlparse(re.sub('.shp$', '.dbf', ds.url))
            input_stream_dbf = hdfs.open(path2)

            dbf_content = path2.read()

            dbf_io = BytesIO(dbf_content)
            handler = dbf.Dbf(dbf_io)

            types = {
                'C': DataType.CHARACTER,
                'B': DataType.DOUBLE,
                'D': DataType.DATETIME,
                'F': DataType.FLOAT,
                'I': DataType.INTEGER,
                'L': DataType.INTEGER,
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
                'Cannot infer the schema for format {}'.format(ds.format))

    @staticmethod
    @requires_auth
    def post(data_source_id):

        ds = DataSource.query.get_or_404(data_source_id)
        request_body = {}
        if request.data:
            request_body = json.loads(request.data)

        DataSourceInferSchemaApi.infer_schema(ds, request_body)
        return {'status': 'OK'}

    @staticmethod
    def _get_reader(hdfs, ds, path):
        #FIXME support gzip and check encoding

        input_stream = hdfs.open(path)

        # Handle UTF-8 with BOM
        # See https://stackoverflow.com/a/13591421/4245898
        raw = input_stream.read(4)

        if raw.startswith(codecs.BOM_UTF8):
            encoding = 'utf-8-sig'
        else:
            input_stream.seek(0)
            encoding = ds.encoding or 'UTF-8'

        return input_stream, encoding

    @staticmethod
    def _get_csv_attributes(attrs, csv_reader, use_header):
        for row in csv_reader:
            if use_header and len(attrs) == 0:
                attrs = DataSourceInferSchemaApi._get_header(row)
            else:
                if len(attrs) == 0:
                    attrs = DataSourceInferSchemaApi._get_default_header(
                        row)
                for i, value in enumerate(row):
                    if value is None or value == '':
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
                    date_parser.parse(value)
                    if len(value) > 5:
                        attrs[
                            i].type = DataType.DATETIME
                    else:
                        attrs[
                            i].type = DataType.CHARACTER
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
        elif type(v) in [int]:
            attrs[i].type = DataType.INTEGER
        elif type(v) in [long]:
            attrs[i].type = DataType.LONG
        elif type(v) in [int]:
            attrs[i].type = DataType.INTEGER
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
            attr = strip_accents(attr.replace(' ', '_').decode('utf8'))
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
            return dict(status="ERROR", message="Not found"), 404

    @staticmethod
    @requires_auth
    def patch(data_source_id):
        result = dict(status="ERROR", message="Insufficient data")
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
                            status="OK", message="Updated",
                            data=response_schema.dump(data_source).data), 200
                    else:
                        result = dict(status="ERROR", message="Not found")
                except Exception as e:
                    current_app.logger.exception(e)
                    log.exception('Error in PATCH')
                    result, result_code = dict(status="ERROR",
                                               message="Internal error"), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()
            else:
                result = dict(status="ERROR", message="Invalid data",
                              errors=form.errors)
        return result, result_code
