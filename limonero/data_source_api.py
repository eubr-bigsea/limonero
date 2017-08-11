# -*- coding: utf-8 -*-}
import StringIO
import csv
import logging
import math
import uuid
from ast import literal_eval
from urlparse import urlparse

from dateutil import parser as date_parser
from flask import g
from flask import request, current_app
from flask_restful import Resource
from py4j.compat import bytearray2
from sqlalchemy import or_, and_
from sqlalchemy.orm import subqueryload

from app_auth import requires_auth
from schema import *

log = logging.getLogger(__name__)


def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != '':
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


def _filter_by_permissions(data_sources, permissions):
    if g.user.id != 0:  # It is not a inter service call
        conditions = or_(
            DataSource.is_public,
            DataSource.user_id == g.user.id,
            and_(
                DataSourcePermission.user_id == g.user.id,
                DataSourcePermission.permission.in_(permissions)
            )
        )
        data_sources = data_sources.join(
            DataSource.permissions, isouter=True).filter(conditions)
    return data_sources


class DataSourceListApi(Resource):
    """ REST API for listing class DataSource """

    @staticmethod
    @requires_auth
    def get():
        result, result_code = 'Internal error', 500
        # noinspection PyBroadException
        try:

            if request.args.get('simple') != 'true':
                only = None
            else:
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
                result = DataSourceListResponseSchema(
                    many=True, only=only).dump(data_sources).data
            db.session.commit()
            result_code = 200
        except Exception as ex:
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
        filtered = _filter_by_permissions(DataSource.query,
                                          PermissionType.values())
        data_source = filtered.filter(DataSource.id == data_source_id).first()
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
        identifier = request.args.get('resumableIdentifier', type=str)
        filename = request.args.get('resumableFilename', type=str)
        chunk_number = request.args.get('resumableChunkNumber', type=int)

        result, result_code = 'OK', 200

        if not identifier or not filename or not chunk_number:
            # Parameters are missing or invalid
            result, result_code = 'Missing arguments', 500
        else:
            storage = Storage.query.get(
                request.args.get('storage_id', type=int))
            parsed = urlparse(storage.url)

            # noinspection PyUnresolvedReferences
            jvm = current_app.gateway.jvm

            str_uri = '{proto}://{host}:{port}'.format(
                proto=parsed.scheme, host=parsed.hostname, port=parsed.port)
            uri = jvm.java.net.URI(str_uri)

            conf = jvm.org.apache.hadoop.conf.Configuration()
            conf.set('dfs.client.use.datanode.hostname', 'true')

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
                result, result_code = 'Not found', 404

        return result, result_code

    @staticmethod
    @requires_auth
    def post():
        identifier = request.args.get('resumableIdentifier', type=str)
        filename = request.args.get('resumableFilename', type=unicode)
        chunk_number = request.args.get('resumableChunkNumber', type=int)
        total_chunks = request.args.get('resumableTotalChunks', type=int)
        total_size = request.args.get('resumableTotalSize', type=int)

        result, result_code = 'OK', 200
        if not identifier or not filename or not chunk_number:
            # Parameters are missing or invalid
            import pdb
            pdb.set_trace()
            result, result_code = 'Missing arguments', 500
        else:
            storage = Storage.query.get(
                request.args.get('storage_id', type=int))
            parsed = urlparse(storage.url)

            # noinspection PyUnresolvedReferences
            jvm = current_app.gateway.jvm

            str_uri = '{proto}://{host}:{port}'.format(
                proto=parsed.scheme, host=parsed.hostname, port=parsed.port)
            uri = jvm.java.net.URI(str_uri)

            conf = jvm.org.apache.hadoop.conf.Configuration()
            conf.set('dfs.client.use.datanode.hostname', 'true')

            hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

            tmp_path = DataSourceUploadApi._get_tmp_path(
                jvm, hdfs, parsed, filename)

            chunk_filename = u"{tmp}/{file}.part{part:09d}".format(
                tmp=tmp_path.toString(), file=filename, part=chunk_number)
            current_app.logger.debug('Wrote chunk: %s', chunk_filename)

            chunk_path = jvm.org.apache.hadoop.fs.Path(chunk_filename)

            output_stream = hdfs.create(chunk_path)
            block = bytearray2(request.get_data())
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
                target_path = jvm.org.apache.hadoop.fs.Path(
                    u'{}/{}'.format(u'/limonero/data', final_filename))
                if hdfs.exists(target_path):
                    result, result_code = {"status": "error",
                                           "message": "File already exists"}, \
                                          500
                jvm.org.apache.hadoop.fs.FileUtil.copyMerge(
                    hdfs, full_path, hdfs, target_path, True, conf, None)
                ds = DataSource(
                    name=filename,
                    storage_id=storage.id,
                    description='Imported in Limonero',
                    enabled=True,
                    url='{}{}'.format(str_uri, target_path.toString()),
                    format=DataSourceFormat.TEXT,
                    user_id=1,
                    estimated_size_in_mega_bytes=total_size / 1024.0 ** 2,
                    user_login='FIXME',
                    user_name='FIXME')

                db.session.add(ds)
                db.session.commit()

        return result, result_code, {
            'Content-Type': 'application/json; charset=utf-8'}


class DataSourceInferSchemaApi(Resource):
    tests = {
        DataType.DATETIME: lambda x: date_parser.parse(x),
        DataType.DECIMAL: '',
        DataType.DOUBLE: '',
    }

    @staticmethod
    @requires_auth
    def post(data_source_id):

        ds = DataSource.query.get_or_404(data_source_id)

        request_body = {}
        if request.data:
            request_body = json.loads(request.data)

        parsed = urlparse(ds.storage.url)

        # noinspection PyUnresolvedReferences
        jvm = current_app.gateway.jvm

        str_uri = '{proto}://{host}:{port}'.format(
            proto=parsed.scheme, host=parsed.hostname, port=parsed.port)
        uri = jvm.java.net.URI(str_uri)

        conf = jvm.org.apache.hadoop.conf.Configuration()
        conf.set('dfs.client.use.datanode.hostname', 'true')

        hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        input_stream = hdfs.open(jvm.org.apache.hadoop.fs.Path(ds.url))
        buffered_reader = jvm.java.io.BufferedReader(
            jvm.java.io.InputStreamReader(input_stream))

        delimiter = request_body.get('delimiter', ',').encode('latin1')
        quote_char = request_body.get('quote_char', None)
        quote_char = quote_char.encode('latin1') if quote_char else None
        use_header = request_body.get('use_header', False)

        # Read 100 lines, may be enough to infer schema
        lines = StringIO.StringIO()
        for _ in range(100):
            line = buffered_reader.readLine()
            if line is None:
                break
            lines.write(line.encode('utf8'))
            lines.write('\n')

        buffered_reader.close()
        lines.seek(0)

        csv_reader = csv.reader(lines, delimiter=delimiter,
                                quotechar=quote_char)

        attrs = []

        for row in csv_reader:
            if use_header and len(attrs) == 0:
                for attr in row:
                    attrs.append(
                        Attribute(name=attr, nullable=False, enumeration=False))
            else:
                if len(attrs) == 0:
                    for i, attr in enumerate(row):
                        # Default name is attrX
                        attrs.append(
                            Attribute(name='attr{}'.format(i), nullable=False,
                                      enumeration=False))
                for i, value in enumerate(row):
                    if value is None or value == '':
                        attrs[i].nullable = True
                    else:
                        if attrs[i].type != DataType.CHARACTER:
                            try:
                                v = literal_eval(value)
                            except ValueError:
                                v = value
                            except SyntaxError:
                                v = value
                            # test if first char is zero to avoid python
                            # convertion of octal
                            if any([(value[0] == '0' and len(value) > 1 and
                                             value[1] != '.'),
                                    type(v) in [str, unicode]]):
                                if type(v) not in [int, float, long]:
                                    # noinspection PyBroadException
                                    try:
                                        date_parser.parse(value)
                                        attrs[i].type = DataType.DATETIME
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
                                left, right = value.split('.')
                                attrs[i].type = DataType.DECIMAL
                                attrs[i].precision = max(
                                    attrs[i].precision,
                                    len(left) + len(right))
                                attrs[i].scale = max(attrs[i].scale,
                                                     len(right))
                        else:
                            attrs[i].size = max(attrs[i].size, len(value))

        old_attrs = Attribute.query.filter(Attribute.data_source_id == ds.id)
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
        '''
        return [(x.name, x.type, x.nullable, x.size, x.precision, x.scale)
                for x
                in attrs]
        '''
        '''
        '''
        return {'status': 'OK'}


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
        # import pdb
        # pdb.set_trace()
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
