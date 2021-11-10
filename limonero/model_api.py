# -*- coding: utf-8 -*-}
import logging
import math

from flask import g as flask_g
from flask import g
from flask import request, current_app
from flask_babel import gettext
from flask_restful import Resource
from py4j.protocol import Py4JJavaError
from sqlalchemy import or_, and_
from marshmallow.exceptions import ValidationError

from limonero.util import upload
from .app_auth import requires_auth
from .schema import *

_ = gettext
log = logging.getLogger(__name__)


def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != '':
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


def _filter_by_permissions(models, permissions):
    if g.user.id != 0:  # It is not a inter service call
        conditions = or_(
            Model.user_id == g.user.id,
            g.user.id == 1,
            and_(
                ModelPermission.user_id == g.user.id,
                ModelPermission.permission.in_(permissions)
            )
        )
        models = models.join(
            Model.permissions, isouter=True).filter(conditions)
    return models


class ModelListApi(Resource):
    """ REST API for listing class Model """

    @staticmethod
    @requires_auth
    def get():
        result, result_code = {'status': 'ERROR',
                               'message': 'Internal error'}, 500
        # noinspection PyBroadException
        try:
            if request.args.get('simple') != 'true':
                only = None
            else:
                only = ('id', 'name', 'created',
                        'user_name', 'user_id')

            if request.args.get('fields'):
                only = tuple(
                    [x.strip() for x in request.args.get('fields').split(',')])

            possible_filters = {'enabled': bool, 'type': None, 'user_id': int}
            models = Model.query
            for f, transform in list(possible_filters.items()):
                models = apply_filter(models, request.args, f,
                                      transform, lambda field: field)

            models = _filter_by_permissions(
                models, list(PermissionType.values())).filter(Model.enabled)

            q = request.args.get('query')
            if q:
                models = models.filter(or_(
                    Model.name.like('%%{}%%'.format(q)),
                    Model.type.like('%%{}%%'.format(q))
                ))
            t = request.args.get('type')
            if t:
                models = models.filter(Model.type == t)
            sort = request.args.get('sort', 'name')
            if sort not in ['name', 'id', 'user_id', 'user_name', 'type']:
                sort = 'id'

            sort_option = getattr(Model, sort)
            if request.args.get('asc', 'true') == 'false':
                sort_option = sort_option.desc()

            models = models.order_by(sort_option)

            page = request.args.get('page') or '1'
            if page is not None and page.isdigit():
                page_size = int(request.args.get('size', 20))
                page = int(page)
                pagination = models.paginate(page, page_size, True)
                result = {
                    'data': ModelListResponseSchema(
                        many=True, only=only).dump(pagination.items),
                    'pagination': {
                        'page': page, 'size': page_size,
                        'total': pagination.total,
                        'pages': int(
                            math.ceil(1.0 * pagination.total // page_size))}
                }
            else:
                result = {
                    'data': ModelListResponseSchema(
                        many=True, only=only).dump(models)}
            db.session.commit()
            result_code = 200
        except Exception as ex:
            log.exception(str(ex))

        return result, result_code

    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR", message="Missing json in the request body"), 400
        if request.json is not None:
            overwrite = request.json.pop('overwrite', False)
            request_schema = ModelCreateRequestSchema()
            response_schema = ModelItemResponseSchema()
            try:
                model = request_schema.load(request.json)
                if overwrite:
                    original = Model.query.filter(
                        Model.task_id==request.json['task_id']).first()
                    if original:
                        model.id = original.id
                        db.session.merge(model)
                    else:
                        db.session.add(model)
                else:
                    db.session.add(model)
                db.session.commit()
                result, result_code = response_schema.dump(model), 200
            except ValidationError as e:
                result = dict(status="ERROR", message=gettext('Invalid data'),
                          errors=e.messages)
                result_code = 400
            except Exception as e:
                log.exception('Error in POST')
                result, result_code = dict(status="ERROR",
                                           message=_("Internal error")), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()

        return result, result_code


class ModelDetailApi(Resource):
    """ REST API for a single instance of class Model """

    @staticmethod
    @requires_auth
    def get(model_id):
        filtered = _filter_by_permissions(Model.query,
                                          list(PermissionType.values()))
        model = filtered.filter(Model.id == model_id).first()
        if model is not None:
            return ModelItemResponseSchema().dump(model)
        else:
            return dict(status="ERROR", message=_("%(type)s not found.",
                                                  type=_('Model'))), 404

    @staticmethod
    @requires_auth
    def delete(model_id):
        result, result_code = dict(
            status="ERROR",
            message=_("%(type)s not found.", type=_('Model'))), 404

        filtered = _filter_by_permissions(
            Model.query, [PermissionType.MANAGE, PermissionType.WRITE])
        model = filtered.filter(Model.id == model_id).first()
        if model is not None:
            try:
                model.enabled = False
                db.session.add(model)
                db.session.commit()
                result, result_code = dict(
                    status="OK",
                    message=_("%(what)s was successfuly deleted",
                              what=_('Model'))), 200
            except Exception as e:
                log.exception('Error in DELETE')
                result, result_code = dict(status="ERROR",
                                           message=_("Internal error")), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        return result, result_code

    @staticmethod
    @requires_auth
    def patch(model_id):
        result = dict(status="ERROR", message=_("Insufficient data"))
        result_code = 404

        if request.json:
            request_schema = partial_schema_factory(
                ModelCreateRequestSchema)
            # Ignore missing fields to allow partial updates
            response_schema = ModelItemResponseSchema()
            try:
                model = request_schema.load(request.json, partial=True)
                model.id = model_id
                model = db.session.merge(model)
                db.session.commit()

                if model is not None:
                    result, result_code = dict(
                        status="OK",
                        message=_("%(what)s was successfuly updated",
                                  what=_('Model')),
                        data=response_schema.dump(model)), 200
                else:
                    result = dict(status="ERROR",
                                  message=_("%(type)s not found.",
                                            type=_('Model')))

            except ValidationError as e:
                result = dict(status="ERROR", message=gettext('Invalid data'),
                          errors=e.messages)
                result_code = 400
            except Exception as e:
                log.exception('Error in PATCH')
                result, result_code = dict(status="ERROR",
                                           message=_("Internal error")), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        return result, result_code


class ModelPermissionApi(Resource):
    """ REST API for sharing a Model """

    @staticmethod
    @requires_auth
    def post(model_id, user_id):
        result, result_code = dict(
            status="ERROR", message=_("Missing json in the request body")), 400

        if request.json is not None:
            form = request.json
            to_validate = ['permission', 'user_name', 'user_login']
            error = False
            for check in to_validate:
                if check not in form or form.get(check, '').strip() == '':
                    result, result_code = dict(
                        status="ERROR", message=_("Validation error"),
                        errors={'Missing': check}), 400
                    error = True
                    break
                if check == 'permission' and form.get(
                        'permission') not in list(PermissionType.values()):
                    result, result_code = dict(
                        status="ERROR", message=_("Validation error"),
                        errors={'Invalid': check}), 400
                    error = True
                    break
            if not error:
                try:
                    filtered = _filter_by_permissions(
                        Model.query, [PermissionType.MANAGE])
                    model = filtered.filter(
                        Model.id == model_id).first()

                    if model is not None:
                        conditions = [ModelPermission.model_id ==
                                      model_id,
                                      ModelPermission.user_id == user_id]
                        permission = ModelPermission.query.filter(
                            *conditions).first()

                        action_performed = _('%(what)s saved with success')
                        if permission is not None:
                            permission.permission = form['permission']
                        else:
                            permission = ModelPermission(
                                model=model, user_id=user_id,
                                user_name=form['user_name'],
                                user_login=form['user_login'],
                                permission=form['permission'])

                        db.session.add(permission)
                        db.session.commit()
                        result, result_code = {'message': action_performed,
                                               'status': 'OK'}, 200
                    else:
                        result, result_code = dict(
                            status="ERROR", message=_("%(type)s not found.",
                                                      type=_('Model'))), 404
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message=_("Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
                    db.session.rollback()

        return result, result_code

    @staticmethod
    @requires_auth
    def delete(model_id, user_id):
        result, result_code = dict(
            status="ERROR",
            message=_("%(type)s not found.", type=_('Model'))), 404

        filtered = _filter_by_permissions(Model.query,
                                          [PermissionType.MANAGE])
        model = filtered.filter(Model.id == model_id).first()
        if model is not None:
            permission = ModelPermission.query.filter(
                ModelPermission.model_id == model_id,
                ModelPermission.user_id == user_id).first()
            if permission is not None:
                try:
                    db.session.delete(permission)
                    db.session.commit()
                    result, result_code = dict(
                        status="OK",
                        message=_("%(what)s was successfuly deleted",
                                  what=_('Model'))), 200
                except Exception as e:
                    log.exception(
                        _('Error deleting %(what)s.', what=_('Model')))
                    result, result_code = dict(
                        status="ERROR", message=_("Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
                    db.session.rollback()
        return result, result_code


class ModelUploadApi(Resource):
    """ REST API for upload a Model """

    @staticmethod
    @requires_auth
    def get():
        # noinspection PyBroadException
        try:
            result, result_code = 'OK', 200

            identifier = request.args.get('resumableIdentifier', type=str)
            filename = request.args.get('resumableFilename', type=str)
            chunk_number = request.args.get('resumableChunkNumber', type=int)
            storage_id = request.args.get('storage_id', type=int)

            if not all([storage_id, identifier, filename, chunk_number]):
                result, result_code = {'status': 'ERROR', 'message': gettext(
                    'Missing required parameters')}, 400
            else:
                use_hostname = current_app.config.get(
                    'dfs.client.use.datanode.hostname', True)

                chunk_path, hdfs = upload.create_hdfs_chunk(
                    chunk_number, filename,
                    Storage.query.get(storage_id),
                    use_hostname, current_app.gateway_port)

                current_app.logger.debug('Creating chunk: %s', chunk_path)
                if not hdfs.exists(chunk_path):
                    # The chunk does not exists and needs to be uploaded
                    # by resumable.js
                    result, result_code = {'status': 'OK',
                                           'message': gettext('Not found')}, 404

            return result, result_code
        except Py4JJavaError as java_ex:
            return ModelUploadApi.handle_jvm_error(java_ex)
        except:
            raise

    @staticmethod
    def handle_jvm_error(java_ex):
        log.exception('Java error')
        if 'Could not obtain block' in java_ex.java_exception.getMessage():
            result, status = {'status': 'ERROR',
                              'message': upload.WRONG_HDFS_CONFIG}, 400
        elif 'Could not obtain block' in java_ex.java_exception.getMessage():
            result, status = {'status': 'ERROR',
                              'message': upload.WRONG_HDFS_CONFIG}, 400
        else:
            result, status = {'status': 'ERROR',
                              'message': gettext('Internal error')}, 400
        return result, status

    @staticmethod
    @requires_auth
    def post():
        try:
            result, result_code = 'OK', 200

            identifier = request.args.get('resumableIdentifier', type=str)
            filename = request.args.get('resumableFilename', type=str)
            chunk_number = request.args.get('resumableChunkNumber', type=int)
            total_chunks = request.args.get('resumableTotalChunks', type=int)
            total_size = request.args.get('resumableTotalSize', type=int)
            storage_id = request.args.get('storage_id', type=int)

            if not all([identifier, filename, chunk_number]):
                result, result_code = {'status': 'ERROR', 'message': gettext(
                    'Missing required parameters')}, 400
            else:
                use_hostname = current_app.config.get(
                    'dfs.client.use.datanode.hostname', True)
                conf, jvm = upload.create_gateway_and_hdfs_conf(
                    use_hostname, current_app.gateway_port)

                storage = Storage.query.get(storage_id)
                file_data, hdfs, uri, full_path, counter = upload.write_chunk(
                    jvm, chunk_number, filename, storage, request.get_data(),
                    conf)

                current_app.logger.debug('Wrote chunk: %s', full_path)

                if counter == total_chunks:
                    result_code, result, target_path = upload.merge_chunks(
                        conf, filename, full_path, hdfs, jvm, uri,
                        current_app.config.get('instance', 'unnamed'))
                    if result_code != 500:
                        user = getattr(flask_g, 'user')

                        ds, response_schema = ModelUploadApi.after_merge_chunk(
                            user, storage, filename, target_path, file_data,
                            total_size)
                        result = {'status': 'OK',
                                  'data': response_schema.dump(ds)}

            return result, result_code, {
                'Content-Type': 'application/json; charset=utf-8'}
        except Py4JJavaError as java_ex:
            return ModelUploadApi.handle_jvm_error(java_ex)
        except:
            raise

    @staticmethod
    def after_merge_chunk(user, storage, filename, target_path,
                          file_data=None, total_size=None):
        model = Model(
            name=filename,
            enabled=True,
            created=datetime.datetime.now(),
            path=target_path.toString(),
            class_name=None,
            type=ModelType.UNSPECIFIED,
            user_id=user.id,
            user_login=user.login,
            user_name='{} {}'.format(
                user.first_name.encode('utf8'),
                user.last_name.encode('utf8')).strip(),
            storage_id=storage.id
        )
        db.session.add(model)
        db.session.commit()
        response_schema = ModelItemResponseSchema()
        return model, response_schema
