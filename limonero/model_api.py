# -*- coding: utf-8 -*-}
import logging
import math

from flask import g
from flask import request, current_app
from flask_babel import gettext
from flask_restful import Resource
from sqlalchemy import or_, and_

from app_auth import requires_auth
from schema import *

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
        result, result_code = 'Internal error', 500
        # noinspection PyBroadException
        try:
            if request.args.get('simple') != 'true':
                only = None
            else:
                only = ('id', 'name', 'created',
                        'user_name', 'permissions', 'user_id')

            if request.args.get('fields'):
                only = tuple(
                    [x.strip() for x in request.args.get('fields').split(',')])

            possible_filters = {'enabled': bool, 'format': None, 'user_id': int}
            models = Model.query
            for f, transform in possible_filters.items():
                models = apply_filter(models, request.args, f,
                                      transform, lambda field: field)

            models = _filter_by_permissions(
                models, PermissionType.values())

            sort = request.args.get('sort', 'name')
            if sort not in ['name', 'id', 'user_id', 'user_name']:
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
                        many=True, only=only).dump(pagination.items).data,
                    'pagination': {
                        'page': page, 'size': page_size,
                        'total': pagination.total,
                        'pages': int(
                            math.ceil(1.0 * pagination.total / page_size))}
                }
            else:
                result = {
                    'data': ModelListResponseSchema(
                        many=True, only=only).dump(models).data}
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
            request_schema = ModelCreateRequestSchema()
            response_schema = ModelItemResponseSchema()
            form = request_schema.load(request.json)
            if form.errors:
                result, result_code = dict(
                    status="ERROR", message=_("Validation error"),
                    errors=form.errors), 400
            else:
                try:
                    model = form.data
                    db.session.add(model)
                    db.session.commit()
                    result, result_code = response_schema.dump(
                        model).data, 200
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message=_("Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()

        return result, result_code


class ModelDetailApi(Resource):
    """ REST API for a single instance of class Model """

    @staticmethod
    @requires_auth
    def get(model_id):
        filtered = _filter_by_permissions(Model.query,
                                          PermissionType.values())
        model = filtered.filter(Model.id == model_id).first()
        if model is not None:
            return ModelItemResponseSchema().dump(model).data
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
                    result['debug_detail'] = e.message
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
            form = request_schema.load(request.json, partial=True)
            response_schema = ModelItemResponseSchema()
            if not form.errors:
                try:
                    form.data.id = model_id
                    model = db.session.merge(form.data)
                    db.session.commit()

                    if model is not None:
                        result, result_code = dict(
                            status="OK",
                            message=_("%(what)s was successfuly updated",
                                      what=_('Model')),
                            data=response_schema.dump(model).data), 200
                    else:
                        result = dict(status="ERROR",
                                      message=_("%(type)s not found.",
                                                type=_('Model')))
                except Exception as e:
                    log.exception('Error in PATCH')
                    result, result_code = dict(status="ERROR",
                                               message=_("Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()
            else:
                result = dict(status="ERROR", message=_("Invalid data"),
                              errors=form.errors)
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
                        'permission') not in PermissionType.values():
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
                        result['debug_detail'] = e.message
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
                        result['debug_detail'] = e.message
                    db.session.rollback()
        return result, result_code
