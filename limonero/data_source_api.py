# -*- coding: utf-8 -*-}
from app_auth import requires_auth
from flask import request, current_app, g
from flask_restful import Resource

from app_auth import requires_auth
from schema import *
from sqlalchemy import or_


def _get_data_sources(data_sources, permissions):
    if g.user.id != 0:  # It is not a inter service call
        data_sources = data_sources.filter(or_(
            DataSource.user_id == g.user.id,
            DataSource.permissions.any(
                DataSourcePermission.permission.in_(permissions))))
    return data_sources

log = logging.getLogger(__name__)


class DataSourceListApi(Resource):
    """ REST API for listing class DataSource """

    @staticmethod
    @requires_auth
    def get():
        if request.args.get('fields'):
            only = [x.strip() for x in
                    request.args.get('fields').split(',')]
        else:
            only = ('id', 'name', 'attributes.name',
                    'attributes.type', 'user_id') \
                if request.args.get('simple', 'false') == 'true' else None

        data_sources = _get_data_sources(
            DataSource.query, [PermissionType.READ, PermissionType.MANAGE])

        possible_filters = ['enabled', 'format']
        for f in possible_filters:
            if f in request.args:
                v = {f: request.args.get(f)}
                data_sources = data_sources.filter_by(**v)

        return DataSourceListResponseSchema(
            many=True, only=only).dump(data_sources).data

    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR", message="Missing json in the request body"), 401
        if request.json is not None:
            request_schema = DataSourceCreateRequestSchema()
            response_schema = DataSourceItemResponseSchema()
            form = request_schema.load(request.json)
            if form.errors:
                result, result_code = dict(
                    status="ERROR", message="Validation error",
                    errors=form.errors), 401
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
        if request.args.get('fields'):
            only = [x.strip() for x in
                    request.args.get('fields').split(',')]
        else:
            only = ('id', 'name', 'attributes.name',
                    'attributes.type', 'user_id') \
                if request.args.get('simple', 'false') == 'true' else None

        data_sources = _get_data_sources(
            DataSource.query.filter(DataSource.id == data_source_id),
            [PermissionType.READ, PermissionType.MANAGE])

        data_sources = data_sources.all()
        if len(data_sources) == 1:
            data_source = data_sources[0]
            return DataSourceItemResponseSchema(only=only).dump(
                data_source).data
        else:
            return dict(status="ERROR", message="Not found"), 404

    @staticmethod
    @requires_auth
    def delete(data_source_id):
        result, result_code = dict(status="ERROR", message="Not found"), 404

        data_sources = _get_data_sources(
            DataSource.query.filter(DataSource.id == data_source_id),
            [PermissionType.MANAGE])

        if len(data_sources) == 1:
            data_source = data_sources[0]
            try:
                db.session.delete(data_source)
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
        result_code = 404

        if request.json:
            request_schema = partial_schema_factory(
                DataSourceCreateRequestSchema)
            # Ignore missing fields to allow partial updates
            form = request_schema.load(request.json, partial=True)
            response_schema = DataSourceItemResponseSchema()
            if not form.errors:
                try:
                    data_sources = _get_data_sources(DataSource.query.filter(
                        DataSource.id == data_source_id),
                        [PermissionType.MANAGE])

                    if len(data_sources) == 1:
                        form.data.id = data_source_id
                        data_source = db.session.merge(form.data)
                        db.session.commit()

                        if data_source is not None:
                            result, result_code = dict(
                                status="OK", message="Updated",
                                data=response_schema.dump(
                                    data_source).data), 200
                        else:
                            result = dict(status="ERROR", message="Not found")
                    else:
                        result = dict(status="ERROR", message="Not found")
                except Exception as e:
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
