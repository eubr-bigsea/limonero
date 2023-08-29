# -*- coding: utf-8 -*-}
import logging
import math

from flask import request, current_app, g
from flask_babel import gettext
from flask_restful import Resource
from http import HTTPStatus
from marshmallow.exceptions import ValidationError

from limonero.app_auth import requires_auth, requires_permission
from limonero.schema import *
from limonero.models import *

log = logging.getLogger(__name__)


class DataSourceValidationDetailApi(Resource):
    """ REST API for a single instance of class DataSourceValidation """

    def __init__(self):
            self.human_name = gettext('DataSourceValidation')

    @requires_auth
    def get(self, data_source_validation_id):

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Retrieving %s (id=%s)'), self.human_name,
                      data_source_validation_id)

        data_source_validation = DataSourceValidation.query.get(data_source_validation_id)
        return_code = HTTPStatus.OK
        if data_source_validation is not None:
            result = {
                'status': 'OK',
                'data': [DataSourceValidationItemResponseSchema().dump(
                    data_source_validation)]
            }
        else:
            return_code = HTTPStatus.NOT_FOUND
            result = {
                'status': 'ERROR',
                'message': gettext(
                    '%(name)s not found (id=%(id)s)',
                    name=self.human_name, id=data_source_validation_id)
            }

        return result, return_code

    @requires_auth
    @requires_permission('ADMINISTRATOR',)
    def delete(self, data_source_validation_id):
        return_code = HTTPStatus.OK

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Deleting %s (id=%s)'), self.human_name,
                      data_source_validation_id)

        data_source_validation = DataSourceValidation.query.get(data_source_validation_id)
        if data_source_validation is not None:
            try:
                db.session.delete(data_source_validation)
                db.session.commit()
                result = {
                    'status': 'OK',
                    'message': gettext('%(name)s deleted with success!',
                                       name=self.human_name)
                }
            except Exception as e:
                result = {'status': 'ERROR',
                          'message': gettext("Internal error")}
                return_code = HTTPStatus.INTERNAL_SERVER_ERROR
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        else:
            return_code = HTTPStatus.NOT_FOUND
            result = {
                'status': 'ERROR',
                'message': gettext('%(name)s not found (id=%(id)s).',
                                   name=self.human_name, id=data_source_validation_id)
            }
        return result, return_code

    @requires_auth
    @requires_permission('ADMINISTRATOR',)
    def patch(self, data_source_validation_id):
        result = {'status': 'ERROR', 'message': gettext('Insufficient data.')}
        return_code = HTTPStatus.NOT_FOUND

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Updating %s (id=%s)'), self.human_name,
                      data_source_validation_id)
        
        if request.json:
            request_schema = partial_schema_factory(
                DataSourceValidationCreateRequestSchema)
            # Ignore missing fields to allow partial updates
            data_source_validation = request_schema.load(request.json, partial=True)
            response_schema = DataSourceValidationItemResponseSchema()
            try:
                data_source_validation.id = data_source_validation_id
                data_source_validation = db.session.merge(data_source_validation)
                db.session.commit()

                if data_source_validation is not None:
                    return_code = HTTPStatus.OK
                    result = {
                        'status': 'OK',
                        'message': gettext('%(name)s (id=%(id)s) was updated with success!',
                                            name=self.human_name,
                                            id=data_source_validation_id),
                        'data': [response_schema.dump(data_source_validation)]
                    }
            except ValidationError as e:
                result= {
                   'status': 'ERROR', 
                   'message': gettext('Invalid data for %(name)s (id=%(id)s)',
                                      name=self.human_name,
                                      id=data_source_validation_id),
                   'errors': e.messages
                }
            except Exception as e:
                result = {'status': 'ERROR',
                          'message': gettext("Internal error")}
                return_code = 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        return result, return_code


class DataSourceValidationListApi(Resource):
    """ REST API for listing class DataSourceValidation """

    def __init__(self):
        self.human_name = gettext('DataSourceValidation')

    @requires_auth
    def get(self):
        all_data_source_validations = request.args.get('all') in ["true", 1, "1"]
        only = None

        data_source_id_filter = request.args.get('data_source_id')
        if data_source_id_filter:
            data_source_validations = DataSourceValidation.query.filter(
                DataSourceValidation.data_source_id == data_source_id_filter)
        else:
            data_source_validations = DataSourceValidation.query

        page = request.args.get('page') or '1' 
        if page is not None and page.isdigit():
            page_size = int(request.args.get('size', 20))
            page = int(page)
            pagination = data_source_validations.paginate(page, page_size, True)
            result = {
                'data': DataSourceValidationListResponseSchema(
                    many=True, only=only).dump(pagination.items),
                'pagination': {
                    'page': page, 'size': page_size,
                    'total': pagination.total,
                    'pages': int(math.ceil(1.0 * pagination.total / page_size))}
            }
        else:
            result = {
                'data': DataSourceValidationListResponseSchema(
                    many=True, only=only).dump(
                    data_source_validations)}

        return result

    @requires_auth
    @requires_permission('ADMINISTRATOR',)
    def post(self):
        result = {'status': 'ERROR',
                  'message': gettext("Missing json in the request body")}
        return_code = HTTPStatus.BAD_REQUEST
        
        if request.json is not None:
            request_schema = DataSourceValidationCreateRequestSchema()
            response_schema = DataSourceValidationItemResponseSchema()
            data_source_validation = request_schema.load(request.json)
            try:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug(gettext('Adding %s'), self.human_name)
                    
                data_source_validation = data_source_validation
                db.session.add(data_source_validation)
                db.session.commit()
                result = response_schema.dump(data_source_validation)
                return_code = HTTPStatus.CREATED
            except ValidationError as e:
                result= {
                   'status': 'ERROR', 
                   'message': gettext('Invalid data for %(name)s.)',
                                      name=self.human_name),
                   'errors': e.messages
                }
            except Exception as e:
                result = {'status': 'ERROR',
                          'message': gettext("Internal error")}
                return_code = 500
                if current_app.debug:
                    result['debug_detail'] = str(e)

                log.exception(e)
                db.session.rollback()

        return result, return_code


class DataSourceValidationExecutionDetailApi(Resource):
    """ REST API for a single instance of class DataSourceValidationExecution """

    def __init__(self):
            self.human_name = gettext('DataSourceValidationExecution')

    @requires_auth
    def get(self, data_source_validation_execution_id):

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Retrieving %s (id=%s)'), self.human_name,
                      data_source_validation_execution_id)

        data_source_validation_execution = DataSourceValidationExecution.query.get(data_source_validation_execution_id)
        return_code = HTTPStatus.OK
        if data_source_validation_execution is not None:
            result = {
                'status': 'OK',
                'data': [DataSourceValidationExecutionItemResponseSchema().dump(
                    data_source_validation_execution)]
            }
        else:
            return_code = HTTPStatus.NOT_FOUND
            result = {
                'status': 'ERROR',
                'message': gettext(
                    '%(name)s not found (id=%(id)s)',
                    name=self.human_name, id=data_source_validation_execution_id)
            }

        return result, return_code

    @requires_auth
    @requires_permission('ADMINISTRATOR',)
    def delete(self, data_source_validation_execution_id):
        return_code = HTTPStatus.OK

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Deleting %s (id=%s)'), self.human_name,
                      data_source_validation_execution_id)
        
        data_source_validation_execution = DataSourceValidationExecution.query.get(data_source_validation_execution_id)
        if data_source_validation_execution is not None:
            try:
                db.session.delete(data_source_validation_execution)
                db.session.commit()
                result = {
                    'status': 'OK',
                    'message': gettext('%(name)s deleted with success!',
                                       name=self.human_name)
                }
            except Exception as e:
                result = {'status': 'ERROR',
                          'message': gettext("Internal error")}
                return_code = HTTPStatus.INTERNAL_SERVER_ERROR
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        else:
            return_code = HTTPStatus.NOT_FOUND
            result = {
                'status': 'ERROR',
                'message': gettext('%(name)s not found (id=%(id)s).',
                                   name=self.human_name, id=data_source_validation_execution_id)
            }
        return result, return_code


class DataSourceValidationExecutionListApi(Resource):
    """ REST API for listing class DataSourceValidationExecution """

    def __init__(self):
        self.human_name = gettext('DataSourceValidationExecution')

    @requires_auth
    def get(self):
        all_data_source_validation_executions = request.args.get('all') in ["true", 1, "1"]
        only = None
        
        data_source_validation_id_filter = request.args.get('data_source_validation_id')
        if data_source_validation_id_filter:
            data_source_validation_executions = DataSourceValidationExecution.query.filter(
                DataSourceValidationExecution.data_source_validation_id == data_source_validation_id_filter)
        else:
            data_source_validation_executions = DataSourceValidationExecution.query

        page = request.args.get('page') or '1' 
        if page is not None and page.isdigit():
            page_size = int(request.args.get('size', 20))
            page = int(page)
            pagination = data_source_validation_executions.paginate(page, page_size, True)
            result = {
                'data': DataSourceValidationExecutionListResponseSchema(
                    many=True, only=only).dump(pagination.items),
                'pagination': {
                    'page': page, 'size': page_size,
                    'total': pagination.total,
                    'pages': int(math.ceil(1.0 * pagination.total / page_size))}
            }
        else:
            result = {
                'data': DataSourceValidationExecutionListResponseSchema(
                    many=True, only=only).dump(
                    data_source_validation_executions)}

        return result

    @requires_auth
    @requires_permission('ADMINISTRATOR',)
    def post(self):
        result = {'status': 'ERROR',
                  'message': gettext("Missing json in the request body")}
        return_code = HTTPStatus.BAD_REQUEST
        
        if request.json is not None:
            request_schema = DataSourceValidationExecutionCreateRequestSchema()
            response_schema = DataSourceValidationExecutionItemResponseSchema()
            data_source_validation_execution = request_schema.load(request.json)
            try:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug(gettext('Adding %s'), self.human_name)
                data_source_validation_execution = data_source_validation_execution
                db.session.add(data_source_validation_execution)
                db.session.commit()
                result = response_schema.dump(data_source_validation_execution)
                return_code = HTTPStatus.CREATED
            except ValidationError as e:
                result= {
                   'status': 'ERROR', 
                   'message': gettext('Invalid data for %(name)s.)',
                                      name=self.human_name),
                   'errors': e.messages
                }
            except Exception as e:
                result = {'status': 'ERROR',
                          'message': gettext("Internal error")}
                return_code = 500
                if current_app.debug:
                    result['debug_detail'] = str(e)

                log.exception(e)
                db.session.rollback()

        return result, return_code