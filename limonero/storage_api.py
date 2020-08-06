# -*- coding: utf-8 -*-}
from limonero.app_auth import requires_auth, requires_permission
from flask import request, current_app, g as flask_globals
from flask_restful import Resource
from sqlalchemy import or_
from flask import g as flask_g

import math
import logging
from requests import compat as req_compat
from limonero.schema import *
from flask_babel import gettext

log = logging.getLogger(__name__)


_ = gettext


class StorageListApi(Resource):
    """ REST API for listing class Storage """

    def __init__(self):
        self.human_name = gettext('Storage')

    @requires_auth
    def get(self):
        if request.args.get('fields'):
            only = [f.strip() for f in request.args.get('fields').split(',')]
        else:
            only = ('id', ) if request.args.get(
                'simple', 'false') == 'true' else None
        enabled_filter = request.args.get('enabled')
        if enabled_filter:
            storages = Storage.query.filter(
                Storage.enabled == (enabled_filter != 'false'))
        else:
            storages = Storage.query
        
        sort = request.args.get('sort', 'name')
        if sort not in ['name', 'id', 'type']:
            sort = 'id'

        sort_option = getattr(Storage, sort)
        if request.args.get('asc', 'true') == 'false':
            sort_option = sort_option.desc()
        
        storages = storages.order_by(sort_option)

        query = request.args.get('query') or request.args.get('name')
        if query:
            storages = storages.filter(or_(
                Storage.name.ilike('%%{}%%'.format(query)),
                Storage.type.ilike('%%{}%%'.format(query)),
            ))


        user = getattr(flask_g, 'user')
        # Administrative have access to URL
        exclude = tuple() if user.id in (0, 1) else tuple(['url'])

        page = request.args.get('page') or '1'
        if page is not None and page.isdigit():
            page_size = int(request.args.get('size', 20))
            page = int(page)
            pagination = storages.paginate(page, page_size, True)
            result = {
                'data': StorageListResponseSchema(
                    many=True, only=only, exclude=exclude).dump(pagination.items).data,
                'pagination': {
                    'page': page, 'size': page_size,
                    'total': pagination.total,
                    'pages': int(math.ceil(1.0 * pagination.total / page_size))}
            }
        else:
            result = {
                'data': StorageListResponseSchema(
                    many=True, only=only, exclude=exclude).dump(
                    storages).data}

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Listing %(name)s', name=self.human_name))
        return result

    @requires_auth
    @requires_permission('ADMINISTRATOR')
    def post(self):
        result = {'status': 'ERROR',
                  'message': gettext("Missing json in the request body")}
        return_code = 400
        
        if request.json is not None:
            request_schema = StorageCreateRequestSchema()
            response_schema = StorageItemResponseSchema()
            form = request_schema.load(request.json)
            if form.errors:
                result = {'status': 'ERROR',
                          'message': gettext("Validation error"),
                          'errors': translate_validation(form.errors)}
            else:
                try:
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug(gettext('Adding %s'), self.human_name)
                    storage = form.data
                    db.session.add(storage)
                    db.session.commit()
                    result = response_schema.dump(storage).data
                    return_code = 200
                except Exception as e:
                    result = {'status': 'ERROR',
                              'message': gettext("Internal error")}
                    return_code = 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)

                    log.exception(e)
                    db.session.rollback()

        return result, return_code


class StorageDetailApi(Resource):
    """ REST API for a single instance of class Storage """
    def __init__(self):
        self.human_name = gettext('Storage')

    @requires_auth
    def get(self, storage_id):

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Retrieving %s (id=%s)'), self.human_name,
                      storage_id)

        storage = Storage.query.get(storage_id)
        return_code = 200

        user = getattr(flask_g, 'user')
        exclude = tuple() if user.id in (0, 1) else tuple(['url'])

        if storage is not None:
            result = {
                'status': 'OK',
                'data': [StorageItemResponseSchema(exclude=exclude).dump(
                    storage).data]
            }
        else:
            return_code = 404
            result = {
                'status': 'ERROR',
                'message': gettext(
                    '%(name)s not found (id=%(id)s)',
                    name=self.human_name, id=storage_id)
            }

        return result, return_code

    @requires_auth
    @requires_permission('ADMINISTRATOR')
    def delete(self, storage_id):
        return_code = 200
        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Deleting %s (id=%s)'), self.human_name,
                      storage_id)
        storage = Storage.query.get(storage_id)
        if storage is not None:
            try:
                storage.enabled = False
                db.session.add(storage)
                db.session.commit()
                result = {
                    'status': 'OK',
                    'message': gettext('%(name)s deleted with success!',
                                       name=self.human_name)
                }
            except Exception as e:
                result = {'status': 'ERROR',
                          'message': gettext("Internal error")}
                return_code = 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        else:
            return_code = 404
            result = {
                'status': 'ERROR',
                'message': gettext('%(name)s not found (id=%(id)s).',
                                   name=self.human_name, id=storage_id)
            }
        return result, return_code

    @requires_auth
    @requires_permission('ADMINISTRATOR')
    def patch(self, storage_id):
        result = {'status': 'ERROR', 'message': gettext('Insufficient data.')}
        return_code = 404

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Updating %s (id=%s)'), self.human_name,
                      storage_id)
        if request.json:
            request_schema = partial_schema_factory(
                StorageCreateRequestSchema)
            # Ignore missing fields to allow partial updates
            form = request_schema.load(request.json, partial=True)
            response_schema = StorageItemResponseSchema()
            if not form.errors:
                try:
                    form.data.id = storage_id
                    storage = db.session.merge(form.data)
                    db.session.commit()

                    if storage is not None:
                        return_code = 200
                        result = {
                            'status': 'OK',
                            'message': gettext(
                                '%(n)s (id=%(id)s) was updated with success!',
                                n=self.human_name,
                                id=storage_id),
                            'data': [response_schema.dump(
                                storage).data]
                        }
                except Exception as e:
                    result = {'status': 'ERROR',
                              'message': gettext("Internal error")}
                    return_code = 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
                    db.session.rollback()
            else:
                result = {
                    'status': 'ERROR',
                    'message': gettext('Invalid data for %(name)s (id=%(id)s)',
                                       name=self.human_name,
                                       id=storage_id),
                    'errors': form.errors
                }
        return result, return_code

class StorageMetadataApi(Resource):
    """ REST API for a single instance of class storage metadata"""

    @staticmethod
    @requires_auth
    def get(storage_id):
        user = getattr(flask_g, 'user')
        exclude = tuple() if user.id in (0, 1) else tuple(['url'])

        storage = Storage.query.filter(Storage.enabled,
                                       Storage.id == storage_id).first()

        parsed = req_compat.urlparse(
            next((a for a in [storage.client_url, storage.url] 
                if a), None))
        result = {}
        try:
            if storage is not None:
                if storage.type == 'HIVE':
                    from pyhive import hive
                    from TCLIService.ttypes import TOperationState
                    extra = {}
                    if storage.extra_params:
                        extra = json.loads(storage.extra_params)
                    auth = extra.get('auth', 'CUSTOM')
                    if not parsed.password:
                        auth = None
                    cursor = hive.connect(
                           host=parsed.hostname, 
                           port=int(parsed.port or 10000), 
                           username=parsed.username,
                           password=parsed.password if parsed.password else None,
                           database=(parsed.path or 'default').replace('/', ''), 
                           auth=auth,
                           configuration={
                               'hive.cli.print.header': 'true'}, 
                           kerberos_service_name=extra.get('kerberos_service_name'), 
                           thrift_transport=None).cursor()

                    tables = []
                    for cmd in ['SHOW TABLES', 'SHOW VIEWS']: 
                        cursor.execute(cmd, async_=True)
                        status = cursor.poll().operationState
                        while status in (TOperationState.INITIALIZED_STATE, 
                                TOperationState.RUNNING_STATE):
                            status = cursor.poll().operationState
                        tables.extend(row[0] for row in cursor)
                    result, status_code = dict(status='OK',
                                               data=tables), 200
                    cursor.close()
                elif storage.type == 'JDBC':
                    pass # FIXME: Implement
                return result
            else:
                return dict(status="ERROR",
                        message=_("%(type)s not found.",
                                  type=_('Storage'))), 404
        except Exception as ex:
                return dict(status="ERROR", message=str(ex)), 500
