import json
import logging
import math
from urllib.parse import urlparse

from flask import g as flask_g
from flask import request
from flask_babel import gettext
from flask_restful import Resource
from sqlalchemy import or_

from limonero.app_auth import requires_auth, requires_permission
from limonero.models import Storage, db
from limonero.schema import (StorageCreateRequestSchema,
                             StorageItemResponseSchema,
                             StorageListResponseSchema, partial_schema_factory)

log = logging.getLogger(__name__)


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

        storages = Storage.query

        # Filtering
        is_enabled = request.args.get('enabled')
        if is_enabled is not None:
            storages = storages.filter(Storage.enabled == ('true' == is_enabled))
        
        query = request.args.get('query') or request.args.get('name')
        if query:
            storages = storages.filter(or_(
                Storage.name.ilike(f'%%{query}%%'),
                Storage.type.ilike(f'%%{query}%%'),
            ))

        # Sorting
        sort = request.args.get('sort', 'id')
        sort_option = (getattr(Storage, sort) 
                if sort in ['name', 'id', 'type'] else Storage.id)
        sort_option = (sort_option.desc() 
                if request.args.get('asc', True) == False else sort_option)
        storages = storages.order_by(sort_option)

         
        # Administrative have access to URL
        user = getattr(flask_g, 'user')
        exclude = tuple() if user.id in (0, 1) else tuple(['url'])

        # Pagination
        page = request.args.get('page', type=int, default=1)
        page_size = request.args.get('size', type=int, default=20)
        pagination = storages.paginate(page, page_size, True)

        result = {
            'data': StorageListResponseSchema(
                many=True, only=only, exclude=exclude).dump(pagination.items),
            'pagination': {
                'page': page, 'size': page_size,
                'total': pagination.total,
                'pages': int(math.ceil(1.0 * pagination.total / page_size))}
        }

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

            storage = request_schema.load(request.json)
            if log.isEnabledFor(logging.DEBUG):
                log.debug(gettext('Adding %s'), self.human_name)
            db.session.add(storage)
            db.session.commit()
            result = response_schema.dump(storage)
            return_code = 200

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
                    storage)]
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
        return_code = 204
        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Deleting %s (id=%s)'), self.human_name,
                      storage_id)
        storage = Storage.query.get(storage_id)
        if storage is not None:
            storage.enabled = False
            db.session.add(storage)
            db.session.commit()
            result = {
                'status': 'OK',
                'message': gettext('%(name)s deleted with success!',
                                   name=self.human_name)
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
            storage = request_schema.load(request.json, partial=True)
            response_schema = StorageItemResponseSchema()
            storage.id = storage_id
            if Storage.query.get(storage_id):
                storage = db.session.merge(storage)
                db.session.commit()

                return_code = 200
                result = {
                    'status': 'OK',
                    'message': gettext(
                        '%(n)s (id=%(id)s) was updated with success!',
                        n=self.human_name,
                        id=storage_id),
                    'data': [response_schema.dump(
                        storage)]
                }
        return result, return_code


class StorageMetadataApi(Resource):
    """ REST API for a single instance of class storage metadata"""

    @staticmethod
    @requires_auth
    def get(storage_id):
        # user = getattr(flask_g, 'user')
        # exclude = tuple() if user.id in (0, 1) else tuple(['url'])

        storage = Storage.query.filter(Storage.enabled,
                                       Storage.id == storage_id).first()

        parsed = urlparse(
            next((a for a in [storage.client_url, storage.url]
                  if a), None))
        result = {}
        status_code = 200
        try:
            if storage is not None:
                if storage.type in ('HIVE', 'HIVE_WAREHOUSE'):
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
                        kerberos_service_name=extra.get(
                            'kerberos_service_name'),
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
                    pass  # FIXME: Implement
                return result, status_code
            else:
                return dict(status="ERROR",
                            message=gettext("%(type)s not found.",
                                      type=gettext('Storage'))), 404
        except Exception as ex:
            return dict(status="ERROR", message=str(ex)), 500
