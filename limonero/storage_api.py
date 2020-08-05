# -*- coding: utf-8 -*-}
from flask import g as flask_g
from flask import request
from flask_babel import gettext
from flask_restful import Resource

from requests import compat as req_compat
from .app_auth import requires_auth
from .schema import *

_ = gettext


class StorageListApi(Resource):
    """ REST API for listing class Storage """

    @staticmethod
    @requires_auth
    def get():
        only = ('id', 'name') \
            if request.args.get('simple', 'false') == 'true' else None
        storages = Storage.query.filter(Storage.enabled).order_by('name')
        user = getattr(flask_g, 'user')

        # Administrative have access to URL
        exclude = tuple() if user.id in (0, 1) else tuple(['url'])

        return StorageListResponseSchema(many=True, only=only,
                                         exclude=exclude).dump(storages).data


class StorageDetailApi(Resource):
    """ REST API for a single instance of class Storage """

    @staticmethod
    @requires_auth
    def get(storage_id):
        user = getattr(flask_g, 'user')
        exclude = tuple() if user.id in (0, 1) else tuple(['url'])

        storage = Storage.query.filter(Storage.enabled,
                                       Storage.id == storage_id).first()
        if storage is not None:
            return StorageItemResponseSchema(exclude=exclude).dump(storage).data
        else:
            return dict(status="ERROR",
                        message=_("%(type)s not found.",
                                  type=_('Storage'))), 404

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
