# -*- coding: utf-8 -*-}
from flask import g as flask_g
from flask import request
from flask_babel import gettext
from flask_restful import Resource

from app_auth import requires_auth
from schema import *

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
