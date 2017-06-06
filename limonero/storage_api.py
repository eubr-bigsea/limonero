# -*- coding: utf-8 -*-}
from app_auth import requires_auth
from flask import request, current_app
from flask_restful import Resource
from schema import *
from flask import g
from sqlalchemy import or_


def _get_storages(storages, permissions):
    if g.user.id != 0:  # It is not a inter service call
        storages = storages.filter(or_(
            Storage.user_id == g.user.id,
            Storage.permissions.any(
                StoragePermission.permission.in_(permissions))))
    return storages


class StorageListApi(Resource):
    """ REST API for listing class Storage """

    @staticmethod
    @requires_auth
    def get():
        only = ('id', 'name') \
            if request.args.get('simple', 'false') == 'true' else None
        storages = _get_storages(Storage.query,
                                 [PermissionType.WRITE, PermissionType.MANAGE])

        return StorageListResponseSchema(
            many=True, only=only).dump(storages.all()).data


class StorageDetailApi(Resource):
    """ REST API for a single instance of class Storage """

    @staticmethod
    @requires_auth
    def get(storage_id):
        storages = _get_storages(Storage.query.filter(Storage.id == storage_id),
                                 [PermissionType.WRITE, PermissionType.MANAGE])
        storages = storages.all()
        if len(storages) == 1:
            return StorageItemResponseSchema().dump(storages[0]).data
        else:
            return dict(status="ERROR", message="Not found"), 404
