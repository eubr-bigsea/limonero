# -*- coding: utf-8 -*-
import logging

from flask import request, current_app
from flask_babel import gettext
from flask_restful import Resource

from app_auth import requires_auth
from schema import *

_ = gettext
log = logging.getLogger(__name__)


class GlobalPrivacyListApi(Resource):
    """ REST API for a managing data source privacy """

    @staticmethod
    @requires_auth
    def get():
        attr_privacy = AttributePrivacy.query \
            .filter(AttributePrivacy.is_global_law) \
            .all()
        return {'data': AttributePrivacyListResponseSchema().dump(
            attr_privacy, many=True).data}

    @staticmethod
    @requires_auth
    def patch():
        result = dict(status="ERROR", message=_("Insufficient data"))
        result_code = 400
        json_data = request.json or json.loads(request.data)
        if json_data:
            request_schema = partial_schema_factory(
                AttributePrivacyCreateRequestSchema)

            form = request_schema.load(json_data, partial=True)
            response_schema = AttributePrivacyListResponseSchema()

            if not form.errors:
                try:
                    if form.data.id == 0:  # New
                        form.data.id = None

                    attribute_privacy = db.session.merge(form.data)
                    attribute_privacy.is_global_law = True
                    db.session.commit()
                    if attribute_privacy is not None:
                        result, result_code = dict(
                            status="OK",
                            message=_("%(what)s was successfuly updated",
                                      what=_('Privacy')),
                            data=response_schema.dump(
                                attribute_privacy).data), 200
                    else:
                        result = dict(status="ERROR",
                                      message=_("%(type)s not found.",
                                                type=_('Privacy')))
                except Exception as e:
                    current_app.logger.exception(e)
                    log.exception('Error in PATCH')
                    result, result_code = dict(
                        status="ERROR", message=_("Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()
            else:
                result = dict(status="ERROR", message=_("Invalid data"),
                              errors=form.errors)
        return result, result_code

    @staticmethod
    @requires_auth
    def delete():
        result, result_code = dict(status="ERROR",
                                   message=_("%(type)s not found.",
                                             type=_('Privacy'))), 404

        json_data = request.json or json.loads(request.data)
        if json_data:
            attribute_privacy = AttributePrivacy.query.get(json_data['id'])
            if attribute_privacy is not None:
                try:
                    db.session.delete(attribute_privacy)
                    db.session.commit()
                    result, result_code = dict(
                        status="OK",
                        message=_("%(what)s was successfuly deleted",
                                  what=_('Privacy'))), 200
                except Exception as e:
                    log.exception(
                        _('Error deleting %(what)s.', what=_('Privacy')))
                    result, result_code = dict(
                        status="ERROR", message=_("Internal error")), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
                    db.session.rollback()

        return result, result_code


class AttributePrivacyGroupListApi(Resource):
    """ REST API for listing class Storage """

    @staticmethod
    @requires_auth
    def get():
        only = ('id', 'name') \
            if request.args.get('simple', 'false') == 'true' else None
        storages = AttributePrivacyGroup.query.all()

        return AttributePrivacyGroupListResponseSchema(
            many=True, only=only).dump(storages).data


class AttributePrivacyGroupDetailApi(Resource):
    """ REST API for a single instance of class Storage """

    @staticmethod
    @requires_auth
    def get(storage_id):
        storage = AttributePrivacyGroup.query.get(storage_id)
        if storage is not None:
            return AttributePrivacyGroupItemResponseSchema().dump(storage).data
        else:
            return dict(status="ERROR", message=_("%(type)s not found.",
                                                  type=_('Privacy'))), 404
