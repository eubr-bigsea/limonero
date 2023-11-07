# -*- coding: utf-8 -*-}
import logging
import math
import os
import json

from urllib.parse import urlparse
from flask import g as flask_g, abort
from flask import request, current_app, Response, stream_with_context
from flask_babel import gettext
from flask_restful import Resource
from py4j.protocol import Py4JJavaError
from sqlalchemy import or_, and_
from flask.views import MethodView
from marshmallow.exceptions import ValidationError

from limonero.py4j_init import create_gateway
from limonero.util import upload, parse_hdfs_extra_params, get_hdfs_conf
from .app_auth import requires_auth
from .schema import *

_ = gettext
log = logging.getLogger(__name__)


def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != "":
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


def _filter_by_permissions(models, permissions):
    if flask_g.user.id != 0:  # It is not a inter service call
        conditions = or_(
            Model.user_id == flask_g.user.id,
            flask_g.user.id == 1,
            and_(
                ModelPermission.user_id == flask_g.user.id,
                ModelPermission.permission.in_(permissions),
            ),
        )
        models = models.join(Model.permissions, isouter=True).filter(conditions)
    return models


class ModelListApi(Resource):
    """REST API for listing class Model"""

    @staticmethod
    @requires_auth
    def get():
        result, result_code = {"status": "ERROR", "message": "Internal error"}, 500
        
        if request.args.get("simple") != "true":
            only = None
        else:
            only = ("id", "name", "created", "user_name", "user_id")

        if request.args.get("fields"):
            only = tuple([x.strip() for x in request.args.get("fields").split(",")])

        possible_filters = {"enabled": bool, "type": None, "user_id": int}
        models = Model.query
        for f, transform in list(possible_filters.items()):
            models = apply_filter(
                models, request.args, f, transform, lambda field: field
            )

        models = _filter_by_permissions(
            models, list(PermissionType.values())
        ).filter(Model.enabled)

        q = request.args.get("query")
        if q:
            models = models.filter(
                or_(
                    Model.name.like("%%{}%%".format(q)),
                    Model.type.like("%%{}%%".format(q)),
                )
            )
        t = request.args.get("type")
        if t:
            models = models.filter(Model.type.in_(t.split(",")))
        sort = request.args.get("sort", "name")
        if sort not in ["name", "id", "user_id", "user_name", "type"]:
            sort = "id"

        sort_option = getattr(Model, sort)
        if request.args.get("asc", "true") == "false":
            sort_option = sort_option.desc()

        models = models.order_by(sort_option)

        page = request.args.get("page") or "1"
        if page is not None and page.isdigit():
            page_size = int(request.args.get("size", 20))
            page = int(page)
            pagination = models.paginate(page, page_size, True)
            result = {
                "data": ModelListResponseSchema(many=True, only=only).dump(
                    pagination.items
                ),
                "pagination": {
                    "page": page,
                    "size": page_size,
                    "total": pagination.total,
                    "pages": int(math.ceil(1.0 * pagination.total // page_size)),
                },
            }
        else:
            result = {
                "data": ModelListResponseSchema(many=True, only=only).dump(models)
            }
        db.session.commit()
        result_code = 200

        return result, result_code

    @staticmethod
    @requires_auth
    def post():
        result, result_code = (
            dict(status="ERROR", message="Missing json in the request body"),
            400,
        )
        if request.json is not None:
            overwrite = request.json.pop("overwrite", False)
            request_schema = ModelCreateRequestSchema()
            response_schema = ModelItemResponseSchema()
            
            model = request_schema.load(request.json)
            if overwrite:
                original = Model.query.filter(
                    Model.task_id == request.json["task_id"]
                ).first()
                if original:
                    model.id = original.id
                    db.session.merge(model)
                else:
                    db.session.add(model)
            else:
                db.session.add(model)
            db.session.commit()
            result, result_code = response_schema.dump(model), 200

        return result, result_code


class ModelDetailApi(Resource):
    """REST API for a single instance of class Model"""

    def __init__(self):
        self.human_name = gettext("Model")

    @requires_auth
    def get(self, model_id):
        filtered = _filter_by_permissions(Model.query, 
                                          list(PermissionType.values()))
        model = filtered.filter(Model.id == model_id).first()
        if model is not None:
            return {"status": "OK", 
                    "data": [ModelItemResponseSchema().dump(model)]}
        else:
            return (
                dict(status="ERROR", 
                     message=gettext("%(type)s not found.", 
                                     type=gettext("Model"))), 404,)

    @requires_auth
    def delete(self, model_id):
        result, result_code = (
            dict(
                status="ERROR", message=gettext("%(type)s not found.", type=_("Model"))
            ),
            404,
        )

        filtered = _filter_by_permissions(
            Model.query, [PermissionType.MANAGE, PermissionType.WRITE]
        )
        model = filtered.filter(Model.id == model_id).first()
        if model is not None:
            model.enabled = False
            db.session.add(model)
            db.session.commit()
            result, result_code = (
                dict(
                    status="OK",
                    message=gettext(
                        "%(what)s was successfuly deleted", what=gettext("Model")
                    ),
                ),
                200,
            )
        return result, result_code

    @requires_auth
    def patch(self, model_id):
        result, result_code = {"status": "ERROR", "message": gettext("Insufficient data.")}, 404

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext("Updating %s (id=%s)"), self.human_name, model_id)
        if request.json:
            request_schema = partial_schema_factory(ModelCreateRequestSchema)
            # Ignore missing fields to allow partial updates
            model = request_schema.load(request.json, partial=True)
            response_schema = ModelItemResponseSchema()
            model.id = model_id
            if Model.query.get(model_id):
                model = db.session.merge(model)
                db.session.commit()

                result_code = 200
                result = {
                    "status": "OK",
                    "message": gettext(
                        "%(n)s (id=%(id)s) was updated with success!",
                        n=self.human_name,
                        id=model_id,
                    ),
                    "data": [response_schema.dump(model)],
                }
            else:
                result, result_code = (
                    dict(status="ERROR", message=_("%(type)s not found.", type=_("Model"))),
                    404,
                )
        return result, result_code


class ModelPermissionApi(Resource):
    """REST API for sharing a Model"""

    @staticmethod
    @requires_auth
    def post(model_id, user_id):
        result, result_code = (
            dict(status="ERROR", message=_("Missing json in the request body")),
            400,
        )

        if request.json is not None:
            form = request.json
            to_validate = ["permission", "user_name", "user_login"]
            error = False
            for check in to_validate:
                if check not in form or form.get(check, "").strip() == "":
                    result, result_code = (
                        dict(
                            status="ERROR",
                            message=_("Validation error"),
                            errors={"Missing": check},
                        ),
                        400,
                    )
                    error = True
                    break
                if check == "permission" and form.get("permission") not in list(
                    PermissionType.values()
                ):
                    result, result_code = (
                        dict(
                            status="ERROR",
                            message=_("Validation error"),
                            errors={"Invalid": check},
                        ),
                        400,
                    )
                    error = True
                    break
            if not error:
                filtered = _filter_by_permissions(
                    Model.query, [PermissionType.MANAGE]
                )
                model = filtered.filter(Model.id == model_id).first()

                if model is not None:
                    conditions = [
                        ModelPermission.model_id == model_id,
                        ModelPermission.user_id == user_id,
                    ]
                    permission = ModelPermission.query.filter(*conditions).first()

                    action_performed = _("%(what)s saved with success")
                    if permission is not None:
                        permission.permission = form["permission"]
                    else:
                        permission = ModelPermission(
                            model=model,
                            user_id=user_id,
                            user_name=form["user_name"],
                            user_login=form["user_login"],
                            permission=form["permission"],
                        )

                    db.session.add(permission)
                    db.session.commit()
                    result, result_code = {
                        "message": action_performed,
                        "status": "OK",
                    }, 200
                else:
                    result, result_code = (
                        dict(
                            status="ERROR",
                            message=_("%(type)s not found.", type=_("Model")),
                        ),
                        404,
                    )

        return result, result_code

    @staticmethod
    @requires_auth
    def delete(model_id, user_id):
        result, result_code = (
            dict(status="ERROR", message=_("%(type)s not found.", type=_("Model"))),
            404,
        )

        filtered = _filter_by_permissions(Model.query, [PermissionType.MANAGE])
        model = filtered.filter(Model.id == model_id).first()
        if model is not None:
            permission = ModelPermission.query.filter(
                ModelPermission.model_id == model_id, ModelPermission.user_id == user_id
            ).first()
            if permission is not None:
                db.session.delete(permission)
                db.session.commit()
                result, result_code = (
                    dict(
                        status="OK",
                        message=_(
                            "%(what)s was successfuly deleted", what=_("Model")
                        ),
                    ),
                    200,
                )
        return result, result_code


class ModelUploadApi(Resource):
    """REST API for upload a Model"""

    @staticmethod
    @requires_auth
    def get():
        # noinspection PyBroadException
        try:
            result, result_code = "OK", 200

            identifier = request.args.get("resumableIdentifier", type=str)
            filename = request.args.get("resumableFilename", type=str)
            chunk_number = request.args.get("resumableChunkNumber", type=int)
            storage_id = request.args.get("storage_id", type=int)

            if not all([storage_id, identifier, filename, chunk_number]):
                result, result_code = {
                    "status": "ERROR",
                    "message": gettext("Missing required parameters"),
                }, 400
            else:
                use_hostname = current_app.config.get(
                    "dfs.client.use.datanode.hostname", True
                )

                chunk_path, hdfs = upload.create_hdfs_chunk(
                    chunk_number,
                    filename,
                    Storage.query.get(storage_id),
                    use_hostname,
                    current_app.gateway_port,
                )

                current_app.logger.debug("Creating chunk: %s", chunk_path)
                if not hdfs.exists(chunk_path):
                    # The chunk does not exists and needs to be uploaded
                    # by resumable.js
                    result, result_code = {
                        "status": "OK",
                        "message": gettext("Not found"),
                    }, 404

            return result, result_code
        except Py4JJavaError as java_ex:
            return ModelUploadApi.handle_jvm_error(java_ex)
        except:
            raise

    @staticmethod
    def handle_jvm_error(java_ex):
        log.exception("Java error")
        if "Could not obtain block" in java_ex.java_exception.getMessage():
            result, status = {
                "status": "ERROR",
                "message": upload.WRONG_HDFS_CONFIG,
            }, 400
        elif "Could not obtain block" in java_ex.java_exception.getMessage():
            result, status = {
                "status": "ERROR",
                "message": upload.WRONG_HDFS_CONFIG,
            }, 400
        else:
            result, status = {
                "status": "ERROR",
                "message": gettext("Internal error"),
            }, 400
        return result, status

    @staticmethod
    @requires_auth
    def post():
        try:
            result, result_code = "OK", 200

            identifier = request.args.get("resumableIdentifier", type=str)
            filename = request.args.get("resumableFilename", type=str)
            chunk_number = request.args.get("resumableChunkNumber", type=int)
            total_chunks = request.args.get("resumableTotalChunks", type=int)
            total_size = request.args.get("resumableTotalSize", type=int)
            storage_id = request.args.get("storage_id", type=int)

            if not all([identifier, filename, chunk_number]):
                result, result_code = {
                    "status": "ERROR",
                    "message": gettext("Missing required parameters"),
                }, 400
            else:
                use_hostname = current_app.config.get(
                    "dfs.client.use.datanode.hostname", True
                )
                conf, jvm = upload.create_gateway_and_hdfs_conf(
                    use_hostname, current_app.gateway_port
                )

                storage = Storage.query.get(storage_id)
                file_data, hdfs, uri, full_path, counter = upload.write_chunk(
                    jvm, chunk_number, filename, storage, request.get_data(), conf
                )

                current_app.logger.debug("Wrote chunk: %s", full_path)

                if counter == total_chunks:
                    result_code, result, target_path = upload.merge_chunks(
                        conf,
                        filename,
                        full_path,
                        hdfs,
                        jvm,
                        uri,
                        current_app.config.get("instance", "unnamed"),
                    )
                    if result_code != 500:
                        user = getattr(flask_g, "user")

                        ds, response_schema = ModelUploadApi.after_merge_chunk(
                            user, storage, filename, target_path, file_data, total_size
                        )
                        result = {"status": "OK", "data": response_schema.dump(ds)}

            return (
                result,
                result_code,
                {"Content-Type": "application/json; charset=utf-8"},
            )
        except Py4JJavaError as java_ex:
            return ModelUploadApi.handle_jvm_error(java_ex)
        except:
            raise

    @staticmethod
    def after_merge_chunk(
        user, storage, filename, target_path, file_data=None, total_size=None
    ):
        model = Model(
            name=filename,
            enabled=True,
            created=datetime.datetime.now(),
            path=target_path.toString(),
            class_name=None,
            type=ModelType.UNSPECIFIED,
            user_id=user.id,
            user_login=user.login,
            user_name="{} {}".format(
                user.first_name.encode("utf8"), user.last_name.encode("utf8")
            ).strip(),
            storage_id=storage.id,
        )
        db.session.add(model)
        db.session.commit()
        response_schema = ModelItemResponseSchema()
        return model, response_schema


class ModelDownloadApi(MethodView):
    """Entry point for downloading a Model"""

    # noinspection PyUnresolvedReferences
    @staticmethod
    @requires_auth
    def get(model_id):
        if "ADMINISTRATOR" in flask_g.user.permissions:
            model = Model.query.filter(Model.id == model_id).first()
        else:
            model = Model.query.filter(
                Model.id == model_id, Model.user_id == flask_g.user.id
            ).first()

        if not model:
            abort(404)
        url = model.storage.url
        if url[-1] == "/":
            url = url[:-1]
        parsed = urlparse(f"{url}{model.path}")

        gateway = create_gateway(log, current_app.gateway_port or 18001)
        jvm = gateway.jvm
        content_type = (
            "application/zip"
            if model.type == ModelType.MLEAP
            else "application/x-binary"
        )

        if parsed.scheme == "file":

            def do_download():
                total = 0
                done = False
                with open(parsed.path, "rb") as f:
                    while not done:
                        read_data = f.read(4096)
                        total += len(read_data)
                        if len(read_data) != 4096:
                            done = True
                        yield read_data

            name = parsed.path.split("/")[-1]
            result = Response(stream_with_context(do_download()), mimetype=content_type)

            result.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            result.headers["Pragma"] = "no-cache"
            result.headers["Content-Disposition"] = "attachment; filename={}".format(
                name
            )
            result_code = 200
        else:
            if parsed.port:
                str_uri = "{proto}://{host}:{port}".format(
                    proto=parsed.scheme, host=parsed.hostname, port=parsed.port
                )
            else:
                str_uri = "{proto}://{host}".format(
                    proto=parsed.scheme, host=parsed.hostname
                )
            try:
                uri = jvm.java.net.URI(str_uri)

                extra_params = parse_hdfs_extra_params(model.storage.extra_params)
                conf = get_hdfs_conf(jvm, extra_params, current_app.config)

                hdfs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

                chunk_path = jvm.org.apache.hadoop.fs.Path(parsed.path)
                if not hdfs.exists(chunk_path):
                    result, result_code = (
                        gettext("%(type)s not found.", type=gettext("Data source")),
                        404,
                    )
                else:
                    buf = jvm.java.nio.ByteBuffer.allocate(4096)
                    input_in = hdfs.open(chunk_path)

                    def do_download():
                        total = 0
                        done = False
                        while not done:
                            lido = input_in.read(buf)
                            total += lido
                            buf.position(0)
                            if lido != 4096:
                                done = True
                                yield bytes(buf.array())[:lido]
                            else:
                                yield bytes(buf.array())

                    name = parsed.path.split("/")[-1]

                    result = Response(
                        stream_with_context(do_download()), mimetype=content_type
                    )

                    result.headers[
                        "Cache-Control"
                    ] = "no-cache, no-store, must-revalidate"
                    result.headers["Pragma"] = "no-cache"
                    result.headers[
                        "Content-Disposition"
                    ] = "attachment; filename={}".format(name)
                    result_code = 200
            except Py4JJavaError as java_ex:
                if "Could not obtain block" in java_ex.java_exception.getMessage():
                    return {"status": "ERROR", "message": WRONG_HDFS_CONFIG}, 400
                log.exception("Java error")

        return result, result_code
