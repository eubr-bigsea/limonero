#!/usr/bin/env python

import argparse
import itertools
import logging
import logging.config
import os
import signal

import sqlalchemy_utils
import yaml
from flask import Flask, request, g as flask_g
from flask_babel import get_locale, Babel
from flask_babel import gettext
from flask_cors import CORS
from flask_migrate import Migrate
from flask_restful import Api, abort
from flask_swagger_ui import get_swaggerui_blueprint

from limonero import CustomJSONEncoder as LimoneroJSONEncoder
from limonero.cache import cache
from limonero.data_source_api import DataSourceDetailApi, DataSourceListApi, \
    DataSourcePermissionApi, DataSourceUploadApi, DataSourceInferSchemaApi, \
    DataSourcePrivacyApi, DataSourceDownload, DataSourceSampleApi, \
    DataSourceInitializationApi
from limonero.model_api import ModelDetailApi, ModelListApi, ModelDownloadApi
from limonero.models import db, DataSource, Storage
from limonero.py4j_init import init_jvm
from limonero.storage_api import StorageDetailApi, StorageListApi, \
    StorageMetadataApi
from cryptography.fernet import Fernet

from marshmallow.exceptions import ValidationError
from werkzeug.exceptions import HTTPException

from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)
os.chdir(os.environ.get('LIMONERO_HOME', '.'))

# noinspection PyUnusedLocal
def exit_gracefully(s, frame):
    os.kill(os.getpid(), signal.SIGTERM)

def translate_validation(validation_errors):
    for field, errors in list(validation_errors.items()):
        validation_errors[field] = [gettext(error) for error in errors]
    return validation_errors


def create_app(main_module: bool = False):
    app = Flask(__name__, static_url_path='/static', static_folder='static')
    
    app.config['BABEL_TRANSLATION_DIRECTORIES'] = os.path.abspath(
        'limonero/i18n/locales')
    app.json_encoder = LimoneroJSONEncoder

    # Error handlers
    @app.errorhandler(ValidationError)
    def register_validation_error(e):
        result = {'status': 'ERROR',
                  'message': gettext("Validation error"),
                  'errors': translate_validation(e.messages)}
        db.session.rollback()
        return result, 400
    
    @app.errorhandler(Exception)
    def handle_exception(e):
        # pass through HTTP errors
        if isinstance(e, HTTPException):
            return e
        result = {'status': 'ERROR',
                  'message': gettext("Internal error")}
        if app.debug:
            result['debug_detail'] = str(e)

        log.exception(e)
        db.session.rollback()
        return result, 500



    
    babel = Babel(app)
    
    logging.config.fileConfig('logging_config.ini')
    
    app.secret_key = 'l3m0n4d1'
    
    
    # Cryptography key
    app.download_key = Fernet.generate_key()
    app.fernet = Fernet(app.download_key)
    
    
    # Cache
    cache.init_app(app)
    
    # CORS
    CORS(app, resources={r"/*": {"origins": "*"}})
    api = Api(app)
    
    # Swagger
    swaggerui_blueprint = get_swaggerui_blueprint(
        '/api/docs',  
        '/static/swagger.yaml',
        config={  # Swagger UI config overrides
            'app_name': "Lemonade Caipirinha"
        },
        # oauth_config={  # OAuth config. See https://github.com/swagger-api/swagger-ui#oauth2-configuration .
        #    'clientId': "your-client-id",
        #    'clientSecret': "your-client-secret-if-required",
        #    'realm': "your-realms",
        #    'appName': "your-app-name",
        #    'scopeSeparator': " ",
        #    'additionalQueryStringParams': {'test': "hello"}
        # }
    )
    
    app.register_blueprint(swaggerui_blueprint)
    
    mappings = {
        '/datasources': DataSourceListApi,
        '/datasources/upload': DataSourceUploadApi,
        '/datasources/infer-schema/<int:data_source_id>': DataSourceInferSchemaApi,
        '/datasources/sample/<int:data_source_id>': DataSourceSampleApi,
        '/datasources/initialize/<status>/<int:data_source_id>': 
            DataSourceInitializationApi,
        '/datasources/<int:data_source_id>': DataSourceDetailApi,
        '/datasources/<int:data_source_id>/permission/<int:user_id>':
            DataSourcePermissionApi,
        '/models': ModelListApi,
        '/models/<int:model_id>': ModelDetailApi,
    
        '/storages': StorageListApi,
        '/storages/<int:storage_id>': StorageDetailApi,
        '/storages/metadata/<int:storage_id>': StorageMetadataApi,
    }
    grouped_mappings = itertools.groupby(sorted(mappings.items()),
                                         key=lambda path: path[1])
    for view, g in grouped_mappings:
        api.add_resource(view, *[x[0] for x in g], endpoint=view.__name__)
    
    app.add_url_rule('/datasources/public/<int:data_source_id>/download',
                     methods=['GET'], endpoint='DataSourceDownload',
                     view_func=DataSourceDownload.as_view('download'))

    app.add_url_rule('/models/<int:model_id>/download',
                     methods=['GET'], endpoint='ModelDownloadApi',
                     view_func=ModelDownloadApi.as_view('download_model'))
    migrate = Migrate(app, db)
    app.handle_exception

    @babel.localeselector
    def get_locale():
        user = getattr(flask_g, 'user', None)
        if user is not None and user.locale:
            return user.locale
        else:
            return request.args.get(
                'lang', request.accept_languages.best_match(['en', 'pt', 'es']))
    
    sqlalchemy_utils.i18n.get_locale = get_locale

    config_file = None
    signal.signal(signal.SIGINT, exit_gracefully)
    if main_module:
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", type=str,
                            help="Config file", required=False)
        args = parser.parse_args()
        config_file = args.config

    if config_file is None:
        config_file = os.environ.get('LIMONERO_CONFIG')

    logger = logging.getLogger(__name__)
    if config_file:
        with open(config_file) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)['limonero']

        app.config['LIMONERO_CONFIG'] = config
        app.config["RESTFUL_JSON"] = {"cls": app.json_encoder}

        server_config = config.get('servers', {})
        app.config['SQLALCHEMY_DATABASE_URI'] = server_config.get(
            'database_url')
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

        is_mysql = 'mysql://' in app.config['SQLALCHEMY_DATABASE_URI']
        if config.get('config') is not None and 'config' in config and is_mysql:
            app.config.update(config.get('config', {}))
            app.config['SQLALCHEMY_POOL_SIZE'] = 10
            app.config['SQLALCHEMY_POOL_RECYCLE'] = 240

        db.init_app(app)

        port = int(config.get('port', 5000))
        logger.debug(
            gettext('Running in %(mode)s mode', mode=config.get('environment')))

        init_jvm(app, logger)
        if main_module:
            # JVM, used to interact with HDFS.
            if config.get('environment', 'dev') == 'dev':
                app.run(debug=True, port=port, host='0.0.0.0')
        else:
            return app    
    else:
        logger.error(
            gettext('Please, set LIMONERO_CONFIG environment variable'))
        exit(1)
    return app


if __name__ == '__main__':
    create_app(__name__ == '__main__')
