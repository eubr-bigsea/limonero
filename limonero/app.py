#!/usr/bin/env python
# -*- coding: utf-8 -*-

# noinspection PyBroadException
try:
    import eventlet

    eventlet.monkey_patch(all=True, thread=False)
except:
    pass

import argparse
import itertools
import logging
import logging.config
import os
import signal

import eventlet.wsgi
import sqlalchemy_utils
import yaml
from flask import Flask, request, g as flask_g
from flask_admin import Admin
from flask_babel import get_locale, Babel
from flask_babel import gettext
from flask_cors import CORS
from flask_redis import FlaskRedis
from flask_restful import Api, abort

from limonero import CustomJSONEncoder as LimoneroJSONEncoder
from limonero.admin import DataSourceModelView, StorageModelView, HomeView, \
    init_login, AuthenticatedMenuLink
from limonero.cache import cache
from limonero.data_source_api import DataSourceDetailApi, DataSourceListApi, \
    DataSourcePermissionApi, DataSourceUploadApi, DataSourceInferSchemaApi, \
    DataSourcePrivacyApi, DataSourceDownload, DataSourceSampleApi, \
    DataSourceInitializationApi
from limonero.model_api import ModelDetailApi, ModelListApi
from limonero.models import db, DataSource, Storage
from limonero.privacy_api import GlobalPrivacyListApi, \
    AttributePrivacyGroupListApi
from limonero.py4j_init import init_jvm
from limonero.storage_api import StorageDetailApi, StorageListApi

os.chdir(os.environ.get('LIMONERO_HOME', '.'))
sqlalchemy_utils.i18n.get_locale = get_locale

app = Flask(__name__, static_url_path='', static_folder='static')

app.config['BABEL_TRANSLATION_DIRECTORIES'] = os.path.abspath(
    'limonero/i18n/locales')
app.json_encoder = LimoneroJSONEncoder

babel = Babel(app)

logging.config.fileConfig('logging_config.ini')

app.secret_key = 'l3m0n4d1'

# Flask Admin
admin = Admin(app, name='Lemonade Limonero', template_mode='bootstrap3',
              url="/control-panel", base_template='admin/master.html',
              index_view=HomeView(url='/control-panel'))

admin.add_link(AuthenticatedMenuLink(name='Logout',
                                     endpoint='admin.logout_view'))

# Cache
cache.init_app(app)

# CORS
CORS(app, resources={r"/*": {"origins": "*"}})
api = Api(app)

redis_store = FlaskRedis()

# Initialize flask-login
init_login(app)

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
    '/datasources/<int:data_source_id>/privacy': DataSourcePrivacyApi,
    '/privacy': GlobalPrivacyListApi,
    '/privacy/attribute-groups': AttributePrivacyGroupListApi,
    '/models': ModelListApi,
    '/models/<int:model_id>': ModelDetailApi,

    '/storages': StorageListApi,
    '/storages/<int:storage_id>': StorageDetailApi,
}
grouped_mappings = itertools.groupby(sorted(mappings.items()),
                                     key=lambda path: path[1])
for view, g in grouped_mappings:
    api.add_resource(view, *[x[0] for x in g], endpoint=view.__name__)

app.add_url_rule('/datasources/<int:data_source_id>/download',
                 methods=['GET'], endpoint='DataSourceDownload',
                 view_func=DataSourceDownload.as_view('download'))


# for route in app.url_map.iter_rules():
#    print route


@app.route('/static/<path:path>')
def static_file(path):
    return app.send_static_file(path)


@babel.localeselector
def get_locale():
    user = getattr(flask_g, 'user', None)
    if user is not None and user.locale:
        return user.locale
    else:
        return request.args.get(
            'lang', request.accept_languages.best_match(['en', 'pt', 'es']))


# noinspection PyUnusedLocal
def exit_gracefully(s, frame):
    os.kill(os.getpid(), signal.SIGTERM)


def main(is_main_module):
    config_file = None
    signal.signal(signal.SIGINT, exit_gracefully)
    if is_main_module:
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
        app.config['SQLALCHEMY_POOL_SIZE'] = 10
        app.config['SQLALCHEMY_POOL_RECYCLE'] = 240
        app.config['SQLALCHEMY_POOL_RECYCLE'] = 240

        app.config.update(config.get('config', {}))

        db.init_app(app)

        port = int(config.get('port', 5000))
        logger.debug(
            gettext('Running in %(mode)s mode', mode=config.get('environment')))

        if is_main_module:
            # JVM, used to interact with HDFS.
            init_jvm(app, logger)
            if config.get('environment', 'dev') == 'dev':
                admin.add_view(DataSourceModelView(DataSource, db.session))
                admin.add_view(StorageModelView(Storage, db.session))
                app.run(debug=True, port=port, host='0.0.0.0')
            else:
                eventlet.wsgi.server(eventlet.listen(('', port)), app)
    else:
        logger.error(
            gettext('Please, set LIMONERO_CONFIG environment variable'))
        exit(1)


if __name__ == '__main__':
    main(__name__ == '__main__')
