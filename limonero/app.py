#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import itertools
import logging
import logging.config

import eventlet
import eventlet.wsgi
import os
import sqlalchemy_utils
import yaml
from flask import Flask, request
from flask_admin import Admin
from flask_babel import get_locale, Babel
from flask_cors import CORS
from flask_redis import FlaskRedis
from flask_restful import Api, abort

from data_source_api import DataSourceDetailApi, DataSourceListApi, \
    DataSourcePermissionApi, DataSourceUploadApi, DataSourceInferSchemaApi
from limonero.admin import DataSourceModelView, StorageModelView
from limonero.models import db, DataSource, Storage
from limonero.storage_api import StorageDetailApi, StorageListApi
from py4j_init import init_jvm

os.chdir(os.environ.get('LIMONERO_HOME', '.'))
sqlalchemy_utils.i18n.get_locale = get_locale

eventlet.monkey_patch(all=True)
app = Flask(__name__, static_url_path='')

babel = Babel(app)

logging.config.fileConfig('logging_config.ini')

app.secret_key = 'l3m0n4d1'
# Flask Admin 
admin = Admin(app, name='Lemonade', template_mode='bootstrap3')

# CORS
CORS(app, resources={r"/*": {"origins": "*"}})
api = Api(app)

redis_store = FlaskRedis()

mappings = {
    '/datasources': DataSourceListApi,
    '/datasources/upload': DataSourceUploadApi,
    '/datasources/infer-schema/<int:data_source_id>': DataSourceInferSchemaApi,
    '/datasources/<int:data_source_id>': DataSourceDetailApi,
    '/datasources/<int:data_source_id>/permission/<int:user_id>':
        DataSourcePermissionApi,
    '/storages': StorageListApi,
    '/storages/<int:storage_id>': StorageDetailApi,
}
grouped_mappings = itertools.groupby(sorted(mappings.items()),
                                     key=lambda path: path[1])
for view, g in grouped_mappings:
    v = list(g)
    api.add_resource(view, *[x[0] for x in v])


# for route in app.url_map.iter_rules():
#    print route

# @app.before_request
def before():
    if request.args and 'lang' in request.args:
        if request.args['lang'] not in ('es', 'en'):
            return abort(404)


@app.route('/static/<path:path>')
def static_file(path):
    return app.send_static_file(path)


@babel.localeselector
def get_locale():
    return request.args.get('lang', 'en')


def main(is_main_module):
    config_file = None
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
            config = yaml.load(f)['limonero']

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
        # redis_store.init_app(app)

        # with app.app_context():
        #    db.create_all()

        port = int(config.get('port', 5000))
        logger.debug('Running in %s mode', config.get('environment'))

        if is_main_module:
            # JVM, used to interact with HDFS.
            init_jvm(app)
            if config.get('environment', 'dev') == 'dev':
                admin.add_view(DataSourceModelView(DataSource, db.session))
                admin.add_view(StorageModelView(Storage, db.session))
                app.run(debug=True, port=port)
            else:
                eventlet.wsgi.server(eventlet.listen(('', port)), app)
    else:
        logger.error('Please, set LIMONERO_CONFIG environment variable')
        exit(1)


if __name__ == '__main__':
    main(__name__ == '__main__')
