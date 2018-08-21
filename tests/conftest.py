# from project.database import db as _db
import datetime
import itertools
import logging
import os
import sys

import pytest
from flask import Flask
from flask_babel import Babel
from flask_restful import Api

from limonero.app import db as the_db, mappings
from limonero.models import Storage, StorageType, DataSource, DataSourceFormat
from limonero.util import CustomJSONEncoder

sys.path.append(os.path.dirname(os.path.curdir))

TESTDB = 'test_project.db'
TESTDB_PATH = "{}/{}".format(os.path.dirname(__file__), TESTDB)
TEST_DATABASE_URI = 'sqlite:///' + TESTDB_PATH
TEST_TOKEN = 'T0K3N_T35T'


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def client(app, db):
    _client = app.test_client()
    yield _client


@pytest.fixture(scope='function')
def auth_token():
    yield TEST_TOKEN


@pytest.yield_fixture(scope='function')
def app(request):
    """Session-wide test `Flask` application."""

    the_app = Flask(__name__, static_url_path='', static_folder='static')
    the_app.config['BABEL_TRANSLATION_DIRECTORIES'] = os.path.abspath(
        'limonero/i18n/locales')

    Babel(the_app)

    api = Api(the_app)
    grouped_mappings = itertools.groupby(sorted(mappings.items()),
                                         key=lambda path: path[1])
    for view, g in grouped_mappings:
        v = list(g)
        api.add_resource(view, *[x[0] for x in v])

    the_app.debug = True
    the_app.config['TESTING'] = True
    the_app.config['RESTFUL_JSON'] = {'cls': CustomJSONEncoder}
    the_app.config['LIMONERO_CONFIG'] = {'secret': TEST_TOKEN}
    # db_fd, app.config['DATABASE'] = tempfile.mkstemp()
    the_app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///:memory:"
    the_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    sqla_logger = logging.getLogger('sqlalchemy.engine.base.Engine')
    sqla_logger.setLevel(logging.ERROR)
    sqla_logger.propagate = False

    the_db.init_app(the_app)

    yield the_app


# noinspection PyShadowingNames
@pytest.yield_fixture(scope='function')
def db(app, request):
    the_db.app = app
    the_db.create_all()
    yield the_db
    the_db.drop_all()


# noinspection PyShadowingNames
@pytest.yield_fixture(scope='function')
def db1(app, request):
    with app.app_context():
        the_db.init_app(app)
        the_db.create_all()
    yield the_db
    with app.app_context():
        the_db.drop_all()


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def session(db, request):
    """Creates a new database session for a test."""
    connection = db.engine.connect()
    transaction = connection.begin()

    result_session = db.create_scoped_session(
        options=dict(bind=connection, binds={}))

    db.session = result_session
    yield result_session

    transaction.rollback()
    connection.close()
    result_session.remove()


# Storage

# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def default_storage(app, db):
    with app.app_context():
        storage = Storage(name='Default', type=StorageType.HDFS,
                          enabled=True, url='hdfs://test.com')
        db.session.add(storage)
        db.session.commit()
        yield storage


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def storages(app, db):
    names = ['Storage HDFS', 'Storage File', 'Storage MySQL']
    types = ['HDFS', 'HDFS', 'JDBC']
    statuses = [True, True, False]
    urls = ['hdfs://ok', 'file:///var/tmp/ok', 'mysql://server:3306/db/tb']

    storages = []
    with app.app_context():
        for name, ttype, enabled, url in zip(names, types, statuses, urls):
            storage = Storage(name=name, type=ttype, enabled=enabled, url=url)
            db.session.add(storage)
            storages.append(storage)
        db.session.commit()
        yield storages


# Data sources

# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def default_ds(app, db, default_storage):
    with app.app_context():
        data_source = DataSource(name='Default',
                                 description='Default data source',
                                 enabled=True,
                                 statistics_process_counter=0,
                                 read_only=False,
                                 privacy_aware=False,
                                 url='hdfs://test:9000/db/test',
                                 created=datetime.datetime.utcnow(),
                                 updated=datetime.datetime.utcnow(),
                                 format=DataSourceFormat.CSV,
                                 provenience=None,
                                 estimated_rows=0,
                                 estimated_size_in_mega_bytes=0,
                                 expiration=None,
                                 user_id=1,
                                 user_login='lemonade',
                                 user_name='Lemonade project',
                                 tags=None,
                                 temporary=False,
                                 workflow_id=None,
                                 task_id=None,
                                 attribute_delimiter=',',
                                 text_delimiter='"',
                                 is_public=True,
                                 treat_as_missing='NA',
                                 encoding='UTF8',
                                 is_first_line_header=True,
                                 command='',
                                 is_multiline=False,
                                 storage=default_storage
                                 )
        db.session.add(data_source)
        db.session.commit()
        yield data_source


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def datasources(app, db, default_storage):
    names = ['Storage HDFS', 'Storage File', 'Storage MySQL']
    types = ['HDFS', 'HDFS', 'JDBC']
    statuses = [True, True, False]
    urls = ['hdfs://ok', 'file:///var/tmp/ok', 'mysql://server:3306/db/tb']

    data_sources = []
    with app.app_context():
        for name, ttype, enabled, url in zip(names, types, statuses, urls):
            data_source = DataSource(name='Default',
                                     description='Default data source',
                                     enabled=True,
                                     statistics_process_counter=0,
                                     read_only=False,
                                     privacy_aware=False,
                                     url='hdfs://test:9000/db/test',
                                     created=datetime.datetime.utcnow(),
                                     updated=datetime.datetime.utcnow(),
                                     format=DataSourceFormat.CSV,
                                     provenience=None,
                                     estimated_rows=0,
                                     estimated_size_in_mega_bytes=0,
                                     expiration=None,
                                     user_id=1,
                                     user_login='lemonade',
                                     user_name='Lemonade project',
                                     tags=None,
                                     temporary=False,
                                     workflow_id=None,
                                     task_id=None,
                                     attribute_delimiter=',',
                                     text_delimiter='"',
                                     is_public=True,
                                     treat_as_missing='NA',
                                     encoding='UTF8',
                                     is_first_line_header=True,
                                     command='',
                                     is_multiline=False,
                                     storage=default_storage
                                     )
            db.session.add(data_source)
            data_sources.append(data_source)
        db.session.commit()
        yield data_sources
