# from project.database import db as _db
import datetime
import itertools
import logging
import os
import shutil
import sys
import tempfile

import pytest
from flask import Flask
from flask_babel import Babel
from flask_restful import Api

from limonero.app import db as the_db, mappings
from limonero.data_source_api import DataSourceDownload
from limonero.models import Storage, StorageType, DataSource, DataSourceFormat, \
    DataType
from limonero.py4j_init import init_jvm, create_gateway
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
        api.add_resource(view, *[x[0] for x in g], endpoint=view.__name__)

    the_app.add_url_rule('/datasources/<int:data_source_id>/download',
                         methods=['GET'], endpoint='DataSourceDownload',
                         view_func=DataSourceDownload.as_view('download'))

    the_app.debug = True
    the_app.config['TESTING'] = True
    the_app.config['RESTFUL_JSON'] = {'cls': CustomJSONEncoder}
    the_app.config['LIMONERO_CONFIG'] = {'secret': TEST_TOKEN}
    # db_fd, app.config['DATABASE'] = tempfile.mkstemp()
    the_app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///:memory:"
    the_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    the_app.config['SERVER_NAME'] = 'test-limonero'

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
def logger():
    yield logging.getLogger()


# noinspection PyShadowingNames
@pytest.yield_fixture(scope='function')
def jvm(app, logger):
    init_jvm(app, logger)
    gateway = create_gateway(logger, app.gateway_port)
    yield gateway.jvm


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
def file_storage(app, db):
    with app.app_context():
        storage = Storage(name='File Storage', type=StorageType.HDFS,
                          enabled=True, url='file:///tmp/')
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
def file_ds(app, db, default_storage):
    fd, path = tempfile.mkstemp()
    data = "id,name\n1,Alice\n2,John"
    os.write(fd, data)
    with app.app_context():
        data_source = DataSource(name='File Data Source',
                                 description=data,
                                 enabled=True,
                                 statistics_process_counter=0,
                                 read_only=False,
                                 privacy_aware=False,
                                 url='file://{}'.format(path),
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
    os.unlink(path)


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def infer_ds(app, db, default_storage):
    data_sources = []
    content = [
        (
            "id,name,date,salary\n"
            "1,Bob,2018-01-01,12344.43\n"
            "2,Alice,2011-04-12,21312.99\n"
            "2,Sarah,2016-05-21,11312.00",
            [
                ('id', DataType.INTEGER),
                ('name', DataType.CHARACTER),
                ('data', DataType.DATETIME),
                ('salary', DataType.DECIMAL)
            ]
        )
    ]
    with app.app_context():
        for c, meta in content:
            fd, path = tempfile.mkstemp()
            os.write(fd, c)
            data_source = DataSource(name=path,
                                     description='',
                                     enabled=True,
                                     statistics_process_counter=0,
                                     read_only=False,
                                     privacy_aware=False,
                                     url='file:///{}'.format(path),
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
                                     temporary=False,
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
            data_sources.append([data_source, meta])
        db.session.commit()
        yield data_sources
        for ds, meta in data_sources:
            os.unlink(ds.name)


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


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def hdfs(jvm):
    # noinspection PyPep8Naming
    MiniDFSCluster = jvm.org.apache.hadoop.hdfs.MiniDFSCluster

    base_dir = tempfile.mkdtemp()

    conf = jvm.org.apache.hadoop.conf.Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, base_dir)
    cluster = MiniDFSCluster.Builder(conf).build()
    uri = "hdfs://localhost:{}/".format(cluster.getNameNodePort())
    fs = cluster.getFileSystem()
    yield (cluster, uri, fs)
    cluster.shutdown()
    shutil.rmtree(base_dir)
