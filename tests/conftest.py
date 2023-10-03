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
import flask_migrate

from limonero.app import create_app, db
from limonero.data_source_api import DataSourceDownload
from limonero.models import (
    Storage,
    StorageType,
    DataSource,
    DataSourceFormat,
    DataType,
    Model,
    ModelType,
    DeploymentStatus,
)
from limonero.py4j_init import init_jvm, create_gateway
from limonero.util import CustomJSONEncoder

sys.path.append(os.path.dirname(os.path.curdir))

TESTDB = "test_project.db"
TESTDB_PATH = "{}/{}".format(os.path.dirname(__file__), TESTDB)
TEST_DATABASE_URI = "sqlite:///" + TESTDB_PATH
TEST_TOKEN = "T0K3N_T35T"


def _get_models():
    return [
        Model(
            id=1,
            name="Model #1",
            enabled=True,
            created=datetime.datetime.now(),
            path="/tmp/model/model1.zip",
            class_name="LinearRegressionModel",
            type=ModelType.SPARK_ML_CLASSIFICATION,
            deployment_status=DeploymentStatus.NOT_DEPLOYED,
            user_id=1,
            user_login="admin@lemonade.org.br",
            user_name="Admin",
            workflow_id=0,
            workflow_name=None,
            task_id="",
            job_id=0,
            storage_id=1,
        ),
        Model(
            id=2,
            name="Model #2",
            enabled=False,
            created=datetime.datetime.now(),
            path="/tmp/model/model1.zip",
            class_name="DecisionTreeRegressionModel",
            type=ModelType.SPARK_ML_REGRESSION,
            deployment_status=DeploymentStatus.NOT_DEPLOYED,
            user_id=1,
            user_login="admin@lemonade.org.br",
            user_name="Admin",
            workflow_id=0,
            workflow_name=None,
            task_id="",
            job_id=0,
            storage_id=1,
        ),
    ]


def _get_storages():
    return [
        Storage(
            name="Default", type=StorageType.HDFS, enabled=True, url="hdfs://test.com"
        ),
        Storage(
            name="File Storage", type=StorageType.HDFS, enabled=True, url="file:///tmp/"
        ),
        Storage(
            name="HDFS Storage",
            type=StorageType.HDFS,
            enabled=True,
            url="hdfs:///demo/",
        ),
        Storage(
            name="MySQL Storage",
            type=StorageType.JDBC,
            enabled=True,
            url="mysql://server:3306/db/tb",
        ),
        Storage(
            name="MySQL Storage Disabled",
            type=StorageType.JDBC,
            enabled=False,
            url="mysql://server:3306/db2/tb",
        ),
    ]


def _get_visualizations():
    return [
        DataSource(
            name="Default",
            description="Default data source",
            enabled=True,
            statistics_process_counter=0,
            read_only=False,
            privacy_aware=False,
            url="hdfs://test:9000/db/test",
            created=datetime.datetime.utcnow(),
            updated=datetime.datetime.utcnow(),
            format=DataSourceFormat.CSV,
            provenience=None,
            estimated_rows=0,
            estimated_size_in_mega_bytes=0,
            expiration=None,
            user_id=1,
            user_login="lemonade",
            user_name="Lemonade project",
            tags=None,
            temporary=False,
            workflow_id=None,
            task_id=None,
            attribute_delimiter=",",
            text_delimiter='"',
            is_public=True,
            treat_as_missing="NA",
            encoding="UTF8",
            is_first_line_header=True,
            command="",
            is_multiline=False,
            storage_id=1,
        ),
        DataSource(
            name="Optional",
            description="Optional data source",
            enabled=False,
            statistics_process_counter=0,
            read_only=False,
            privacy_aware=False,
            url="hdfs://test2:9000/db/test2",
            created=datetime.datetime.utcnow(),
            updated=datetime.datetime.utcnow(),
            format=DataSourceFormat.CSV,
            provenience=None,
            estimated_rows=0,
            estimated_size_in_mega_bytes=0,
            expiration=None,
            user_id=1,
            user_login="lemonade",
            user_name="Lemonade project",
            tags=None,
            temporary=False,
            workflow_id=None,
            task_id=None,
            attribute_delimiter=",",
            text_delimiter='"',
            is_public=True,
            treat_as_missing="NA",
            encoding="UTF8",
            is_first_line_header=True,
            command="",
            is_multiline=False,
            storage_id=1,
        ),
    ]


# noinspection PyShadowingNames


@pytest.fixture(scope="session")
def app():
    path = os.path.dirname(os.path.abspath(__name__))
    app = create_app()
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{path}/test.db"
    app.config["TESTING"] = True
    app.config["GATEWAY_PORT"] = 18001
    app.debug = False
    return app


# noinspection PyShadowingNames


@pytest.fixture(scope="session")
def client(app):
    path = os.path.dirname(os.path.abspath(__name__))
    # import pdb; pdb.set_trace()
    test_db_file = "test.db"
    with app.test_client() as client:
        with app.app_context():
            # flask_migrate.downgrade(revision="base")
            if os.path.exists(os.path.join(path, test_db_file)):
                os.remove(os.path.join(path, test_db_file))
            flask_migrate.upgrade(revision="head")
            for storage in _get_storages():
                db.session.add(storage)
            for visualization in _get_visualizations():
                db.session.add(visualization)
            for model in _get_models():
                db.session.add(model)
            db.session.commit()
        client.secret = app.config["LIMONERO_CONFIG"]["secret"]
        yield client
        os.unlink(test_db_file)


# @pytest.yield_fixture(scope='function')
# def logger():
#     yield logging.getLogger()


# noinspection PyShadowingNames
# @pytest.yield_fixture(scope='session')
# def jvm(app):
#     logger = logging.getLogger()
#     init_jvm(app, logger)
#     gateway = create_gateway(logger, app.config['GATEWAY_PORT'])
#     yield gateway.jvm


# # noinspection PyShadowingNames
# @pytest.fixture(scope='function')
# def session(db, request):
#     """Creates a new database session for a test."""
#     connection = db.engine.connect()
#     transaction = connection.begin()

#     result_session = db.create_scoped_session(
#         options=dict(bind=connection, binds={}))

#     db.session = result_session
#     yield result_session

#     transaction.rollback()
#     connection.close()
#     result_session.remove()


# Data sources

# # noinspection PyShadowingNames
# @pytest.fixture(scope='function')
# def infer_ds(app, db, default_storage):
#     data_sources = []
#     content = [
#         (
#             "id,name,date,salary\n"
#             "1,Bob,2018-01-01,12344.43\n"
#             "2,Alice,2011-04-12,21312.99\n"
#             "2,Sarah,2016-05-21,11312.00",
#             [
#                 ('id', DataType.INTEGER),
#                 ('name', DataType.CHARACTER),
#                 ('data', DataType.DATETIME),
#                 ('salary', DataType.DECIMAL)
#             ]
#         )
#     ]
#     with app.app_context():
#         for c, meta in content:
#             fd, path = tempfile.mkstemp()
#             os.write(fd, c)
#             data_source = DataSource(name=path,
#                                      description='',
#                                      enabled=True,
#                                      statistics_process_counter=0,
#                                      read_only=False,
#                                      privacy_aware=False,
#                                      url='file:///{}'.format(path),
#                                      created=datetime.datetime.utcnow(),
#                                      updated=datetime.datetime.utcnow(),
#                                      format=DataSourceFormat.CSV,
#                                      provenience=None,
#                                      estimated_rows=0,
#                                      estimated_size_in_mega_bytes=0,
#                                      expiration=None,
#                                      user_id=1,
#                                      user_login='lemonade',
#                                      user_name='Lemonade project',
#                                      temporary=False,
#                                      attribute_delimiter=',',
#                                      text_delimiter='"',
#                                      is_public=True,
#                                      treat_as_missing='NA',
#                                      encoding='UTF8',
#                                      is_first_line_header=True,
#                                      command='',
#                                      is_multiline=False,
#                                      storage=default_storage
#                                      )
#             db.session.add(data_source)
#             data_sources.append([data_source, meta])
#         db.session.commit()
#         yield data_sources
#         for ds, meta in data_sources:
#             os.unlink(ds.name)


# # noinspection PyShadowingNames
# @pytest.fixture(scope='function')
# def datasources(app, db, default_storage):
#     names = ['Storage HDFS', 'Storage File', 'Storage MySQL']
#     types = ['HDFS', 'HDFS', 'JDBC']
#     statuses = [True, True, False]
#     urls = ['hdfs://ok', 'file:///var/tmp/ok', 'mysql://server:3306/db/tb']

#     data_sources = []
#     with app.app_context():
#         for name, ttype, enabled, url in zip(names, types, statuses, urls):
#             data_source = DataSource(name='Default',
#                                      description='Default data source',
#                                      enabled=True,
#                                      statistics_process_counter=0,
#                                      read_only=False,
#                                      privacy_aware=False,
#                                      url='hdfs://test:9000/db/test',
#                                      created=datetime.datetime.utcnow(),
#                                      updated=datetime.datetime.utcnow(),
#                                      format=DataSourceFormat.CSV,
#                                      provenience=None,
#                                      estimated_rows=0,
#                                      estimated_size_in_mega_bytes=0,
#                                      expiration=None,
#                                      user_id=1,
#                                      user_login='lemonade',
#                                      user_name='Lemonade project',
#                                      tags=None,
#                                      temporary=False,
#                                      workflow_id=None,
#                                      task_id=None,
#                                      attribute_delimiter=',',
#                                      text_delimiter='"',
#                                      is_public=True,
#                                      treat_as_missing='NA',
#                                      encoding='UTF8',
#                                      is_first_line_header=True,
#                                      command='',
#                                      is_multiline=False,
#                                      storage=default_storage
#                                      )
#             db.session.add(data_source)
#             data_sources.append(data_source)
#         db.session.commit()
#         yield data_sources


# # noinspection PyShadowingNames
# @pytest.fixture(scope='function')
# def hdfs(jvm):
#     # noinspection PyPep8Naming
#     MiniDFSCluster = jvm.org.apache.hadoop.hdfs.MiniDFSCluster

#     base_dir = tempfile.mkdtemp()

#     conf = jvm.org.apache.hadoop.conf.Configuration()
#     conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, base_dir)
#     cluster = MiniDFSCluster.Builder(conf).build()
#     uri = "hdfs://localhost:{}/".format(cluster.getNameNodePort())
#     fs = cluster.getFileSystem()
#     yield (cluster, uri, fs)
#     cluster.shutdown()
#     shutil.rmtree(base_dir)
