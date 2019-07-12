"""Add new types

Revision ID: 32053847c4db
Revises: 05a62958a9cc
Create Date: 2019-06-11 10:36:14.456629

"""

from alembic import context
from sqlalchemy.orm import sessionmaker

# revision identifiers, used by Alembic.
revision = '32053847c4db'
down_revision = '05a62958a9cc'
branch_labels = None
depends_on = None

all_commands = [
    (""" ALTER TABLE data_source CHANGE `format` `format` ENUM(
        'CSV','CUSTOM','GEO_JSON','HAR_IMAGE_FOLDER','HDF5','DATA_FOLDER',
        'IMAGE_FOLDER', 'JDBC','JSON','NETCDF4','PARQUET','PICKLE','SHAPEFILE',
        'TAR_IMAGE_FOLDER','TEXT', 'VIDEO_FOLDER',
        'UNKNOWN','XML_FILE') CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;""",
     """ ALTER TABLE data_source CHANGE `format` `format` ENUM(
        'CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON',
        'NETCDF4','PARQUET','PICKLE','SHAPEFILE','TEXT',
        'UNKNOWN','XML_FILE') CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;"""
     ),
    ("""
        ALTER TABLE `storage` CHANGE `type` `type` ENUM(
            'HDFS','OPHIDIA','ELASTIC_SEARCH','MONGODB','POSTGIS','HBASE',
            'CASSANDRA','JDBC','LOCAL') CHARSET utf8 COLLATE
            utf8_unicode_ci NOT NULL;""",
     """
     ALTER TABLE `storage` CHANGE `type` `type` ENUM(
        'HDFS','OPHIDIA','ELASTIC_SEARCH','MONGODB','POSTGIS','HBASE',
        'CASSANDRA','JDBC') CHARSET utf8 COLLATE
        utf8_unicode_ci NOT NULL;""",
     ),
    (
        """ALTER TABLE `model` CHANGE `type` `type` ENUM(
            'KERAS','SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION',
            'SPARK_ML_CLASSIFICATION','UNSPECIFIED')
            CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL; """,
        """ALTER TABLE `model` CHANGE `type` `type` ENUM(
            'KERAS','SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION',
            'SPARK_ML_CLASSIFICATION','UNSPECIFIED')
            CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL; """
    )
]


def upgrade():
    ctx = context.get_context()
    session = sessionmaker(bind=ctx.bind)()
    connection = session.connection()

    try:
        for cmd in all_commands:
            if isinstance(cmd[0], (unicode, str)):
                connection.execute(cmd[0])
            elif isinstance(cmd[0], list):
                for row in cmd[0]:
                    connection.execute(row)
            else:
                cmd[0]()
    except:
        session.rollback()
        raise
    session.commit()


def downgrade():
    ctx = context.get_context()
    session = sessionmaker(bind=ctx.bind)()
    connection = session.connection()
    connection.execute('SET foreign_key_checks = 0;')

    try:
        for cmd in reversed(all_commands):
            if isinstance(cmd[1], (unicode, str)):
                connection.execute(cmd[1])
            elif isinstance(cmd[1], list):
                for row in cmd[1]:
                    connection.execute(row)
            else:
                cmd[1]()
    except:
        session.rollback()
        raise
    connection.execute('SET foreign_key_checks = 1;')
    session.commit()
