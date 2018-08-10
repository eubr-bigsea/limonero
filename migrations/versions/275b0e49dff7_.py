"""empty messagepark-2.3.0/

Revision ID: 275b0e49dff7
Revises: 66d4be40bced
Create Date: 2018-07-11 16:15:33.196417

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision = '275b0e49dff7'
down_revision = '66d4be40bced'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('data_source',
                   sa.Column('command', mysql.LONGTEXT(), nullable=True))
    op.get_bind().execute(text(
        "ALTER TABLE `limonero`.`data_source` CHANGE `format` `format` "
        "ENUM('XML_FILE','NETCDF4','HDF5','SHAPEFILE','TEXT','CUSTOM','JSON',"
        "'CSV','PICKLE','GEO_JSON','JDBC') CHARSET utf8 "
        "COLLATE utf8_unicode_ci NOT NULL;"
    ))
    op.get_bind().execute(text("""
        ALTER TABLE storage CHANGE `type` `type` 
        ENUM('HDFS', 'OPHIDIA','ELASTIC_SEARCH','MONGODB',
             'POSTGIS','HBASE','CASSANDRA','JDBC') CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;"""
            ))
    op.add_column('storage', sa.Column('enabled', sa.Boolean(), nullable=False, 
         server_default=sa.schema.DefaultClause("1"), default=1))
    op.add_column('data_source', sa.Column('updated', sa.DateTime(), nullable=False, 
            server_default='2018-01-01')) 



def downgrade():
    op.drop_column('data_source', 'command')
    try:
        op.get_bind().execute(text(
            "ALTER TABLE `limonero`.`data_source` CHANGE `format` `format` "
            "ENUM('XML_FILE','NETCDF4','HDF5','SHAPEFILE','TEXT','CUSTOM','JSON',"
            "'CSV','PICKLE','GEO_JSON') CHARSET utf8 "
            "COLLATE utf8_unicode_ci NOT NULL;"
        ))
        op.get_bind().execute(text("""
            ALTER TABLE storage CHANGE `type` `type` 
            ENUM('HDFS', 'OPHIDIA','ELASTIC_SEARCH','MONGODB',
                 'POSTGIS','HBASE','CASSANDRA') CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;"""
                ))
    except:
        pass
    op.drop_column('storage', 'enabled')
    op.drop_column('data_source', 'updated')
