"""Update data source

Revision ID: 6c4c02043e82
Revises: 55f14e046318
Create Date: 2018-09-05 16:08:47.192237

"""

from alembic import context
from sqlalchemy.orm import sessionmaker

# revision identifiers, used by Alembic.
revision = '6c4c02043e82'
down_revision = '55f14e046318'
branch_labels = None
depends_on = None
all_commands = [
    ['''
        ALTER TABLE `limonero`.`data_source` CHANGE `format` `format`
        ENUM('CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
        'PARQUET','PICKLE','SHAPEFILE','TEXT','UNKNOWN', 'XML_FILE')
        CHARSET utf8 COLLATE utf8_unicode_ci
        NOT NULL; ''',
     '''
         ALTER TABLE `limonero`.`data_source` CHANGE `format` `format`
         ENUM('CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
        'PICKLE','SHAPEFILE','TEXT','XML_FILE')
        CHARSET utf8 COLLATE utf8_unicode_ci
        NOT NULL;
     '''],
    ["""
        ALTER TABLE `limonero`.`data_source` CHANGE `task_id` `task_id`
        VARCHAR(255) NULL; """,
     """ALTER TABLE `limonero`.`data_source` CHANGE `task_id` `task_id`
     INT NULL; """,
     ],
    ["""
    ALTER TABLE `limonero`.`attribute` CHANGE `type` `type`
        ENUM('BINARY','CHARACTER','DECIMAL','DATE','DATETIME','DOUBLE','ENUM',
        'FLOAT','INTEGER','LAT_LONG','LONG','TEXT','TIME','VECTOR','TIMESTAMP')
        CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;
    """,
     """
     ALTER TABLE `limonero`.`attribute` CHANGE `type` `type`
        ENUM('BINARY','CHARACTER','DECIMAL','DATE','DATETIME','DOUBLE','ENUM',
        'FLOAT','INTEGER','LAT_LONG','LONG','TEXT','TIME','VECTOR','TIMESTAMP')
        CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL; """]
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
