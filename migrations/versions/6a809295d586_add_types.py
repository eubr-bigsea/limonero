"""add_types

Revision ID: 6a809295d586
Revises: 0eadac2ac523
Create Date: 2018-05-04 19:37:29.033365

"""

from alembic import context
from sqlalchemy.orm import sessionmaker

# revision identifiers, used by Alembic.
revision = '6a809295d586'
down_revision = '0eadac2ac523'
branch_labels = None
depends_on = None

all_commands = [
    ['''
        ALTER TABLE `limonero`.`data_source` CHANGE `format` `format`
        ENUM('XML_FILE','NETCDF4','HDF5','SHAPEFILE','TEXT','CUSTOM','JSON',
        'CSV','PICKLE','GEO_JSON') CHARSET utf8 COLLATE utf8_unicode_ci
        NOT NULL; ''',
     '''
         ALTER TABLE `limonero`.`data_source` CHANGE `format` `format`
        ENUM('XML_FILE','NETCDF4','HDF5','SHAPEFILE','TEXT','CUSTOM','JSON',
        'CSV','PICKLE','GEO_JSON') CHARSET utf8 COLLATE utf8_unicode_ci
        NOT NULL;
     ''']]


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
