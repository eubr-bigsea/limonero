"""Add new model types

Revision ID: 96581239a707
Revises: 32053847c4db
Create Date: 2019-08-30 08:51:32.168436

"""
from alembic import context
from sqlalchemy.orm import sessionmaker

# revision identifiers, used by Alembic.
revision = '96581239a707'
down_revision = '32053847c4db'
branch_labels = None
depends_on = None

all_commands = [
    ['''
        ALTER TABLE `model` CHANGE `type` `type`
        ENUM('KERAS','PERFORMANCE_SPARK',
        'PERFORMANCE_KERAS', 'PERFORMANCE',
        'SPARK_ML_REGRESSION',
        'SPARK_MLLIB_CLASSIFICATION',
        'SPARK_ML_CLASSIFICATION','UNSPECIFIED')
        CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;  ''',
     '''
        ALTER TABLE `model` CHANGE `type` `type`
        ENUM('KERAS', 'SPARK_ML_REGRESSION', 'PERFORMANCE', 
        'SPARK_MLLIB_CLASSIFICATION',
        'SPARK_ML_CLASSIFICATION','UNSPECIFIED', 'PERFORMANCE')
        CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;  '''],
    ['ALTER TABLE `model` ADD INDEX `inx_type` (`type`);',
     'ALTER TABLE `model` DROP INDEX `inx_type`;'],
    [
        """
        ALTER TABLE `model`
            CHANGE `job_id` `job_id` INT(11) NULL,
            CHANGE `task_id` `task_id` VARCHAR(200)
                CHARSET utf8 COLLATE utf8_unicode_ci NULL,
            CHANGE `workflow_id` `workflow_id` INT(11) NULL;
        """,
        """
        ALTER TABLE `model`
            CHANGE `job_id` `job_id` INT(11) NOT NULL,
            CHANGE `task_id` `task_id` VARCHAR(200)
                CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL,
            CHANGE `workflow_id` `workflow_id` INT(11) NOT NULL;"""
    ]
]


def upgrade():
    ctx = context.get_context()
    session = sessionmaker(bind=ctx.bind)()
    connection = session.connection()

    try:
        for cmd in all_commands:
            if isinstance(cmd[0], str):
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
            if isinstance(cmd[1], str):
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
