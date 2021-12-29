"""deployment_info

Revision ID: c74c284e47e6
Revises: 8a480de4de4c
Create Date: 2021-12-13 20:30:11.416326

"""
from alembic import op
import sqlalchemy as sa
from limonero.migration_utils import (get_enable_disable_fk_command,
        upgrade_actions, downgrade_actions, get_psql_enum_alter_commands,
        is_mysql, is_psql)

# revision identifiers, used by Alembic.
revision = 'c74c284e47e6'
down_revision = '8a480de4de4c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('model', sa.Column('deployment_status', sa.Enum('NOT_DEPLOYED', 'ERROR', 'EDITING', 'SAVED', 'RUNNING', 'STOPPED', 'SUSPENDED', 'PENDING', 'DEPLOYED', name='DeploymentStatusEnumType'), nullable=False))
    upgrade_actions(get_commands())


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('model', 'deployment_status')
    downgrade_actions(get_commands())   

def get_commands():
    all_commands = []
    if is_mysql():
        all_commands = [
            (
                """ALTER TABLE `model` CHANGE `type` `type` ENUM
                ('KERAS','MLEAP', 'PERFORMANCE_SPARK','PERFORMANCE_KERAS','PERFORMANCE',
                  'SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION','SPARK_ML_CLASSIFICATION','UNSPECIFIED')
                    CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL; """,
                """ALTER TABLE `model` CHANGE `type` `type` ENUM
                ('KERAS','PERFORMANCE_SPARK','PERFORMANCE_KERAS',
                'PERFORMANCE','SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION',
                'SPARK_ML_CLASSIFICATION','UNSPECIFIED')
                    CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL; """
            )
        ]
    elif is_psql():
        new_model_values = ['KERAS','MLEAP', 'PERFORMANCE_SPARK','PERFORMANCE_KERAS',
          'PERFORMANCE','SPARK_ML_REGRESSION',
            'SPARK_MLLIB_CLASSIFICATION','SPARK_ML_CLASSIFICATION','UNSPECIFIED']
        old_model_values = ['KERAS','PERFORMANCE_SPARK','PERFORMANCE_KERAS','PERFORMANCE',
           'SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION',
           'SPARK_ML_CLASSIFICATION','UNSPECIFIED']

        all_commands = [
             [
                get_psql_enum_alter_commands(['model'], ['type'], 
                    'ModelTypeEnumType', new_model_values, 'UNSPECIFIED'),
                get_psql_enum_alter_commands(['model'], ['type'], 
                    'ModelTypeEnumType', old_model_values, 'UNSPECIFIED'),
             ],
        ]

    return all_commands

