"""Data Source Validation

Revision ID: 7574fea33cea
Revises: c6f3cb7e343c
Create Date: 2023-09-06 15:23:28.701436

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.sql import text
from limonero.migration_utils import (downgrade_actions, upgrade_actions,
        is_mysql, is_psql, is_sqlite, get_psql_enum_alter_commands)

# revision identifiers, used by Alembic.
revision = '7574fea33cea'
down_revision = 'c6f3cb7e343c'
branch_labels = None
depends_on = None


def upgrade():
    if is_sqlite():
        with op.batch_alter_table('data_source_validation') as batch_op:
            batch_op.add_column(sa.Column('id', sa.Integer(), nullable=False)),
            batch_op.add_column(sa.Column('description', sa.String(length=200), nullable=True)),
            batch_op.add_column(sa.Column('type', sa.Enum('GREAT_EXPECTATIONS', 'SCRIPT', name='DataSourceValidationTypeEnumType'), nullable=False)),
            batch_op.add_column(sa.Column('enabled', sa.Boolean(), nullable=False)),
            batch_op.add_column(sa.Column('user_id', sa.Integer(), nullable=False)),
            batch_op.add_column(sa.Column('user_name', sa.String(length=200), nullable=False)),
            batch_op.add_column(sa.Column('user_login', sa.String(length=50), nullable=False)),
            batch_op.add_column(sa.Column('data_source_id', sa.Integer(), nullable=False)),
            batch_op.add_column(sa.ForeignKeyConstraint(['data_source_id'], ['data_source.id'], name='fk_data_source_validation_data_source_id')),
            batch_op.add_column(sa.PrimaryKeyConstraint('id'))

        with op.batch_alter_table('data_source_validation_execution') as batch_op:
            batch_op.add_column(sa.Column('id', sa.Integer(), nullable=False)),
            batch_op.add_column(sa.Column('created', sa.DateTime(), nullable=False)),
            batch_op.add_column(sa.Column('finished', sa.DateTime(), nullable=False)),
            batch_op.add_column(sa.Column('status', sa.Enum('SUCCESS', 'ERROR', 'RUNNING', 'PENDING', name='DataSourceValidationExecutionStatusEnumType'), nullable=True)),
            batch_op.add_column(sa.Column('user_id', sa.Integer(), nullable=False)),
            batch_op.add_column(sa.Column('user_name', sa.String(length=200), nullable=False)),
            batch_op.add_column(sa.Column('user_login', sa.String(length=50), nullable=False)),
            batch_op.add_column(sa.Column('result', mysql.LONGTEXT(), nullable=True)),
            batch_op.add_column(sa.Column('data_source_validation_id', sa.Integer(), nullable=False)),
            batch_op.add_column(sa.ForeignKeyConstraint(['data_source_validation_id'], ['data_source_validation.id'], name='fk_data_source_validation_execution_data_source_validation_id')),
            batch_op.add_column(sa.PrimaryKeyConstraint('id'))
        
        with op.batch_alter_table('data_source_validation_item') as batch_op:
            batch_op.add_column(sa.Column('id', sa.Integer(), nullable=False)),
            batch_op.add_column(sa.Column('description', sa.String(length=200), nullable=True)),
            batch_op.add_column(sa.Column('type', sa.String(length=200), nullable=False)),
            batch_op.add_column(sa.Column('enabled', sa.Boolean(), nullable=False)),
            batch_op.add_column(sa.Column('parameters', mysql.LONGTEXT(), nullable=True)),
            batch_op.add_column(sa.Column('data_source_validation_id', sa.Integer(), nullable=False)),
            batch_op.add_column(sa.ForeignKeyConstraint(['data_source_validation_id'], ['data_source_validation.id'], name='fk_data_source_validation_item_data_source_validation_id')),
            batch_op.add_column(sa.PrimaryKeyConstraint('id'))

        # i still need to do the following as it was did on the else case
        # (https://alembic.sqlalchemy.org/en/latest/batch.html#sqlite-batch-constraints)
        # op.create_index(op.f('ix_data_source_validation_data_source_id'), 'data_source_validation', ['data_source_id'], unique=False)
        # op.create_index(op.f('ix_data_source_validation_execution_data_source_validation_id'), 'data_source_validation_execution', ['data_source_validation_id'], unique=False)
        # op.create_index(op.f('ix_data_source_validation_item_data_source_validation_id'), 'data_source_validation_item', ['data_source_validation_id'], unique=False)
        # op.create_index(op.f('ix_attribute_data_source_id'), 'attribute', ['data_source_id'], unique=False)
        # op.create_index(op.f('ix_attribute_foreign_key_foreign_key_id'), 'attribute_foreign_key', ['foreign_key_id'], unique=False)
        # op.create_index(op.f('ix_attribute_foreign_key_from_attribute_id'), 'attribute_foreign_key', ['from_attribute_id'], unique=False)
        # op.create_index(op.f('ix_attribute_foreign_key_to_attribute_id'), 'attribute_foreign_key', ['to_attribute_id'], unique=False)
        # op.create_index(op.f('ix_attribute_privacy_attribute_id'), 'attribute_privacy', ['attribute_id'], unique=False)
        # op.create_index(op.f('ix_attribute_privacy_attribute_privacy_group_id'), 'attribute_privacy', ['attribute_privacy_group_id'], unique=False)
        # op.drop_constraint('attribute_privacy_attribute_id_fk', 'attribute_privacy', type_='foreignkey')
        # op.create_index(op.f('ix_data_source_storage_id'), 'data_source', ['storage_id'], unique=False)
        # op.create_index(op.f('ix_data_source_foreign_key_from_source_id'), 'data_source_foreign_key', ['from_source_id'], unique=False)
        # op.create_index(op.f('ix_data_source_foreign_key_to_source_id'), 'data_source_foreign_key', ['to_source_id'], unique=False)
        # op.create_index(op.f('ix_data_source_permission_data_source_id'), 'data_source_permission', ['data_source_id'], unique=False)
        # op.create_index(op.f('ix_model_storage_id'), 'model', ['storage_id'], unique=False)
        # op.create_index(op.f('ix_model_permission_model_id'), 'model_permission', ['model_id'], unique=False)
        # op.create_index(op.f('ix_privacy_risk_data_source_id'), 'privacy_risk', ['data_source_id'], unique=False)
        # op.create_index(op.f('ix_storage_permission_storage_id'), 'storage_permission', ['storage_id'], unique=False)
    

    else:
        op.create_table('data_source_validation',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('description', sa.String(length=200), nullable=True),
        sa.Column('type', sa.Enum('GREAT_EXPECTATIONS', 'SCRIPT', name='DataSourceValidationTypeEnumType'), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('user_name', sa.String(length=200), nullable=False),
        sa.Column('user_login', sa.String(length=50), nullable=False),
        sa.Column('data_source_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['data_source_id'], ['data_source.id'], name='fk_data_source_validation_data_source_id'),
        sa.PrimaryKeyConstraint('id')
        )
        op.create_index(op.f('ix_data_source_validation_data_source_id'), 'data_source_validation', ['data_source_id'], unique=False)
        op.create_table('data_source_validation_execution',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('created', sa.DateTime(), nullable=False),
        sa.Column('finished', sa.DateTime(), nullable=False),
        sa.Column('status', sa.Enum('SUCCESS', 'ERROR', 'RUNNING', 'PENDING', name='DataSourceValidationExecutionStatusEnumType'), nullable=True),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('user_name', sa.String(length=200), nullable=False),
        sa.Column('user_login', sa.String(length=50), nullable=False),
        sa.Column('result', mysql.LONGTEXT(), nullable=True),
        sa.Column('data_source_validation_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['data_source_validation_id'], ['data_source_validation.id'], name='fk_data_source_validation_execution_data_source_validation_id'),
        sa.PrimaryKeyConstraint('id')
        )
        op.create_index(op.f('ix_data_source_validation_execution_data_source_validation_id'), 'data_source_validation_execution', ['data_source_validation_id'], unique=False)
        op.create_table('data_source_validation_item',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('description', sa.String(length=200), nullable=True),
        sa.Column('type', sa.String(length=200), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False),
        sa.Column('parameters', mysql.LONGTEXT(), nullable=True),
        sa.Column('data_source_validation_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['data_source_validation_id'], ['data_source_validation.id'], name='fk_data_source_validation_item_data_source_validation_id'),
        sa.PrimaryKeyConstraint('id')
        )
        op.create_index(op.f('ix_data_source_validation_item_data_source_validation_id'), 'data_source_validation_item', ['data_source_validation_id'], unique=False)
        op.create_index(op.f('ix_attribute_data_source_id'), 'attribute', ['data_source_id'], unique=False)
        op.create_index(op.f('ix_attribute_foreign_key_foreign_key_id'), 'attribute_foreign_key', ['foreign_key_id'], unique=False)
        op.create_index(op.f('ix_attribute_foreign_key_from_attribute_id'), 'attribute_foreign_key', ['from_attribute_id'], unique=False)
        op.create_index(op.f('ix_attribute_foreign_key_to_attribute_id'), 'attribute_foreign_key', ['to_attribute_id'], unique=False)
        op.create_index(op.f('ix_attribute_privacy_attribute_id'), 'attribute_privacy', ['attribute_id'], unique=False)
        op.create_index(op.f('ix_attribute_privacy_attribute_privacy_group_id'), 'attribute_privacy', ['attribute_privacy_group_id'], unique=False)
        op.drop_constraint('attribute_privacy_attribute_id_fk', 'attribute_privacy', type_='foreignkey')
        op.create_index(op.f('ix_data_source_storage_id'), 'data_source', ['storage_id'], unique=False)
        op.create_index(op.f('ix_data_source_foreign_key_from_source_id'), 'data_source_foreign_key', ['from_source_id'], unique=False)
        op.create_index(op.f('ix_data_source_foreign_key_to_source_id'), 'data_source_foreign_key', ['to_source_id'], unique=False)
        op.create_index(op.f('ix_data_source_permission_data_source_id'), 'data_source_permission', ['data_source_id'], unique=False)
        op.create_index(op.f('ix_model_storage_id'), 'model', ['storage_id'], unique=False)
        op.create_index(op.f('ix_model_permission_model_id'), 'model_permission', ['model_id'], unique=False)
        op.create_index(op.f('ix_privacy_risk_data_source_id'), 'privacy_risk', ['data_source_id'], unique=False)
        op.create_index(op.f('ix_storage_permission_storage_id'), 'storage_permission', ['storage_id'], unique=False)
    
    
def downgrade():
    if is_sqlite():
        with op.batch_alter_table('storage_permission') as batch_op:
            batch_op.drop_index(op.f('ix_storage_permission_storage_id'))
        with op.batch_alter_table('privacy_risk') as batch_op:
            batch_op.drop_index(op.f('ix_privacy_risk_data_source_id'))
        with op.batch_alter_table('model_permission') as batch_op:
            batch_op.drop_index(op.f('ix_model_permission_model_id'))
        with op.batch_alter_table('model') as batch_op:
            batch_op.drop_index(op.f('ix_model_storage_id'))
        with op.batch_alter_table('data_source_permission') as batch_op:
            batch_op.drop_index(op.f('ix_data_source_permission_data_source_id'))
        with op.batch_alter_table('data_source_foreign_key') as batch_op:
            batch_op.drop_index(op.f('ix_data_source_foreign_key_to_source_id'))
            batch_op.drop_index(op.f('ix_data_source_foreign_key_from_source_id'))
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.drop_index(op.f('ix_data_source_storage_id'))
        with op.batch_alter_table('attribute_privacy') as batch_op:
            batch_op.drop_index(op.f('ix_attribute_privacy_attribute_privacy_group_id'))
            batch_op.drop_index(op.f('ix_attribute_privacy_attribute_id'))
        with op.batch_alter_table('attribute_foreign_key') as batch_op:
            batch_op.drop_index(op.f('ix_attribute_foreign_key_to_attribute_id'))
            batch_op.drop_index(op.f('ix_attribute_foreign_key_from_attribute_id'))
            batch_op.drop_index(op.f('ix_attribute_foreign_key_foreign_key_id'))
        with op.batch_alter_table('data_source_validation_item') as batch_op:
            batch_op.drop_index(op.f('ix_data_source_validation_item_data_source_validation_id'))
        with op.batch_alter_table('data_source_validation_execution') as batch_op:
            batch_op.drop_index(op.f('ix_data_source_validation_execution_data_source_validation_id'))
        with op.batch_alter_table('data_source_validation') as batch_op:
            batch_op.drop_index(op.f('ix_data_source_validation_data_source_id'))
        
        # still need to do the following as was did on the else case
        # op.create_foreign_key('attribute_privacy_attribute_id_fk', 'attribute_privacy', 'attribute', ['attribute_id'], ['id'], ondelete='CASCADE')
        # op.drop_table('data_source_validation_item')
        # op.drop_table('data_source_validation_execution')
        # op.drop_table('data_source_validation')
        
    
    else:
        op.drop_index(op.f('ix_storage_permission_storage_id'), table_name='storage_permission')
        op.drop_index(op.f('ix_privacy_risk_data_source_id'), table_name='privacy_risk')
        op.drop_index(op.f('ix_model_permission_model_id'), table_name='model_permission')
        op.drop_index(op.f('ix_model_storage_id'), table_name='model')
        op.drop_index(op.f('ix_data_source_permission_data_source_id'), table_name='data_source_permission')
        op.drop_index(op.f('ix_data_source_foreign_key_to_source_id'), table_name='data_source_foreign_key')
        op.drop_index(op.f('ix_data_source_foreign_key_from_source_id'), table_name='data_source_foreign_key')
        op.drop_index(op.f('ix_data_source_storage_id'), table_name='data_source')
        op.create_foreign_key('attribute_privacy_attribute_id_fk', 'attribute_privacy', 'attribute', ['attribute_id'], ['id'], ondelete='CASCADE')
        op.drop_index(op.f('ix_attribute_privacy_attribute_privacy_group_id'), table_name='attribute_privacy')
        op.drop_index(op.f('ix_attribute_privacy_attribute_id'), table_name='attribute_privacy')
        op.drop_index(op.f('ix_attribute_foreign_key_to_attribute_id'), table_name='attribute_foreign_key')
        op.drop_index(op.f('ix_attribute_foreign_key_from_attribute_id'), table_name='attribute_foreign_key')
        op.drop_index(op.f('ix_attribute_foreign_key_foreign_key_id'), table_name='attribute_foreign_key')
        op.drop_index(op.f('ix_attribute_data_source_id'), table_name='attribute')
        op.drop_index(op.f('ix_data_source_validation_item_data_source_validation_id'), table_name='data_source_validation_item')
        op.drop_table('data_source_validation_item')
        op.drop_index(op.f('ix_data_source_validation_execution_data_source_validation_id'), table_name='data_source_validation_execution')
        op.drop_table('data_source_validation_execution')
        op.drop_index(op.f('ix_data_source_validation_data_source_id'), table_name='data_source_validation')
        op.drop_table('data_source_validation')
