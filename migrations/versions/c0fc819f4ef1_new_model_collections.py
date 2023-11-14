"""new model collections

Revision ID: c0fc819f4ef1
Revises: c6f3cb7e343c
Create Date: 2023-11-14 09:35:39.088993

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.sql import text
from limonero.migration_utils import (downgrade_actions, upgrade_actions,
        is_mysql, is_psql, is_sqlite, get_psql_enum_alter_commands)

# revision identifiers, used by Alembic.
revision = 'c0fc819f4ef1'
down_revision = 'c6f3cb7e343c'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('feature',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=100), nullable=True),
    sa.Column('type', sa.Enum('CATEGORICAL', 'NUMERICAL', name='FeatureTypeEnumType'), nullable=True),
    sa.Column('algorithm', sa.String(length=100), nullable=True),
    sa.Column('missing_handling', sa.String(length=100), nullable=True),
    sa.Column('scaling', sa.String(length=100), nullable=True),
    sa.Column('categorical_handling', sa.String(length=100), nullable=True),
    sa.Column('numerical_handling', sa.String(length=100), nullable=True),
    sa.Column('usage', sa.Enum('LABEL', 'FEATURE', name='FeatureUsageEnumType'), nullable=True),
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['model_id'], ['model.id'], name='fk_feature_model_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_feature_model_id'), 'feature', ['model_id'], unique=False)
    op.create_table('hyperparameter',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=100), nullable=False),
    sa.Column('value', sa.Float(), nullable=False),
    sa.Column('description', sa.String(length=500), nullable=True),
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['model_id'], ['model.id'], name='fk_hyperparameter_model_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_hyperparameter_model_id'), 'hyperparameter', ['model_id'], unique=False)
    op.create_table('metric',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=100), nullable=False),
    sa.Column('value', sa.Float(), nullable=False),
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['model_id'], ['model.id'], name='fk_metric_model_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_metric_model_id'), 'metric', ['model_id'], unique=False)
    op.create_table('metric_result',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('type', sa.String(length=100), nullable=False),
    sa.Column('description', sa.String(length=500), nullable=True),
    # sa.Column('value', mysql.LONGTEXT(), nullable=False),
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['model_id'], ['model.id'], name='fk_metric_result_model_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_metric_result_model_id'), 'metric_result', ['model_id'], unique=False)
    op.create_index(op.f('ix_attribute_data_source_id'), 'attribute', ['data_source_id'], unique=False)
    op.create_index(op.f('ix_attribute_foreign_key_foreign_key_id'), 'attribute_foreign_key', ['foreign_key_id'], unique=False)
    op.create_index(op.f('ix_attribute_foreign_key_from_attribute_id'), 'attribute_foreign_key', ['from_attribute_id'], unique=False)
    op.create_index(op.f('ix_attribute_foreign_key_to_attribute_id'), 'attribute_foreign_key', ['to_attribute_id'], unique=False)
    op.create_index(op.f('ix_attribute_privacy_attribute_id'), 'attribute_privacy', ['attribute_id'], unique=False)
    op.create_index(op.f('ix_attribute_privacy_attribute_privacy_group_id'), 'attribute_privacy', ['attribute_privacy_group_id'], unique=False)
    # op.drop_constraint('attribute_privacy_attribute_id_fk', 'attribute_privacy', type_='foreignkey')
    op.create_index(op.f('ix_data_source_storage_id'), 'data_source', ['storage_id'], unique=False)
    op.create_index(op.f('ix_data_source_foreign_key_from_source_id'), 'data_source_foreign_key', ['from_source_id'], unique=False)
    op.create_index(op.f('ix_data_source_foreign_key_to_source_id'), 'data_source_foreign_key', ['to_source_id'], unique=False)
    op.create_index(op.f('ix_data_source_permission_data_source_id'), 'data_source_permission', ['data_source_id'], unique=False)
    op.create_index(op.f('ix_model_storage_id'), 'model', ['storage_id'], unique=False)
    op.create_index(op.f('ix_model_permission_model_id'), 'model_permission', ['model_id'], unique=False)
    op.create_index(op.f('ix_privacy_risk_data_source_id'), 'privacy_risk', ['data_source_id'], unique=False)
    op.create_index(op.f('ix_storage_permission_storage_id'), 'storage_permission', ['storage_id'], unique=False)
    
    if is_sqlite():
        with op.batch_alter_table('metric_result') as batch_op:
            batch_op.add_column(sa.Column('value', sa.Text(), nullable=False))
    
    else:
        op.add_column('metric_result',
            sa.Column('value', mysql.LONGTEXT(), nullable=False))


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
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
    op.drop_index(op.f('ix_metric_result_model_id'), table_name='metric_result')
    op.drop_table('metric_result')
    op.drop_index(op.f('ix_metric_model_id'), table_name='metric')
    op.drop_table('metric')
    op.drop_index(op.f('ix_hyperparameter_model_id'), table_name='hyperparameter')
    op.drop_table('hyperparameter')
    op.drop_index(op.f('ix_feature_model_id'), table_name='feature')
    op.drop_table('feature')
    # ### end Alembic commands ###
