"""add Metric and MetricResult collections

Revision ID: fe2b109f96f3
Revises: f6f1b7d28538
Create Date: 2023-11-06 11:08:35.553531

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.sql import text
from limonero.migration_utils import (downgrade_actions, upgrade_actions,
        is_mysql, is_psql, is_sqlite, get_psql_enum_alter_commands)

# revision identifiers, used by Alembic.
revision = 'fe2b109f96f3'
down_revision = 'f6f1b7d28538'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('attribute_privacy_group',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=100), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('storage',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=100), nullable=False),
    sa.Column('type', sa.Enum('MONGODB', 'ELASTIC_SEARCH', 'HDFS', 'HIVE', 'HIVE_WAREHOUSE', 'KAFKA', 'LOCAL', 'JDBC', 'CASSANDRA', name='StorageTypeEnumType'), nullable=False),
    sa.Column('enabled', sa.Boolean(), nullable=False),
    sa.Column('url', sa.String(length=1000), nullable=False),
    sa.Column('client_url', sa.String(length=1000), nullable=True),
    # sa.Column('extra_params', mysql.LONGTEXT(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('data_source',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=100), nullable=False),
    sa.Column('description', sa.String(length=500), nullable=True),
    sa.Column('enabled', sa.Boolean(), nullable=False),
    sa.Column('statistics_process_counter', sa.Integer(), nullable=False),
    sa.Column('read_only', sa.Boolean(), nullable=False),
    sa.Column('privacy_aware', sa.Boolean(), nullable=False),
    sa.Column('url', sa.String(length=200), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=False),
    sa.Column('format', sa.Enum('CSV', 'CUSTOM', 'GEO_JSON', 'JDBC', 'IMAGE_FOLDER', 'DATA_FOLDER', 'HAR_IMAGE_FOLDER', 'HDF5', 'HIVE', 'JSON', 'NPY', 'PICKLE', 'PARQUET', 'SAV', 'SHAPEFILE', 'TAR_IMAGE_FOLDER', 'TEXT', 'VIDEO_FOLDER', 'XML_FILE', 'UNKNOWN', name='DataSourceFormatEnumType'), nullable=False),
    sa.Column('initialization', sa.Enum('NO_INITIALIZED', 'INITIALIZING', 'INITIALIZED', name='DataSourceInitializationEnumType'), nullable=False),
    sa.Column('initialization_job_id', sa.String(length=200), nullable=True),
    # sa.Column('provenience', mysql.LONGTEXT(), nullable=True),
    sa.Column('estimated_rows', sa.Integer(), nullable=True),
    sa.Column('estimated_size_in_mega_bytes', sa.Numeric(precision=10, scale=2), nullable=True),
    sa.Column('expiration', sa.String(length=200), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('user_login', sa.String(length=50), nullable=True),
    sa.Column('user_name', sa.String(length=200), nullable=True),
    sa.Column('tags', sa.String(length=100), nullable=True),
    sa.Column('temporary', sa.Boolean(), nullable=False),
    sa.Column('workflow_id', sa.Integer(), nullable=True),
    sa.Column('workflow_version', sa.Integer(), nullable=True),
    sa.Column('task_id', sa.String(length=200), nullable=True),
    sa.Column('attribute_delimiter', sa.String(length=20), nullable=True),
    sa.Column('record_delimiter', sa.String(length=20), nullable=True),
    sa.Column('text_delimiter', sa.String(length=20), nullable=True),
    sa.Column('is_public', sa.Boolean(), nullable=False),
    # sa.Column('treat_as_missing', mysql.LONGTEXT(), nullable=True),
    sa.Column('encoding', sa.String(length=200), nullable=True),
    sa.Column('is_first_line_header', sa.Boolean(), nullable=False),
    sa.Column('is_multiline', sa.Boolean(), nullable=False),
    # sa.Column('command', mysql.LONGTEXT(), nullable=True),
    sa.Column('is_lookup', sa.Boolean(), nullable=False),
    sa.Column('use_in_workflow', sa.Boolean(), nullable=False),
    sa.Column('storage_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['storage_id'], ['storage.id'], name='fk_data_source_storage_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_data_source_storage_id'), 'data_source', ['storage_id'], unique=False)
    op.create_index(op.f('ix_data_source_task_id'), 'data_source', ['task_id'], unique=False)
    op.create_index(op.f('ix_data_source_use_in_workflow'), 'data_source', ['use_in_workflow'], unique=False)
    op.create_index(op.f('ix_data_source_workflow_id'), 'data_source', ['workflow_id'], unique=False)
    op.create_table('model',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=100), nullable=False),
    sa.Column('enabled', sa.Boolean(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('path', sa.String(length=500), nullable=False),
    sa.Column('class_name', sa.String(length=500), nullable=False),
    sa.Column('type', sa.Enum('KERAS', 'MLEAP', 'PERFORMANCE_SPARK', 'PERFORMANCE_KERAS', 'SPARK_ML_CLASSIFICATION', 'SPARK_ML_REGRESSION', 'SPARK_MLLIB_CLASSIFICATION', 'UNSPECIFIED', name='ModelTypeEnumType'), nullable=False),
    sa.Column('deployment_status', sa.Enum('NOT_DEPLOYED', 'ERROR', 'EDITING', 'SAVED', 'RUNNING', 'STOPPED', 'SUSPENDED', 'PENDING', 'DEPLOYED', name='DeploymentStatusEnumType'), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('user_login', sa.String(length=50), nullable=False),
    sa.Column('user_name', sa.String(length=200), nullable=False),
    sa.Column('workflow_id', sa.Integer(), nullable=True),
    sa.Column('workflow_name', sa.String(length=200), nullable=True),
    sa.Column('task_id', sa.String(length=200), nullable=True),
    sa.Column('job_id', sa.Integer(), nullable=True),
    sa.Column('storage_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['storage_id'], ['storage.id'], name='fk_model_storage_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_model_storage_id'), 'model', ['storage_id'], unique=False)
    op.create_table('storage_permission',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('permission', sa.Enum('READ', 'WRITE', 'MANAGE', name='PermissionTypeEnumType'), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('storage_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['storage_id'], ['storage.id'], name='fk_storage_permission_storage_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_storage_permission_storage_id'), 'storage_permission', ['storage_id'], unique=False)
    op.create_table('attribute',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=100), nullable=False),
    sa.Column('description', sa.String(length=500), nullable=True),
    sa.Column('type', sa.Enum('BINARY', 'CHARACTER', 'DATE', 'DATETIME', 'DECIMAL', 'DOUBLE', 'ENUM', 'FILE', 'FLOAT', 'INTEGER', 'LAT_LONG', 'LONG', 'TEXT', 'TIME', 'TIMESTAMP', 'VECTOR', name='DataTypeEnumType'), nullable=False),
    sa.Column('size', sa.Integer(), nullable=True),
    sa.Column('precision', sa.Integer(), nullable=True),
    sa.Column('scale', sa.Integer(), nullable=True),
    sa.Column('nullable', sa.Boolean(), nullable=False),
    sa.Column('enumeration', sa.Boolean(), nullable=False),
    sa.Column('missing_representation', sa.String(length=200), nullable=True),
    sa.Column('feature', sa.Boolean(), nullable=False),
    sa.Column('label', sa.Boolean(), nullable=False),
    sa.Column('distinct_values', sa.Integer(), nullable=True),
    sa.Column('mean_value', sa.Float(), nullable=True),
    sa.Column('median_value', sa.String(length=200), nullable=True),
    sa.Column('max_value', sa.String(length=200), nullable=True),
    sa.Column('min_value', sa.String(length=200), nullable=True),
    sa.Column('std_deviation', sa.Float(), nullable=True),
    sa.Column('missing_total', sa.String(length=200), nullable=True),
    # sa.Column('deciles', mysql.LONGTEXT(), nullable=True),
    sa.Column('format', sa.String(length=100), nullable=True),
    sa.Column('key', sa.Boolean(), nullable=False),
    sa.Column('data_source_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['data_source_id'], ['data_source.id'], name='fk_attribute_data_source_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_attribute_data_source_id'), 'attribute', ['data_source_id'], unique=False)
    op.create_table('data_source_foreign_key',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('from_source_id', sa.Integer(), nullable=False),
    sa.Column('to_source_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['from_source_id'], ['data_source.id'], name='fk_data_source_foreign_key_from_source_id'),
    sa.ForeignKeyConstraint(['to_source_id'], ['data_source.id'], name='fk_data_source_foreign_key_to_source_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_data_source_foreign_key_from_source_id'), 'data_source_foreign_key', ['from_source_id'], unique=False)
    op.create_index(op.f('ix_data_source_foreign_key_to_source_id'), 'data_source_foreign_key', ['to_source_id'], unique=False)
    op.create_table('data_source_permission',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('permission', sa.Enum('READ', 'WRITE', 'MANAGE', name='PermissionTypeEnumType'), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('user_login', sa.String(length=50), nullable=False),
    sa.Column('user_name', sa.String(length=200), nullable=False),
    sa.Column('data_source_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['data_source_id'], ['data_source.id'], name='fk_data_source_permission_data_source_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_data_source_permission_data_source_id'), 'data_source_permission', ['data_source_id'], unique=False)
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
    op.create_table('model_permission',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('permission', sa.Enum('READ', 'WRITE', 'MANAGE', name='PermissionTypeEnumType'), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('user_login', sa.String(length=50), nullable=False),
    sa.Column('user_name', sa.String(length=200), nullable=False),
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['model_id'], ['model.id'], name='fk_model_permission_model_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_model_permission_model_id'), 'model_permission', ['model_id'], unique=False)
    op.create_table('privacy_risk',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('type', sa.Enum('IDENTIFICATION', name='PrivacyRiskTypeEnumType'), nullable=False),
    sa.Column('probability', sa.Float(), nullable=True),
    sa.Column('impact', sa.Float(), nullable=True),
    sa.Column('value', sa.Float(), nullable=False),
    # sa.Column('detail', mysql.LONGTEXT(), nullable=False),
    sa.Column('data_source_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['data_source_id'], ['data_source.id'], name='fk_privacy_risk_data_source_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_privacy_risk_data_source_id'), 'privacy_risk', ['data_source_id'], unique=False)
    op.create_table('attribute_foreign_key',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('order', sa.Integer(), nullable=False),
    sa.Column('direction', sa.Enum('FROM', 'TO', name='AttributeForeignKeyDirectionEnumType'), nullable=False),
    sa.Column('foreign_key_id', sa.Integer(), nullable=False),
    sa.Column('from_attribute_id', sa.Integer(), nullable=False),
    sa.Column('to_attribute_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['foreign_key_id'], ['data_source_foreign_key.id'], name='fk_attribute_foreign_key_foreign_key_id'),
    sa.ForeignKeyConstraint(['from_attribute_id'], ['attribute.id'], name='fk_attribute_foreign_key_from_attribute_id'),
    sa.ForeignKeyConstraint(['to_attribute_id'], ['attribute.id'], name='fk_attribute_foreign_key_to_attribute_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_attribute_foreign_key_foreign_key_id'), 'attribute_foreign_key', ['foreign_key_id'], unique=False)
    op.create_index(op.f('ix_attribute_foreign_key_from_attribute_id'), 'attribute_foreign_key', ['from_attribute_id'], unique=False)
    op.create_index(op.f('ix_attribute_foreign_key_to_attribute_id'), 'attribute_foreign_key', ['to_attribute_id'], unique=False)
    op.create_table('attribute_privacy',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('attribute_name', sa.String(length=200), nullable=False),
    sa.Column('data_type', sa.Enum('BINARY', 'CHARACTER', 'DATE', 'DATETIME', 'DECIMAL', 'DOUBLE', 'ENUM', 'FILE', 'FLOAT', 'INTEGER', 'LAT_LONG', 'LONG', 'TEXT', 'TIME', 'TIMESTAMP', 'VECTOR', name='DataTypeEnumType'), nullable=True),
    sa.Column('privacy_type', sa.Enum('IDENTIFIER', 'QUASI_IDENTIFIER', 'SENSITIVE', 'NON_SENSITIVE', name='PrivacyTypeEnumType'), nullable=False),
    sa.Column('category_technique', sa.String(length=100), nullable=True),
    sa.Column('anonymization_technique', sa.Enum('ENCRYPTION', 'GENERALIZATION', 'SUPPRESSION', 'MASK', 'NO_TECHNIQUE', name='AnonymizationTechniqueEnumType'), nullable=False),
    sa.Column('hierarchical_structure_type', sa.String(length=100), nullable=True),
    sa.Column('privacy_model_technique', sa.String(length=100), nullable=True),
    # sa.Column('hierarchy', mysql.LONGTEXT(), nullable=True),
    # sa.Column('category_model', mysql.LONGTEXT(), nullable=True),
    # sa.Column('privacy_model', mysql.LONGTEXT(), nullable=True),
    # sa.Column('privacy_model_parameters', mysql.LONGTEXT(), nullable=True),
    sa.Column('unlock_privacy_key', sa.String(length=400), nullable=True),
    sa.Column('is_global_law', sa.Boolean(), nullable=True),
    sa.Column('attribute_id', sa.Integer(), nullable=True),
    sa.Column('attribute_privacy_group_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['attribute_id'], ['attribute.id'], name='fk_attribute_privacy_attribute_id'),
    sa.ForeignKeyConstraint(['attribute_privacy_group_id'], ['attribute_privacy_group.id'], name='fk_attribute_privacy_attribute_privacy_group_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_attribute_privacy_attribute_id'), 'attribute_privacy', ['attribute_id'], unique=False)
    op.create_index(op.f('ix_attribute_privacy_attribute_privacy_group_id'), 'attribute_privacy', ['attribute_privacy_group_id'], unique=False)
    
    
    if is_sqlite():
        with op.batch_alter_table('storage') as batch_op:
            batch_op.add_column(sa.Column('extra_params', sa.Text(), nullable=True))
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.add_column(sa.Column('provenience', sa.Text(), nullable=True))
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.add_column(sa.Column('treat_as_missing', sa.Text(), nullable=True))
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.add_column(sa.Column('command', sa.Text(), nullable=True))
        with op.batch_alter_table('attribute') as batch_op:
            batch_op.add_column(sa.Column('deciles', sa.Text(), nullable=True))
        with op.batch_alter_table('metric_result') as batch_op:
            batch_op.add_column(sa.Column('value', sa.Text(), nullable=False))
        with op.batch_alter_table('privacy_risk') as batch_op:
            batch_op.add_column(sa.Column('detail', sa.Text(), nullable=False))
        with op.batch_alter_table('attribute_privacy') as batch_op:
            batch_op.add_column(sa.Column('hierarchy', sa.Text(), nullable=True))
            batch_op.add_column(sa.Column('category_model', sa.Text(), nullable=True))
            batch_op.add_column(sa.Column('privacy_model', sa.Text(), nullable=True))
            batch_op.add_column(sa.Column('privacy_model_parameters', sa.Text(), nullable=True))
            
        
    else:
        op.add_column('storage',
            sa.Column('extra_params', mysql.LONGTEXT(), nullable=True),)
        op.add_column('data_source',
            sa.Column('provenience', mysql.LONGTEXT(), nullable=True))
        op.add_column('data_source',
            sa.Column('treat_as_missing', mysql.LONGTEXT(), nullable=True))
        op.add_column('data_source',
            sa.Column('command', mysql.LONGTEXT(), nullable=True))
        op.add_column('attribute',
            sa.Column('deciles', mysql.LONGTEXT(), nullable=True))
        op.add_column('metric_result',
            sa.Column('value', mysql.LONGTEXT(), nullable=False))
        op.add_column('privacy_risk',
            sa.Column('detail', mysql.LONGTEXT(), nullable=False))
        op.add_column('attribute_privacy',
            sa.Column('hierarchy', mysql.LONGTEXT(), nullable=True),
            sa.Column('category_model', mysql.LONGTEXT(), nullable=True),
            sa.Column('privacy_model', mysql.LONGTEXT(), nullable=True),
            sa.Column('privacy_model_parameters', mysql.LONGTEXT(), nullable=True))


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_attribute_privacy_attribute_privacy_group_id'), table_name='attribute_privacy')
    op.drop_index(op.f('ix_attribute_privacy_attribute_id'), table_name='attribute_privacy')
    op.drop_table('attribute_privacy')
    op.drop_index(op.f('ix_attribute_foreign_key_to_attribute_id'), table_name='attribute_foreign_key')
    op.drop_index(op.f('ix_attribute_foreign_key_from_attribute_id'), table_name='attribute_foreign_key')
    op.drop_index(op.f('ix_attribute_foreign_key_foreign_key_id'), table_name='attribute_foreign_key')
    op.drop_table('attribute_foreign_key')
    op.drop_index(op.f('ix_privacy_risk_data_source_id'), table_name='privacy_risk')
    op.drop_table('privacy_risk')
    op.drop_index(op.f('ix_model_permission_model_id'), table_name='model_permission')
    op.drop_table('model_permission')
    op.drop_index(op.f('ix_metric_result_model_id'), table_name='metric_result')
    op.drop_table('metric_result')
    op.drop_index(op.f('ix_metric_model_id'), table_name='metric')
    op.drop_table('metric')
    op.drop_index(op.f('ix_hyperparameter_model_id'), table_name='hyperparameter')
    op.drop_table('hyperparameter')
    op.drop_index(op.f('ix_feature_model_id'), table_name='feature')
    op.drop_table('feature')
    op.drop_index(op.f('ix_data_source_permission_data_source_id'), table_name='data_source_permission')
    op.drop_table('data_source_permission')
    op.drop_index(op.f('ix_data_source_foreign_key_to_source_id'), table_name='data_source_foreign_key')
    op.drop_index(op.f('ix_data_source_foreign_key_from_source_id'), table_name='data_source_foreign_key')
    op.drop_table('data_source_foreign_key')
    op.drop_index(op.f('ix_attribute_data_source_id'), table_name='attribute')
    op.drop_table('attribute')
    op.drop_index(op.f('ix_storage_permission_storage_id'), table_name='storage_permission')
    op.drop_table('storage_permission')
    op.drop_index(op.f('ix_model_storage_id'), table_name='model')
    op.drop_table('model')
    op.drop_index(op.f('ix_data_source_workflow_id'), table_name='data_source')
    op.drop_index(op.f('ix_data_source_use_in_workflow'), table_name='data_source')
    op.drop_index(op.f('ix_data_source_task_id'), table_name='data_source')
    op.drop_index(op.f('ix_data_source_storage_id'), table_name='data_source')
    op.drop_table('data_source')
    op.drop_table('storage')
    op.drop_table('attribute_privacy_group')
    # ### end Alembic commands ###
