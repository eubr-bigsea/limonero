import datetime
import json
import re
from copy import deepcopy
from marshmallow import Schema, fields, post_load, post_dump, EXCLUDE, INCLUDE
from marshmallow.validate import OneOf
from flask_babel import gettext
from .models import *


def partial_schema_factory(schema_cls):
    schema = schema_cls(partial=True)
    for field_name, field in list(schema.fields.items()):
        if isinstance(field, fields.Nested):
            new_field = deepcopy(field)
            new_field.schema.partial = True
            schema.fields[field_name] = new_field
    return schema


enum_re = re.compile(r'(Must be one of:) (.+)')

enum_re = re.compile(r'(Must be one of:) (.+)')


def translate_validation(validation_errors):
    for field, errors in list(validation_errors.items()):
        if isinstance(errors, dict):
            validation_errors[field] = translate_validation(errors)
        else:
            final_errors = []
            for error in errors:
                found = enum_re.findall(error)
                if found:
                    final_errors.append(
                        f'{gettext(found[0][0])} {found[0][1]}')
                else:
                    final_errors.append(gettext(error))
            validation_errors[field] = final_errors
        return validation_errors


def load_json(str_value):
    try:
        return json.loads(str_value)
    except BaseException:
        return None


# region Protected
def generate_download_token(identifier, expires=None):
    from flask import current_app
    from cryptography.fernet import Fernet
    import time

    f = current_app.fernet
    if expires is None:
        f_expires = 0
    else:
        f_expires = time.time() + expires
    return f.encrypt('{{"id": {}, "expires": {} }}'.format(
        identifier, f_expires).encode('utf8')).decode('utf8')
# endregion


class BaseSchema(Schema):
    @post_dump
    def remove_skip_values(self, data, **kwargs):
        return {
            key: value for key, value in data.items()
            if value is not None  # Empty lists must be kept!
        }


class AttributeListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.values())])
    size = fields.Integer(required=False, allow_none=True)
    precision = fields.Integer(required=False, allow_none=True)
    scale = fields.Integer(required=False, allow_none=True)
    nullable = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    enumeration = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    label = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)
    format = fields.String(required=False, allow_none=True)
    key = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    attribute_privacy = fields.Nested(
        'limonero.schema.AttributePrivacyListResponseSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Attribute"""
        return Attribute(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributeItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.values())])
    size = fields.Integer(required=False, allow_none=True)
    precision = fields.Integer(required=False, allow_none=True)
    scale = fields.Integer(required=False, allow_none=True)
    nullable = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    enumeration = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    label = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)
    format = fields.String(required=False, allow_none=True)
    key = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    attribute_privacy = fields.Nested(
        'limonero.schema.AttributePrivacyItemResponseSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Attribute"""
        return Attribute(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributeCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.values())])
    size = fields.Integer(required=False, allow_none=True)
    precision = fields.Integer(required=False, allow_none=True)
    scale = fields.Integer(required=False, allow_none=True)
    nullable = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    enumeration = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    label = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)
    format = fields.String(required=False, allow_none=True)
    key = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    attribute_privacy = fields.Nested(
        'limonero.schema.AttributePrivacyCreateRequestSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Attribute"""
        return Attribute(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributePrivacyResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    attribute_privacy = fields.Nested(
        'limonero.schema.AttributePrivacyItemResponseSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Attribute"""
        return Attribute(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributePrivacyListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    attribute_name = fields.String(required=True)
    data_type = fields.String(required=False, allow_none=True,
                              validate=[OneOf(DataType.values())])
    privacy_type = fields.String(required=True,
                                 validate=[OneOf(PrivacyType.values())])
    category_technique = fields.String(required=False, allow_none=True)
    anonymization_technique = fields.String(required=True,
                                            validate=[OneOf(AnonymizationTechnique.values())])
    hierarchical_structure_type = fields.String(
        required=False, allow_none=True)
    privacy_model_technique = fields.String(required=False, allow_none=True)
    hierarchy = fields.String(required=False, allow_none=True)
    category_model = fields.String(required=False, allow_none=True)
    privacy_model = fields.String(required=False, allow_none=True)
    privacy_model_parameters = fields.String(required=False, allow_none=True)
    unlock_privacy_key = fields.String(required=False, allow_none=True)
    is_global_law = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    attribute_privacy_group = fields.Nested(
        'limonero.schema.AttributePrivacyGroupListResponseSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of AttributePrivacy"""
        return AttributePrivacy(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributePrivacyItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    attribute_name = fields.String(required=True)
    data_type = fields.String(required=False, allow_none=True,
                              validate=[OneOf(DataType.values())])
    privacy_type = fields.String(required=True,
                                 validate=[OneOf(PrivacyType.values())])
    category_technique = fields.String(required=False, allow_none=True)
    anonymization_technique = fields.String(required=True,
                                            validate=[OneOf(AnonymizationTechnique.values())])
    hierarchical_structure_type = fields.String(
        required=False, allow_none=True)
    privacy_model_technique = fields.String(required=False, allow_none=True)
    hierarchy = fields.String(required=False, allow_none=True)
    category_model = fields.String(required=False, allow_none=True)
    privacy_model = fields.String(required=False, allow_none=True)
    privacy_model_parameters = fields.String(required=False, allow_none=True)
    unlock_privacy_key = fields.String(required=False, allow_none=True)
    is_global_law = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    attribute_privacy_group_id = fields.Integer(
        required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of AttributePrivacy"""
        return AttributePrivacy(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributePrivacyCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    attribute_name = fields.String(required=True)
    data_type = fields.String(required=False, allow_none=True,
                              validate=[OneOf(DataType.values())])
    privacy_type = fields.String(required=True,
                                 validate=[OneOf(PrivacyType.values())])
    category_technique = fields.String(required=False, allow_none=True)
    anonymization_technique = fields.String(required=True,
                                            validate=[OneOf(AnonymizationTechnique.values())])
    hierarchical_structure_type = fields.String(
        required=False, allow_none=True)
    privacy_model_technique = fields.String(required=False, allow_none=True)
    hierarchy = fields.String(required=False, allow_none=True)
    category_model = fields.String(required=False, allow_none=True)
    privacy_model = fields.String(required=False, allow_none=True)
    privacy_model_parameters = fields.String(required=False, allow_none=True)
    unlock_privacy_key = fields.String(required=False, allow_none=True)
    attribute_id = fields.Integer(required=False, allow_none=True)
    attribute_privacy_group_id = fields.Integer(
        required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of AttributePrivacy"""
        return AttributePrivacy(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributePrivacyPrivacyResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    data_type = fields.String(required=False, allow_none=True,
                              validate=[OneOf(DataType.values())])
    privacy_type = fields.String(required=True,
                                 validate=[OneOf(PrivacyType.values())])
    category_technique = fields.String(required=False, allow_none=True)
    anonymization_technique = fields.String(required=True,
                                            validate=[OneOf(AnonymizationTechnique.values())])
    hierarchical_structure_type = fields.String(
        required=False, allow_none=True)
    privacy_model_technique = fields.String(required=False, allow_none=True)
    hierarchy = fields.String(required=False, allow_none=True)
    category_model = fields.String(required=False, allow_none=True)
    privacy_model = fields.String(required=False, allow_none=True)
    privacy_model_parameters = fields.String(required=False, allow_none=True)
    unlock_privacy_key = fields.String(required=False, allow_none=True)
    is_global_law = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of AttributePrivacy"""
        return AttributePrivacy(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributePrivacyGroupListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of AttributePrivacyGroup"""
        return AttributePrivacyGroup(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributePrivacyGroupItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of AttributePrivacyGroup"""
        return AttributePrivacyGroup(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class AttributePrivacyGroupCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    name = fields.String(required=True)
    user_id = fields.Integer(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of AttributePrivacyGroup"""
        return AttributePrivacyGroup(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourceExecuteRequestSchema(BaseSchema):
    """ JSON schema for executing tasks """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourceListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    statistics_process_counter = fields.Integer(
        required=False, allow_none=True, load_default=0, dump_default=0)
    read_only = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    privacy_aware = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    url = fields.String(required=True)
    created = fields.DateTime(required=False, allow_none=True)
    updated = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.values())])
    initialization = fields.String(required=False, allow_none=True, load_default=DataSourceInitialization.INITIALIZED, dump_default=DataSourceInitialization.INITIALIZED,
                                   validate=[OneOf(DataSourceInitialization.values())])
    initialization_job_id = fields.String(required=False, allow_none=True)
    provenience = fields.String(required=False, allow_none=True)
    estimated_rows = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    estimated_size_in_mega_bytes = fields.Decimal(
        required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    workflow_version = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attribute_delimiter = fields.String(required=False, allow_none=True)
    record_delimiter = fields.String(required=False, allow_none=True)
    text_delimiter = fields.String(required=False, allow_none=True)
    is_public = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    treat_as_missing = fields.String(required=False, allow_none=True)
    encoding = fields.String(required=False, allow_none=True)
    is_first_line_header = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    is_multiline = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    command = fields.String(required=False, allow_none=True)
    is_lookup = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    use_in_workflow = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    attributes = fields.Nested(
        'limonero.schema.AttributeListResponseSchema',
        allow_none=True,
        many=True)
    variables = fields.Nested(
        'limonero.schema.DataSourceVariableListResponseSchema',
        allow_none=True,
        many=True)
    permissions = fields.Nested(
        'limonero.schema.DataSourcePermissionListResponseSchema',
        allow_none=True,
        many=True)
    storage = fields.Nested(
        'limonero.schema.StorageListResponseSchema',
        required=True)
    download_token = fields.Function(
        lambda d: generate_download_token(d.id, 600))

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourceCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    statistics_process_counter = fields.Integer(
        required=False, allow_none=True, load_default=0, dump_default=0)
    read_only = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    privacy_aware = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    url = fields.String(required=True)
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.values())])
    initialization = fields.String(required=False, allow_none=True, load_default=DataSourceInitialization.INITIALIZED, dump_default=DataSourceInitialization.INITIALIZED,
                                   validate=[OneOf(DataSourceInitialization.values())])
    initialization_job_id = fields.String(required=False, allow_none=True)
    provenience = fields.String(required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    workflow_version = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attribute_delimiter = fields.String(required=False, allow_none=True)
    record_delimiter = fields.String(required=False, allow_none=True)
    text_delimiter = fields.String(required=False, allow_none=True)
    is_public = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    treat_as_missing = fields.String(required=False, allow_none=True)
    encoding = fields.String(required=False, allow_none=True)
    is_first_line_header = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    is_multiline = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    command = fields.String(required=False, allow_none=True)
    is_lookup = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    use_in_workflow = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    attributes = fields.Nested(
        'limonero.schema.AttributeCreateRequestSchema',
        allow_none=True,
        many=True)
    variables = fields.Nested(
        'limonero.schema.DataSourceVariableCreateRequestSchema',
        allow_none=True,
        many=True)
    permissions = fields.Nested(
        'limonero.schema.DataSourcePermissionCreateRequestSchema',
        allow_none=True,
        many=True)
    storage_id = fields.Integer(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourceItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    statistics_process_counter = fields.Integer(
        required=False, allow_none=True, load_default=0, dump_default=0)
    read_only = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    privacy_aware = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    url = fields.String(required=True)
    created = fields.DateTime(required=False, allow_none=True)
    updated = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.values())])
    initialization = fields.String(required=False, allow_none=True, load_default=DataSourceInitialization.INITIALIZED, dump_default=DataSourceInitialization.INITIALIZED,
                                   validate=[OneOf(DataSourceInitialization.values())])
    initialization_job_id = fields.String(required=False, allow_none=True)
    provenience = fields.String(required=False, allow_none=True)
    estimated_rows = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    estimated_size_in_mega_bytes = fields.Decimal(
        required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    workflow_version = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attribute_delimiter = fields.String(required=False, allow_none=True)
    record_delimiter = fields.String(required=False, allow_none=True)
    text_delimiter = fields.String(required=False, allow_none=True)
    is_public = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    treat_as_missing = fields.String(required=False, allow_none=True)
    encoding = fields.String(required=False, allow_none=True)
    is_first_line_header = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    is_multiline = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    command = fields.String(required=False, allow_none=True)
    is_lookup = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    use_in_workflow = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    attributes = fields.Nested(
        'limonero.schema.AttributeItemResponseSchema',
        allow_none=True,
        many=True)
    variables = fields.Nested(
        'limonero.schema.DataSourceVariableItemResponseSchema',
        allow_none=True,
        many=True)
    permissions = fields.Nested(
        'limonero.schema.DataSourcePermissionItemResponseSchema',
        allow_none=True,
        many=True)
    storage = fields.Nested(
        'limonero.schema.StorageItemResponseSchema',
        required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourcePrivacyResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    privacy_aware = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=False,
        dump_default=False)
    attributes = fields.Nested(
        'limonero.schema.AttributePrivacyResponseSchema',
        allow_none=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourcePermissionListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.values())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSourcePermission"""
        return DataSourcePermission(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourcePermissionItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.values())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSourcePermission"""
        return DataSourcePermission(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourcePermissionCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.values())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSourcePermission"""
        return DataSourcePermission(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourceVariableListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    label = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)
    default_value = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSourceVariable"""
        return DataSourceVariable(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourceVariableItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    label = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)
    default_value = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSourceVariable"""
        return DataSourceVariable(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class DataSourceVariableCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    name = fields.String(required=True)
    label = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)
    default_value = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of DataSourceVariable"""
        return DataSourceVariable(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class ModelListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    created = fields.DateTime(required=False, allow_none=True)
    path = fields.String(required=True)
    class_name = fields.String(required=True)
    type = fields.String(required=False, allow_none=True, load_default=ModelType.UNSPECIFIED, dump_default=ModelType.UNSPECIFIED,
                         validate=[OneOf(ModelType.values())])
    deployment_status = fields.String(required=False, allow_none=True, load_default=DeploymentStatus.NOT_DEPLOYED, dump_default=DeploymentStatus.NOT_DEPLOYED,
                                      validate=[OneOf(DeploymentStatus.values())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    workflow_id = fields.Integer(required=False, allow_none=True)
    workflow_name = fields.String(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    job_id = fields.Integer(required=False, allow_none=True)
    storage = fields.Nested(
        'limonero.schema.StorageListResponseSchema',
        required=True)
    download_token = fields.Function(
        lambda d: generate_download_token(d.id, 600))

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Model"""
        return Model(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class ModelCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    path = fields.String(required=True)
    class_name = fields.String(required=True)
    type = fields.String(required=False, allow_none=True, load_default=ModelType.UNSPECIFIED, dump_default=ModelType.UNSPECIFIED,
                         validate=[OneOf(ModelType.values())])
    deployment_status = fields.String(required=False, allow_none=True, load_default=DeploymentStatus.NOT_DEPLOYED, dump_default=DeploymentStatus.NOT_DEPLOYED,
                                      validate=[OneOf(DeploymentStatus.values())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    workflow_id = fields.Integer(required=False, allow_none=True)
    workflow_name = fields.String(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    job_id = fields.Integer(required=False, allow_none=True)
    storage_id = fields.Integer(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Model"""
        return Model(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class ModelItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    created = fields.DateTime(required=False, allow_none=True)
    path = fields.String(required=True)
    class_name = fields.String(required=True)
    type = fields.String(required=False, allow_none=True, load_default=ModelType.UNSPECIFIED, dump_default=ModelType.UNSPECIFIED,
                         validate=[OneOf(ModelType.values())])
    deployment_status = fields.String(required=False, allow_none=True, load_default=DeploymentStatus.NOT_DEPLOYED, dump_default=DeploymentStatus.NOT_DEPLOYED,
                                      validate=[OneOf(DeploymentStatus.values())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    workflow_id = fields.Integer(required=False, allow_none=True)
    workflow_name = fields.String(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    job_id = fields.Integer(required=False, allow_none=True)
    storage = fields.Nested(
        'limonero.schema.StorageItemResponseSchema',
        required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Model"""
        return Model(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class ModelPermissionListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.values())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ModelPermission"""
        return ModelPermission(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class ModelPermissionItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.values())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ModelPermission"""
        return ModelPermission(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class StorageListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.values())])
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    url = fields.String(required=True)
    client_url = fields.String(required=False, allow_none=True)
    extra_params = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Storage"""
        return Storage(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class StorageItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.values())])
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    url = fields.String(required=True)
    client_url = fields.String(required=False, allow_none=True)
    extra_params = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Storage"""
        return Storage(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class StorageCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.values())])
    enabled = fields.Boolean(
        required=False,
        allow_none=True,
        load_default=True,
        dump_default=True)
    url = fields.String(required=True)
    client_url = fields.String(required=False, allow_none=True)
    extra_params = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Storage"""
        return Storage(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE

