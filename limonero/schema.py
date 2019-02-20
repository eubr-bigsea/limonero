# -*- coding: utf-8 -*-
import datetime
import json
from copy import deepcopy
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf
from limonero.models import *


def partial_schema_factory(schema_cls):
    schema = schema_cls(partial=True)
    for field_name, field in schema.fields.items():
        if isinstance(field, fields.Nested):
            new_field = deepcopy(field)
            new_field.schema.partial = True
            schema.fields[field_name] = new_field
    return schema


def load_json(str_value):
    try:
        return json.loads(str_value)
    except BaseException:
        return "Error loading JSON"


# region Protected\s*
# endregion


class AttributeListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.__dict__.keys())])
    size = fields.Integer(required=False, allow_none=True)
    precision = fields.Integer(required=False, allow_none=True)
    scale = fields.Integer(required=False, allow_none=True)
    nullable = fields.Boolean(required=True, missing=False)
    enumeration = fields.Boolean(required=True, missing=False)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(required=True, missing=True)
    label = fields.Boolean(required=True, missing=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)
    format = fields.String(required=False, allow_none=True)
    attribute_privacy = fields.Nested(
        'limonero.schema.AttributePrivacyListResponseSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Attribute"""
        return Attribute(**data)

    class Meta:
        ordered = True


class AttributeItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.__dict__.keys())])
    size = fields.Integer(required=False, allow_none=True)
    precision = fields.Integer(required=False, allow_none=True)
    scale = fields.Integer(required=False, allow_none=True)
    nullable = fields.Boolean(required=True, missing=False)
    enumeration = fields.Boolean(required=True, missing=False)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(required=True, missing=True)
    label = fields.Boolean(required=True, missing=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)
    format = fields.String(required=False, allow_none=True)
    attribute_privacy = fields.Nested(
        'limonero.schema.AttributePrivacyItemResponseSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Attribute"""
        return Attribute(**data)

    class Meta:
        ordered = True


class AttributeCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.__dict__.keys())])
    size = fields.Integer(required=False, allow_none=True)
    precision = fields.Integer(required=False, allow_none=True)
    scale = fields.Integer(required=False, allow_none=True)
    nullable = fields.Boolean(required=True, missing=False)
    enumeration = fields.Boolean(required=True, missing=False)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(required=True, missing=True)
    label = fields.Boolean(required=True, missing=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)
    format = fields.String(required=False, allow_none=True)
    attribute_privacy = fields.Nested(
        'limonero.schema.AttributePrivacyCreateRequestSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Attribute"""
        return Attribute(**data)

    class Meta:
        ordered = True


class AttributePrivacyResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    attribute_privacy = fields.Nested(
        'limonero.schema.AttributePrivacyItemResponseSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Attribute"""
        return Attribute(**data)

    class Meta:
        ordered = True


class AttributePrivacyListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    attribute_name = fields.String(required=True)
    data_type = fields.String(required=False, allow_none=True,
                              validate=[OneOf(DataType.__dict__.keys())])
    privacy_type = fields.String(required=True,
                                 validate=[OneOf(PrivacyType.__dict__.keys())])
    category_technique = fields.String(required=False, allow_none=True)
    anonymization_technique = fields.String(required=True,
                                            validate=[OneOf(AnonymizationTechnique.__dict__.keys())])
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
        missing=False)
    attribute_privacy_group = fields.Nested(
        'limonero.schema.AttributePrivacyGroupListResponseSchema',
        allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of AttributePrivacy"""
        return AttributePrivacy(**data)

    class Meta:
        ordered = True


class AttributePrivacyItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    attribute_name = fields.String(required=True)
    data_type = fields.String(required=False, allow_none=True,
                              validate=[OneOf(DataType.__dict__.keys())])
    privacy_type = fields.String(required=True,
                                 validate=[OneOf(PrivacyType.__dict__.keys())])
    category_technique = fields.String(required=False, allow_none=True)
    anonymization_technique = fields.String(required=True,
                                            validate=[OneOf(AnonymizationTechnique.__dict__.keys())])
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
        missing=False)
    attribute_privacy_group_id = fields.Integer(
        required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of AttributePrivacy"""
        return AttributePrivacy(**data)

    class Meta:
        ordered = True


class AttributePrivacyCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    attribute_name = fields.String(required=True)
    data_type = fields.String(required=False, allow_none=True,
                              validate=[OneOf(DataType.__dict__.keys())])
    privacy_type = fields.String(required=True,
                                 validate=[OneOf(PrivacyType.__dict__.keys())])
    category_technique = fields.String(required=False, allow_none=True)
    anonymization_technique = fields.String(required=True,
                                            validate=[OneOf(AnonymizationTechnique.__dict__.keys())])
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
    def make_object(self, data):
        """ Deserialize data into an instance of AttributePrivacy"""
        return AttributePrivacy(**data)

    class Meta:
        ordered = True


class AttributePrivacyPrivacyResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    data_type = fields.String(required=False, allow_none=True,
                              validate=[OneOf(DataType.__dict__.keys())])
    privacy_type = fields.String(required=True,
                                 validate=[OneOf(PrivacyType.__dict__.keys())])
    category_technique = fields.String(required=False, allow_none=True)
    anonymization_technique = fields.String(required=True,
                                            validate=[OneOf(AnonymizationTechnique.__dict__.keys())])
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
        missing=False)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of AttributePrivacy"""
        return AttributePrivacy(**data)

    class Meta:
        ordered = True


class AttributePrivacyGroupListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of AttributePrivacyGroup"""
        return AttributePrivacyGroup(**data)

    class Meta:
        ordered = True


class AttributePrivacyGroupItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of AttributePrivacyGroup"""
        return AttributePrivacyGroup(**data)

    class Meta:
        ordered = True


class AttributePrivacyGroupCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    name = fields.String(required=True)
    user_id = fields.Integer(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of AttributePrivacyGroup"""
        return AttributePrivacyGroup(**data)

    class Meta:
        ordered = True


class DataSourceExecuteRequestSchema(Schema):
    """ JSON schema for executing tasks """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True


class DataSourceListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(required=True, missing=True)
    statistics_process_counter = fields.Integer(required=True, missing=0)
    read_only = fields.Boolean(required=True, missing=True)
    privacy_aware = fields.Boolean(required=True, missing=False)
    url = fields.String(required=True)
    created = fields.DateTime(required=True)
    updated = fields.DateTime(required=True, missing=datetime.datetime.utcnow)
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False, allow_none=True)
    estimated_rows = fields.Integer(required=False, allow_none=True, missing=0)
    estimated_size_in_mega_bytes = fields.Decimal(
        required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(required=True, missing=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attribute_delimiter = fields.String(required=False, allow_none=True)
    record_delimiter = fields.String(required=False, allow_none=True)
    text_delimiter = fields.String(required=False, allow_none=True)
    is_public = fields.Boolean(required=True, missing=False)
    treat_as_missing = fields.String(required=False, allow_none=True)
    encoding = fields.String(required=False, allow_none=True)
    is_first_line_header = fields.Boolean(required=True, missing=0)
    is_multiline = fields.Boolean(required=True, missing=0)
    command = fields.String(required=False, allow_none=True)
    attributes = fields.Nested(
        'limonero.schema.AttributeListResponseSchema',
        allow_none=True,
        many=True)
    permissions = fields.Nested(
        'limonero.schema.DataSourcePermissionListResponseSchema',
        allow_none=True,
        many=True)
    storage = fields.Nested(
        'limonero.schema.StorageListResponseSchema',
        required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True


class DataSourceCreateRequestSchema(Schema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(required=True, missing=True)
    statistics_process_counter = fields.Integer(required=True, missing=0)
    read_only = fields.Boolean(required=True, missing=True)
    privacy_aware = fields.Boolean(required=True, missing=False)
    url = fields.String(required=True)
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(required=True, missing=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attribute_delimiter = fields.String(required=False, allow_none=True)
    record_delimiter = fields.String(required=False, allow_none=True)
    text_delimiter = fields.String(required=False, allow_none=True)
    is_public = fields.Boolean(required=True, missing=False)
    treat_as_missing = fields.String(required=False, allow_none=True)
    encoding = fields.String(required=False, allow_none=True)
    is_first_line_header = fields.Boolean(required=True, missing=0)
    is_multiline = fields.Boolean(required=True, missing=0)
    command = fields.String(required=False, allow_none=True)
    attributes = fields.Nested(
        'limonero.schema.AttributeCreateRequestSchema',
        allow_none=True,
        many=True)
    permissions = fields.Nested(
        'limonero.schema.DataSourcePermissionCreateRequestSchema',
        allow_none=True,
        many=True)
    storage_id = fields.Integer(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True


class DataSourceItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(required=True, missing=True)
    statistics_process_counter = fields.Integer(required=True, missing=0)
    read_only = fields.Boolean(required=True, missing=True)
    privacy_aware = fields.Boolean(required=True, missing=False)
    url = fields.String(required=True)
    created = fields.DateTime(required=True)
    updated = fields.DateTime(required=True, missing=datetime.datetime.utcnow)
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False, allow_none=True)
    estimated_rows = fields.Integer(required=False, allow_none=True, missing=0)
    estimated_size_in_mega_bytes = fields.Decimal(
        required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(required=True, missing=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attribute_delimiter = fields.String(required=False, allow_none=True)
    record_delimiter = fields.String(required=False, allow_none=True)
    text_delimiter = fields.String(required=False, allow_none=True)
    is_public = fields.Boolean(required=True, missing=False)
    treat_as_missing = fields.String(required=False, allow_none=True)
    encoding = fields.String(required=False, allow_none=True)
    is_first_line_header = fields.Boolean(required=True, missing=0)
    is_multiline = fields.Boolean(required=True, missing=0)
    command = fields.String(required=False, allow_none=True)
    attributes = fields.Nested(
        'limonero.schema.AttributeItemResponseSchema',
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
    def make_object(self, data):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True


class DataSourcePrivacyResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    privacy_aware = fields.Boolean(required=True, missing=False)
    attributes = fields.Nested(
        'limonero.schema.AttributePrivacyResponseSchema',
        allow_none=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of DataSource"""
        return DataSource(**data)

    class Meta:
        ordered = True


class DataSourcePermissionListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.__dict__.keys())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of DataSourcePermission"""
        return DataSourcePermission(**data)

    class Meta:
        ordered = True


class DataSourcePermissionItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.__dict__.keys())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of DataSourcePermission"""
        return DataSourcePermission(**data)

    class Meta:
        ordered = True


class DataSourcePermissionCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.__dict__.keys())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of DataSourcePermission"""
        return DataSourcePermission(**data)

    class Meta:
        ordered = True


class ModelListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    enabled = fields.Boolean(required=True, missing=True)
    created = fields.DateTime(required=True)
    path = fields.String(required=True)
    class_name = fields.String(required=True)
    type = fields.String(required=True, missing=ModelType.UNSPECIFIED,
                         validate=[OneOf(ModelType.__dict__.keys())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    workflow_id = fields.Integer(required=True)
    workflow_name = fields.String(required=False, allow_none=True)
    task_id = fields.String(required=True)
    job_id = fields.Integer(required=True)
    storage = fields.Nested(
        'limonero.schema.StorageListResponseSchema',
        required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Model"""
        return Model(**data)

    class Meta:
        ordered = True


class ModelCreateRequestSchema(Schema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    enabled = fields.Boolean(required=True, missing=True)
    path = fields.String(required=True)
    class_name = fields.String(required=True)
    type = fields.String(required=True, missing=ModelType.UNSPECIFIED,
                         validate=[OneOf(ModelType.__dict__.keys())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    workflow_id = fields.Integer(required=True)
    workflow_name = fields.String(required=False, allow_none=True)
    task_id = fields.String(required=True)
    job_id = fields.Integer(required=True)
    storage_id = fields.Integer(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Model"""
        return Model(**data)

    class Meta:
        ordered = True


class ModelItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    enabled = fields.Boolean(required=True, missing=True)
    created = fields.DateTime(required=True)
    path = fields.String(required=True)
    class_name = fields.String(required=True)
    type = fields.String(required=True, missing=ModelType.UNSPECIFIED,
                         validate=[OneOf(ModelType.__dict__.keys())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    workflow_id = fields.Integer(required=True)
    workflow_name = fields.String(required=False, allow_none=True)
    task_id = fields.String(required=True)
    job_id = fields.Integer(required=True)
    storage = fields.Nested(
        'limonero.schema.StorageItemResponseSchema',
        required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Model"""
        return Model(**data)

    class Meta:
        ordered = True


class ModelPermissionListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.__dict__.keys())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of ModelPermission"""
        return ModelPermission(**data)

    class Meta:
        ordered = True


class ModelPermissionItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    permission = fields.String(required=True,
                               validate=[OneOf(PermissionType.__dict__.keys())])
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of ModelPermission"""
        return ModelPermission(**data)

    class Meta:
        ordered = True


class StorageListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.__dict__.keys())])
    enabled = fields.Boolean(required=True, missing=True)
    url = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Storage"""
        return Storage(**data)

    class Meta:
        ordered = True


class StorageItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.__dict__.keys())])
    enabled = fields.Boolean(required=True, missing=True)
    url = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Storage"""
        return Storage(**data)

    class Meta:
        ordered = True


class StorageCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    enabled = fields.Boolean(required=True, missing=True)
    url = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data):
        """ Deserialize data into an instance of Storage"""
        return Storage(**data)

    class Meta:
        ordered = True

