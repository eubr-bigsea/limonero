# -*- coding: utf-8 -*-
import datetime
import json
from copy import deepcopy
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf
from models import *


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
    except:
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
    nullable = fields.Boolean(required=True)
    enumeration = fields.Boolean(required=True)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(required=True, missing=True,
                             default=True)
    label = fields.Boolean(required=True, missing=True,
                           default=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)

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
    nullable = fields.Boolean(required=True)
    enumeration = fields.Boolean(required=True)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(required=True, missing=True,
                             default=True)
    label = fields.Boolean(required=True, missing=True,
                           default=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)

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
    nullable = fields.Boolean(required=True)
    enumeration = fields.Boolean(required=True)
    missing_representation = fields.String(required=False, allow_none=True)
    feature = fields.Boolean(required=True, missing=True,
                             default=True)
    label = fields.Boolean(required=True, missing=True,
                           default=True)
    distinct_values = fields.Integer(required=False, allow_none=True)
    mean_value = fields.Float(required=False, allow_none=True)
    median_value = fields.String(required=False, allow_none=True)
    max_value = fields.String(required=False, allow_none=True)
    min_value = fields.String(required=False, allow_none=True)
    std_deviation = fields.Float(required=False, allow_none=True)
    missing_total = fields.String(required=False, allow_none=True)
    deciles = fields.String(required=False, allow_none=True)

    class Meta:
        ordered = True


class DataSourceExecuteRequestSchema(Schema):
    """ JSON schema for executing tasks """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    class Meta:
        ordered = True


class DataSourceListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(required=True, missing=True,
                             default=True)
    statistics_process_counter = fields.Integer(required=True, missing=0,
                                                default=0)
    read_only = fields.Boolean(required=True, missing=True,
                               default=True)
    privacy_aware = fields.Boolean(required=True, missing=False,
                                   default=False)
    url = fields.String(required=True)
    created = fields.DateTime(required=True, missing=func.now(),
                              default=func.now())
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False, allow_none=True)
    estimated_rows = fields.Integer(required=False, allow_none=True)
    estimated_size_in_mega_bytes = fields.Decimal(
        required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(required=True, missing=False,
                               default=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attributes = fields.Nested(
        'limonero.schema.AttributeListResponseSchema',
        required=True,
        many=True)
    storage = fields.Nested(
        'limonero.schema.StorageListResponseSchema',
        required=True)
    privacy_risks = fields.Nested(
        'limonero.schema.PrivacyRiskListResponseSchema',
        required=True,
        many=True)

    class Meta:
        ordered = True


class DataSourceCreateRequestSchema(Schema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(required=True, missing=True,
                             default=True)
    statistics_process_counter = fields.Integer(required=True, missing=0,
                                                default=0)
    read_only = fields.Boolean(required=True, missing=True,
                               default=True)
    privacy_aware = fields.Boolean(required=True, missing=False,
                                   default=False)
    url = fields.String(required=True)
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(required=True, missing=False,
                               default=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attributes = fields.Nested(
        'limonero.schema.AttributeCreateRequestSchema',
        required=True,
        many=True)
    storage_id = fields.Integer(required=True)
    privacy_risks = fields.Nested(
        'limonero.schema.PrivacyRiskCreateRequestSchema',
        required=True,
        many=True)

    class Meta:
        ordered = True


class DataSourceItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(required=True, missing=True,
                             default=True)
    statistics_process_counter = fields.Integer(required=True, missing=0,
                                                default=0)
    read_only = fields.Boolean(required=True, missing=True,
                               default=True)
    privacy_aware = fields.Boolean(required=True, missing=False,
                                   default=False)
    url = fields.String(required=True)
    created = fields.DateTime(required=True, missing=func.now(),
                              default=func.now())
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False, allow_none=True)
    estimated_rows = fields.Integer(required=False, allow_none=True)
    estimated_size_in_mega_bytes = fields.Decimal(
        required=False, allow_none=True)
    expiration = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=False, allow_none=True)
    user_login = fields.String(required=False, allow_none=True)
    user_name = fields.String(required=False, allow_none=True)
    tags = fields.String(required=False, allow_none=True)
    temporary = fields.Boolean(required=True, missing=False,
                               default=False)
    workflow_id = fields.Integer(required=False, allow_none=True)
    task_id = fields.String(required=False, allow_none=True)
    attributes = fields.Nested(
        'limonero.schema.AttributeItemResponseSchema',
        required=True,
        many=True)
    storage = fields.Nested(
        'limonero.schema.StorageItemResponseSchema',
        required=True)
    privacy_risks = fields.Nested(
        'limonero.schema.PrivacyRiskItemResponseSchema',
        required=True,
        many=True)

    class Meta:
        ordered = True


class StorageListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.__dict__.keys())])
    url = fields.String(required=True)

    class Meta:
        ordered = True


class StorageItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.__dict__.keys())])
    url = fields.String(required=True)

    class Meta:
        ordered = True


class StorageCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    url = fields.String(required=True)

    class Meta:
        ordered = True

