# -*- coding: utf-8 -*-

from copy import deepcopy
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf
from models import *


def PartialSchemaFactory(schema_cls):
    schema = schema_cls(partial=True)
    for field_name, field in schema.fields.items():
        if isinstance(field, fields.Nested):
            new_field = deepcopy(field)
            new_field.schema.partial = True
            schema.fields[field_name] = new_field
    return schema

# region Protected\s*
# endregion\w*


class AttributeListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.__dict__.keys())])
    size = fields.Integer(required=False)
    precision = fields.Integer(required=False)
    nullable = fields.Boolean(required=True)
    enumeration = fields.Boolean(required=True)
    missing_representation = fields.String(required=False)
    feature = fields.Boolean(required=True, missing=True,
                             default=True)
    label = fields.Boolean(required=True, missing=True,
                           default=True)
    distinct_values = fields.Integer(required=False)
    mean_value = fields.Float(required=False)
    median_value = fields.String(required=False)
    max_value = fields.String(required=False)
    min_value = fields.String(required=False)
    std_deviation = fields.Float(required=False)
    missing_total = fields.String(required=False)
    deciles = fields.String(required=False)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Attribute"""
        return Attribute(**data)


class AttributeItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.__dict__.keys())])
    size = fields.Integer(required=False)
    precision = fields.Integer(required=False)
    nullable = fields.Boolean(required=True)
    enumeration = fields.Boolean(required=True)
    missing_representation = fields.String(required=False)
    feature = fields.Boolean(required=True, missing=True,
                             default=True)
    label = fields.Boolean(required=True, missing=True,
                           default=True)
    distinct_values = fields.Integer(required=False)
    mean_value = fields.Float(required=False)
    median_value = fields.String(required=False)
    max_value = fields.String(required=False)
    min_value = fields.String(required=False)
    std_deviation = fields.Float(required=False)
    missing_total = fields.String(required=False)
    deciles = fields.String(required=False)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Attribute"""
        return Attribute(**data)


class AttributeCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer()
    name = fields.String(required=True)
    description = fields.String(required=False)
    type = fields.String(required=True,
                         validate=[OneOf(DataType.__dict__.keys())])
    size = fields.Integer(required=False)
    precision = fields.Integer(required=False)
    nullable = fields.Boolean(required=True)
    enumeration = fields.Boolean(required=True)
    missing_representation = fields.String(required=False)
    feature = fields.Boolean(required=True, missing=True,
                             default=True)
    label = fields.Boolean(required=True, missing=True,
                           default=True)
    distinct_values = fields.Integer(required=False)
    mean_value = fields.Float(required=False)
    median_value = fields.String(required=False)
    max_value = fields.String(required=False)
    min_value = fields.String(required=False)
    std_deviation = fields.Float(required=False)
    missing_total = fields.String(required=False)
    deciles = fields.String(required=False)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Attribute"""
        return Attribute(**data)


class DataSourceExecuteRequestSchema(Schema):
    """ JSON schema for executing tasks """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of DataSource"""
        return DataSource(**data)


class DataSourceListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False)
    enabled = fields.Boolean(required=True, missing=True,
                             default=True)
    read_only = fields.Boolean(required=True, missing=True,
                               default=True)
    url = fields.String(required=True)
    created = fields.DateTime(required=True, missing=func.now(),
                             default=func.now())
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False)
    estimated_rows = fields.Integer(required=False)
    estimated_size_in_mega_bytes = fields.Decimal(required=False)
    expiration = fields.String(required=False)
    user_id = fields.Integer(required=False)
    user_login = fields.String(required=False)
    user_name = fields.String(required=False)
    tags = fields.String(required=False)
    temporary = fields.Boolean(required=True, missing=False,
                               default=False)
    workflow_id = fields.Integer(required=False)
    task_id = fields.Integer(required=False)
    attributes = fields.Nested('schema.AttributeListResponseSchema',
                               required=True,
                               many=True)
    storage = fields.Nested('schema.StorageListResponseSchema',
                            required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of DataSource"""
        return DataSource(**data)


class DataSourceCreateRequestSchema(Schema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    description = fields.String(required=False)
    enabled = fields.Boolean(required=True, missing=True,
                             default=True)
    read_only = fields.Boolean(required=True, missing=True,
                               default=True)
    url = fields.String(required=True)
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False)
    expiration = fields.String(required=False)
    user_id = fields.Integer(required=False)
    user_login = fields.String(required=False)
    user_name = fields.String(required=False)
    tags = fields.String(required=False)
    temporary = fields.Boolean(required=True, missing=False,
                               default=False)
    workflow_id = fields.Integer(required=False)
    task_id = fields.Integer(required=False)
    attributes = fields.Nested('schema.AttributeCreateRequestSchema',
                               required=True,
                               many=True)
    storage_id = fields.Integer(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of DataSource"""
        return DataSource(**data)


class DataSourceItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False)
    enabled = fields.Boolean(required=True, missing=True,
                             default=True)
    read_only = fields.Boolean(required=True, missing=True,
                               default=True)
    url = fields.String(required=True)
    created = fields.DateTime(required=True, missing=func.now(),
                             default=func.now())
    format = fields.String(required=True,
                           validate=[OneOf(DataSourceFormat.__dict__.keys())])
    provenience = fields.String(required=False)
    estimated_rows = fields.Integer(required=False)
    estimated_size_in_mega_bytes = fields.Decimal(required=False)
    expiration = fields.String(required=False)
    user_id = fields.Integer(required=False)
    user_login = fields.String(required=False)
    user_name = fields.String(required=False)
    tags = fields.String(required=False)
    temporary = fields.Boolean(required=True, missing=False,
                               default=False)
    workflow_id = fields.Integer(required=False)
    task_id = fields.Integer(required=False)
    attributes = fields.Nested('schema.AttributeItemResponseSchema',
                               required=True,
                               many=True)
    storage = fields.Nested('schema.StorageItemResponseSchema',
                            required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of DataSource"""
        return DataSource(**data)


class StorageListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.__dict__.keys())])

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Storage"""
        return Storage(**data)


class StorageItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True,
                         validate=[OneOf(StorageType.__dict__.keys())])

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Storage"""
        return Storage(**data)


class StorageCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Storage"""
        return Storage(**data)
