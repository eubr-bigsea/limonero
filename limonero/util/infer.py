import re
import typing
from urllib.parse import ParseResult

import pymysql
import sqlglot
import sqlglot.expressions

from limonero.models import Attribute, DataSource, DataType
from limonero.util.jdbc import get_mysql_data_type

MYSQL_FIELD_TYPE_MAP = {
    v: k
    for k, v in pymysql.constants.FIELD_TYPE.__dict__.items()
    if not k.startswith("_")
}


def extract_table_info(sql: str) -> typing.List[typing.Tuple[str, str]]:
    parsed = sqlglot.parse_one(sql)
    tables = set()
    for tb in parsed.find_all(sqlglot.expressions.Table):
        schema = tb.db or None
        table = tb.name
        tables.add((schema, table))

    return tables


def get_column_info(connection, tables):
    column_info = {}
    with connection.cursor() as cursor:
        for schema, table in tables:
            cursor.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                       CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
                       IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
                (schema, table),
            )
            columns = cursor.fetchall()
            for col in columns:
                key = (col[0], col[1])  # (schema, table)
                if key not in column_info:
                    column_info[key] = []
                column_info[key].append(col[2:])  # Skip schema and table name
    return column_info


def get_query_column_types(
    connection: any, sql: str
) -> typing.List[typing.Dict[str, str]]:
    # Extract table info from the SQL using sqlglot
    tables = extract_table_info(sql)

    # Get detailed column info from INFORMATION_SCHEMA
    column_info = get_column_info(connection, tables)

    # Combine PyMySQL types with INFORMATION_SCHEMA data
    combined_info = []
    for (schema, table), columns in column_info.items():
        for col_info in columns:
            combined_info.append(
                {
                    "name": col_info[0],
                    "schema": schema,
                    "table": table,
                    "sql_type": col_info[1],  # DATA_TYPE
                    "max_length": col_info[2],  # CHARACTER_MAXIMUM_LENGTH
                    "numeric_precision": col_info[3],  # NUMERIC_PRECISION
                    "numeric_scale": col_info[4],  # NUMERIC_SCALE
                    "is_nullable": col_info[5] == "YES",  # IS_NULLABLE
                }
            )

    return combined_info


def get_column_types(cursor):
    return [
        MYSQL_FIELD_TYPE_MAP.get(description[1], f"Unknown ({description[1]})")
        for description in cursor.description
    ]


def _get_mysql_data_type(sql_type):
    sql_type_upper = sql_type.upper()
    if sql_type_upper in (
        "TINY",
        "SHORT",
        "INT24",
        "YEAR",
        "BIT",
        "INT",
        "TINYINT",
        "SMALLINT",
    ):
        final_type = DataType.INTEGER
    elif sql_type_upper in ("LONGLONG", "LONG"):
        final_type = DataType.LONG
    elif sql_type_upper in ("NEWDATE",):
        final_type = DataType.DATETIME
    elif sql_type_upper in ("NEWDECIMAL",):
        final_type = DataType.DECIMAL
    elif sql_type_upper in (
        "DECIMAL",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIME",
        "DATETIME",
        "LONG",
        "TIMESTAMP",
    ):
        final_type = sql_type_upper
    else:
        final_type = DataType.CHARACTER
    return final_type


def infer_from_mysql_2(
    ds: DataSource, parsed: ParseResult, gettext: callable
) -> typing.List[Attribute]:
    if ds.command is None or ds.command.strip() == "":
        raise ValueError(
            gettext("Data source does not have a command specified")
        )
    with pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or "3306",
        user=parsed.username,
        passwd=parsed.password,
        db=parsed.path[1:],
    ) as cn:
        result = []
        for i, info in enumerate(get_query_column_types(cn, ds.command)):
            attr = Attribute(
                name=info.get("name"),
                type=_get_mysql_data_type(info.get("sql_type")),
                raw_type=info.get("sql_type"),
                size=min(info.get("max_length"), 2_147_483_647)
                if info.get("max_length")
                else None,
                precision=info.get("numeric_precision"),
                scale=info.get("numeric_scale"),
                nullable=info.get("is_nullable"),
                position=i + 1,
                data_source_id=ds.id,
            )
            attr.data_source = ds
            attr.feature = False
            attr.label = False
            result.append(attr)

    return result


def infer_from_mysql(
    ds: DataSource, parsed: ParseResult, sql: str, gettext: callable
) -> typing.List[Attribute]:
    result: typing.List[Attribute] = []
    try:
        ft = pymysql.constants.FIELD_TYPE
        d = {getattr(ft, k): k for k in dir(ft) if not k.startswith("_")}
        if sql is None or sql.strip() == "":
            raise ValueError(
                gettext("Data source does not have a command specified")
            )
        fix_limit = re.compile(r"\sLIMIT\s+(\d+)")
        cmd = fix_limit.sub("", sql)
        FIELD_TYPE_MAP = {
            v: k
            for k, v in pymysql.constants.FIELD_TYPE.__dict__.items()
            if not k.startswith("_")
        }

        with pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or "3306",
            user=parsed.username,
            passwd=parsed.password,
            db=parsed.path[1:],
        ) as cn:
            cursor = cn.cursor()
            cursor.execute("{} LIMIT 0".format(cmd))

            for i, (
                name,
                dtype,
                size,
                _,
                precision,
                scale,
                nullable,
            ) in enumerate(cursor.description):
                final_type = get_mysql_data_type(d, dtype)
                if d[dtype] == "BLOB":
                    precision = None

                attr = Attribute(
                    name=name,
                    type=final_type,
                    raw_type=FIELD_TYPE_MAP.get(dtype, "UNKNOWN"),
                    size=size,
                    precision=precision,
                    scale=scale,
                    nullable=nullable,
                    position=i + 1,
                    data_source_id=ds.id,
                )
                attr.data_source = ds
                attr.feature = False
                attr.label = False
                result.append(attr)

            return result
    except Exception:
        raise ValueError(gettext("Could not connect to database"))
