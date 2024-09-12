from __future__ import annotations

from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import PyFileSystem

from target_az_parquet.utils import bytes_to_mb

FIELD_TYPE_TO_PYARROW_TYPE = {
    "BOOLEAN": pa.bool_(),
    "STRING": pa.string(),
    "ARRAY": pa.string(),
    "INTEGER": pa.int64(),
    "NUMBER": pa.float64(),
    "OBJECT": pa.string(),
}
COMPRESSION_METHOD_TO_EXTENSION = {
    "snappy": ".snappy",
    "gzip": ".gz",
    "brotli": ".br",
    "zstd": ".zstd",
    "lz4": ".lz4",
}


def field_type_to_pyarrow_field(field_name: str, input_types: dict, required_fields: List[str]) -> pa.Field:
    types = input_types.get("type", [])

    # If type is not defined, check if anyOf is defined
    if not types:
        for any_type in input_types.get("anyOf", []):
            if t := any_type.get("type"):
                if isinstance(t, list):
                    types.extend(t)
                else:
                    types.append(t)

    types = [types] if isinstance(types, str) else types
    types_uppercase = [item.upper() for item in types]
    nullable = "NULL" in types_uppercase or field_name not in required_fields

    if "NULL" in types_uppercase:
        types_uppercase.remove("NULL")

    input_type = next(iter(types_uppercase)) if types_uppercase else ""
    pyarrow_type = FIELD_TYPE_TO_PYARROW_TYPE.get(input_type, pa.string())

    return pa.field(field_name, pyarrow_type, nullable)


def flatten_schema_to_pyarrow_schema(schema: dict) -> pa.Schema:
    """Function that converts a flatten schema to a pyarrow schema in a defined order.

    E.g:
     dictionary = {
        'properties': {
             'key_1': {'type': ['null', 'integer']},
             'key_2__key_3': {'type': ['null', 'string']},
             'key_2__key_4__key_5': {'type': ['null', 'integer']},
             'key_2__key_4__key_6': {'type': ['null', 'array']}
           }
        }
    By calling the function with the dictionary above as parameter,
    you will get the following structure:
        pa.schema([
             pa.field('key_1', pa.int64()),
             pa.field('key_2__key_3', pa.string()),
             pa.field('key_2__key_4__key_5', pa.int64()),
             pa.field('key_2__key_4__key_6', pa.string())
        ])
    """
    flatten_schema = schema.get("properties", {})
    required_fields = schema.get("required", [])

    return pa.schema(
        [
            field_type_to_pyarrow_field(
                field_name=field_name,
                input_types=field_input_types,
                required_fields=required_fields,
            )
            for field_name, field_input_types in flatten_schema.items()
        ]
    )


def create_pyarrow_table(records: List[dict], schema: pa.Schema) -> pa.Table:
    """Create a pyarrow Table from a python list of dict."""
    data = {f: [row.get(f) for row in records] for f in schema.names}

    return pa.table(data).cast(schema)


def concat_tables(records: List[dict], pyarrow_table: pa.Table, pyarrow_schema: pa.Schema) -> pa.Table:
    """Create a dataframe from records and concatenate with the existing one."""
    if not records:
        return pyarrow_table

    new_table = create_pyarrow_table(records=records, schema=pyarrow_schema)

    return pa.concat_tables([pyarrow_table, new_table]) if pyarrow_table else new_table


def persist_pyarrow_table(
    table: pa.Table,
    filesystem: PyFileSystem,
    path: str,
    compression_method: str = "gzip",
    basename_template_prefix: str | None = None,
    partition_cols: List[str] | None = None,
) -> None:
    """Write a pyarrow table to a parquet file."""
    pq.write_to_dataset(
        table,
        root_path=path,
        partition_cols=partition_cols or None,
        filesystem=filesystem,
        compression=compression_method,
        use_threads=True,
        basename_template=(
            f"{basename_template_prefix}{COMPRESSION_METHOD_TO_EXTENSION[compression_method.lower()]}.parquet"
            if basename_template_prefix
            else None
        ),
    )


def get_pyarrow_table_size(table: pa.Table) -> float:
    """Return the size of a pyarrow table in MB."""
    return bytes_to_mb(table.nbytes)
