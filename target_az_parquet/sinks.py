"""AzParquet target sink class, which handles writing streams."""

from __future__ import annotations

import datetime
import os
from typing import List

import pyarrow as pa
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pyarrow.fs import PyFileSystem
from pyarrowfs_adlgen2 import AccountHandler
from singer_sdk.helpers._flattening import flatten_schema, flatten_record
from singer_sdk.sinks import BatchSink

from target_az_parquet.utils.parquet import (
    concat_tables,
    flatten_schema_to_pyarrow_schema,
    get_pyarrow_table_size,
    persist_pyarrow_table,
)


class AzParquetSink(BatchSink):
    """AzParquet target sink class."""

    FLATTEN_MAX_LEVEL = 100  # Max level of nesting to flatten

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        file_system_handler = self.__get_pyarrow_file_system_handler()

        self.__file_system = PyFileSystem(handler=file_system_handler)
        self.__pyarrow_table: pa.Table | None = None
        self.__files_saved: int = 0
        self.__destination_path: str = os.path.join(self.config.get("destination_path", "output"), self.stream_name)
        self.__flatten_schema: dict = flatten_schema(schema=self.schema, max_level=AzParquetSink.FLATTEN_MAX_LEVEL)
        self.__pyarrow_schema: pa.Schema = flatten_schema_to_pyarrow_schema(schema=self.__flatten_schema)
        self.__pyarrow_partition_cols: List[str] = (
            self.config["pyarrow_partition_cols"].split(",") if self.config.get("pyarrow_partition_cols") else None
        )

        self.__validate_arguments()

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        record_flatten = flatten_record(
            record=record,
            flattened_schema=self.__flatten_schema,
            max_level=AzParquetSink.FLATTEN_MAX_LEVEL,
        )

        super().process_record(record=record_flatten, context=context)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.logger.info(msg=f'Processing batch for {self.stream_name} with {len(context["records"])} records.')

        self.__pyarrow_table = concat_tables(
            records=context.get("records", []),
            pyarrow_table=self.__pyarrow_table,
            pyarrow_schema=self.__pyarrow_schema,
        )

        self.logger.info(
            msg=f"Pyarrow table size: {get_pyarrow_table_size(table=self.__pyarrow_table):.5f} MB | "
            f"({len(self.__pyarrow_table)} rows)"
        )

        self.__persist_pyarrow_table()

        self.logger.info(msg=f'Uploaded batch for {self.stream_name} with {len(context["records"])} records.')

    def clean_up(self) -> None:
        """Perform any clean up actions required at end of a stream."""

        self.__pyarrow_table = None

        super().clean_up()

    def __persist_pyarrow_table(self) -> None:
        if self.__pyarrow_table is not None:
            persist_pyarrow_table(
                table=self.__pyarrow_table,
                filesystem=self.__file_system,
                path=self.__destination_path,
                compression_method=self.config.get("pyarrow_compression_method", "gzip"),
                basename_template_prefix=self.__get_basename_template_prefix(),
                partition_cols=self.__pyarrow_partition_cols,
            )

            create_new_pyarrow_table: bool = (
                get_pyarrow_table_size(table=self.__pyarrow_table) > self.config["pyarrow_max_table_size_in_mb"]
            )

            if create_new_pyarrow_table:
                self.__files_saved += 1
                self.__pyarrow_table = None

    def __get_pyarrow_file_system_handler(self) -> AccountHandler:
        connection_string = self.config.get("connection_string")

        if connection_string:
            service_client = DataLakeServiceClient.from_connection_string(conn_str=connection_string)

            return AccountHandler(datalake_service=service_client)

        return AccountHandler.from_account_name(
            account_name=self.config.get("account_name"),
            credential=DefaultAzureCredential(),
        )

    def __validate_arguments(self) -> None:
        if self.__pyarrow_partition_cols:
            assert set(self.__pyarrow_partition_cols).issubset(
                set(self.__pyarrow_schema.names)
            ), "pyarrow_partition_cols must be in the schema"

    def __get_basename_template_prefix(self) -> str:
        timestamp = datetime.datetime.fromtimestamp(
            self.sync_started_at / 1000,
            tz=datetime.timezone.utc,
        ).strftime("%Y%m%d%H%M%S")

        return f"{self.stream_name}-{timestamp}-{self.__files_saved}-{{i}}"
