"""AzParquet target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_az_parquet.sinks import (
    AzParquetSink,
)


class TargetAzParquet(Target):
    """Sample target for AzParquet."""

    name = "target-az-parquet"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "connection_string",
            th.StringType,
            description="Azure storage account connection string "
            "(either 'connection_string' or 'account_name' should be set).",
        ),
        th.Property(
            "account_name",
            th.StringType,
            description="Azure storage account name (either 'connection_string' or 'account_name' should be set).",
        ),
        th.Property(
            "destination_path",
            th.StringType,
            description="The destination path (including container name) to the target output file.",
        ),
        th.Property(
            "pyarrow_compression_method",
            th.StringType,
            description="Compression method: snappy, zstd, brotli and gzip.",
            default="gzip",
        ),
        th.Property(
            "pyarrow_max_table_size_in_mb",
            th.IntegerType,
            description="Max size of pyarrow table in MB (before writing to parquet file). "
            "Controls memory usage of the target process.",
            default=800,
        ),
        th.Property(
            "pyarrow_partition_cols",
            th.StringType,
            description="Column names by which to partition. Columns are partitioned in the order they are given.",
        ),
    ).to_dict()

    default_sink_class = AzParquetSink


if __name__ == "__main__":
    TargetAzParquet.cli()
