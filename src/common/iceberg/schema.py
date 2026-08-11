"""Build PyIceberg schemas and partition specs from the schema CSV files.

`configuration/iceberg/schema/` is the source of truth for every table's
columns; this module is the only place that parses those CSVs into PyIceberg
types, and it injects the shared audit columns (field_id 9001-9005) that
every table carries.
"""

import csv
import logging
import os

from pyiceberg.partitioning import (
    PARTITION_FIELD_ID_START,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.transforms import parse_transform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

logger = logging.getLogger(__name__)


def build_partition_spec(file_path: str, schema: Schema) -> PartitionSpec:
    """Builds a PartitionSpec from the partition_transform column of a schema CSV.

    Rows with an empty partition_transform are skipped. PyIceberg's own
    create_table() does not validate that a transform is compatible with its
    source column's type (for example month() on a string column is accepted
    silently and produces a spec that breaks at write time), so this
    function checks compatibility itself and raises ValueError instead.

    Args:
        file_path (str): Path to the CSV file containing schema definitions.
        schema (Schema): The Schema already built from the same CSV via
            build_table_schema(), used to look up each column's Iceberg type.

    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        ValueError: If a partition_transform value is not a recognized
            transform, or is incompatible with its column's type.

    Returns:
        PartitionSpec: Unpartitioned (empty) if no row declares a transform.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File counld not found: {file_path}")

    partition_fields = []
    with open(file_path) as f:
        reader = csv.DictReader(f)
        for row_n, row in enumerate(reader, start=1):
            if not any(val.strip() for val in row.values() if val is not None):
                continue

            transform_str = (row.get("partition_transform") or "").strip()
            if not transform_str:
                continue

            transform = parse_transform(transform_str)
            source_id = int(row["field_id"])
            field = schema.find_field(source_id)
            if not transform.can_transform(field.field_type):
                raise ValueError(
                    f"Invalid partition_transform '{transform_str}' for column "
                    f"'{row['name']}' (type={field.field_type}) in "
                    f"{file_path}:{row_n}"
                )

            partition_fields.append(
                PartitionField(
                    source_id=source_id,
                    field_id=PARTITION_FIELD_ID_START + len(partition_fields),
                    transform=transform,
                    name=f"{row['name']}_{transform_str.split('[')[0]}",
                )
            )

    return PartitionSpec(*partition_fields)


def str_to_bool(required_field: str) -> bool:
    """
    Convert a string value to a boolean based on strict rules.

    This function interprets the input string as follows:
    - Returns False if the input is None, an empty string, or "false" (case-insensitive)
    - Returns True only if the input is "true" (case-insensitive)
    - Returns False for all other values

    Args:
        required_field (str): The input string to evaluate. Can be None.

    Returns:
        bool: The evaluated boolean value based on the defined rules.

    Notes:
        This is a strict parser and does not handle values such as "1", "yes",
        or other truthy representations.
    """
    # If required_field is null or the lenght of required field is 0 then returns False.
    if required_field is None or len(required_field) == 0:
        return False
    elif required_field.lower() == "false":
        return False
    elif required_field.lower() == "true":
        return True
    # If any other inputs are entered then returns False.
    return False


def build_table_schema(file_path: str):
    """Builds an Iceberg Schema object from a CSV definition file.

    Args:
        file_path (str): Path to the CSV file containing schema definitions.

    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        ValueError: If key columns are missed.
        ValueError: If unsupported data type has been inputed.
        ValueError: Other general erros has occurred.

    Returns:
        _type_: A PyIceberg Schema object.
    """

    TYPE_MAP = {
        "boolean": BooleanType(),
        "date": DateType(),
        "decimal": DecimalType(32, 3),
        "double": DoubleType(),
        "float": FloatType(),
        "int": IntegerType(),
        "long": LongType(),
        "string": StringType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
    }

    column_field = []
    identifier_ids = []

    if not os.path.exists(file_path):
        logger.error(f"File could not found. : {file_path}")
        raise FileNotFoundError(f"File counld not found: {file_path}")
    try:
        logger.info(f"Start loading the definition of schema. : {file_path}")
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row_n, row in enumerate(reader, start=1):
                if not any(val.strip() for val in row.values() if val is not None):
                    logger.warning(f"Skipping empty row detected at line {row_n}")
                    continue

                required_keys = ["field_id", "name", "type"]
                for key in required_keys:
                    if not row.get(key) or str(row[key]).strip() == "":
                        raise ValueError(
                            f"Mandatory column '{key}' is missing or empty."
                            f"Line: {row_n}."
                            f"Row: {row}"
                        )
                # 定義されていない不正なデータ型のチェック
                type_str = row["type"].strip().lower()
                if type_str not in TYPE_MAP:
                    raise ValueError(
                        f"Unsupported data type '{type_str}' at line {row_n}."
                        f"(Column: {row['name']})"
                    )

                field = NestedField(
                    field_id=int(row["field_id"]),
                    name=row["name"],
                    field_type=TYPE_MAP[row["type"]],
                    required=str_to_bool(row["required"]),
                    doc=row["doc"],
                )
                if str_to_bool(row["is_identifier"]):
                    identifier_ids.append(int(row["field_id"]))

                column_field.append(field)
            # Metadata schema
            # --- ここからシステムロジック（監査カラムの強制注入） ---
            audit_fields = [
                NestedField(
                    field_id=9001,
                    name="source_data",
                    field_type=StringType(),
                    required=False,
                    doc="Source data path or URL",
                ),
                NestedField(
                    field_id=9002,
                    name="status",
                    field_type=StringType(),
                    required=False,
                    doc="new or updated",
                ),
                NestedField(
                    field_id=9003,
                    name="ingestion_time",
                    field_type=TimestamptzType(),
                    required=False,
                    doc="Data insertion timestamp",
                ),
                NestedField(
                    field_id=9004,
                    name="ingestion_date",
                    field_type=DateType(),
                    required=False,
                    doc="Data insertion date",
                ),
                NestedField(
                    field_id=9005,
                    name="execution_id",
                    field_type=StringType(),
                    required=False,
                    doc="Pipeline Unique Run ID",
                ),
            ]

            column_field.extend(audit_fields)

            table_schema = Schema(*column_field, identifier_field_ids=identifier_ids)

        return table_schema
    except Exception as e:
        raise RuntimeError(f"An unexpected error has occurred. : {str(e)}") from e
