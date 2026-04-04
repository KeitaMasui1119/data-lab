import csv
import logging
import os

from pyiceberg.schema import Schema
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
            table_schema = Schema(*column_field, identifier_field_ids=identifier_ids)

        return table_schema
    except Exception as e:
        raise ValueError(f"An unexpected error has occured. : {str(e)}") from e
