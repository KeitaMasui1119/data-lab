import csv
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


def str_to_bool(required_field: str) -> bool:
    # もしrequired_fieldがNULLの場合はFalseを返す
    if required_field is None:
        return False
    # もしrequired_fieldが""の場合はFalseを返す
    elif len(required_field) == 0:
        return False
    elif required_field.lower() == "false":
        return False
    elif required_field.lower() == "true":
        return True
    return False


def build_table_schema(file_path: str):
    # Declear type mapping dict
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
    # Declear
    column_field = []
    identifier_ids = []

    # ファイルの存在チェックをする
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File counld not found: {file_path}")
    try:
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
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
        raise ValueError(f"error has occured : {str(e)}") from e


if __name__ == "__main__":
    schema = build_table_schema(r"/workspace/data/schema/schema_spot_price.csv")
    print(schema)
