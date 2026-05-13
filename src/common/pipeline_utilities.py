import polars as pl

from common.utilities import gen_uuid, get_now_utc

# Iceberg（メタデータCSV）の型からPolarsの型へのマッピング
ICEBERG_TO_POLARS_TYPE = {
    "int": pl.Int32,
    "long": pl.Int64,
    "double": pl.Float64,
    "float": pl.Float32,
    "string": pl.Utf8,
    "date": pl.Date,
    "timestamp": pl.Datetime,
    "boolean": pl.Boolean,
}


def add_metadata(df: pl.DataFrame, execution_id: str | None = None) -> pl.DataFrame:
    """
    Add ingestion metadata columns to a Polars DataFrame.

    This function appends the following columns to the input DataFrame:
    - ingestion_time: Current UTC timestamp
    - ingestion_date: Date part of the ingestion time
    - execution_id: A unique identifier for the execution. If not provided,
    a new UUID is generated.

    Args:
        df (pl.DataFrame): The input DataFrame to which metadata columns will be added.
        execution_id (str | None, optional): An optional execution identifier.
        Defaults to None.

    Returns:
        pl.DataFrame: A new DataFrame with additional metadata columns.
    """
    if execution_id is None:
        execution_id = gen_uuid()

    ingestion_time = get_now_utc()
    ingestion_date = ingestion_time.date()

    df = df.with_columns(
        pl.lit(ingestion_time).alias("ingestion_time"),
        pl.lit(ingestion_date).alias("ingestion_date"),
        pl.lit(execution_id).alias("execution_id"),
    )

    return df


def build_schema_exprs(schema_csv_path: str) -> list[pl.Expr]:
    """
    Build a list of Polars expressions from a schema definition CSV.

    This function reads a schema CSV file and generates expressions that
    cast and rename columns based on the schema definition.

    Args:
        schema_csv_path (str): Path to the CSV file containing schema definitions.

    Returns:
        list[pl.Expr]:  A list of Polars expressions for column casting and renaming.
    """
    schema_df = pl.read_csv(schema_csv_path)

    exprs = []
    # Process each row in the schema definition CSV
    for row in schema_df.iter_rows(named=True):
        src_col = row["source_name"]
        tgt_col = row["name"]
        type_str = row["type"]

        # 条件分岐の中で直接 append するか、
        # 解析ツールが「必ず値が入る」と認識できる構造にする
        if type_str == "date":
            exprs.append(
                pl.col(src_col)
                .str.to_date(format="%Y/%m/%d", strict=True)
                .alias(tgt_col)
            )
        elif type_str == "timestamp":
            exprs.append(pl.col(src_col).str.to_datetime(strict=True).alias(tgt_col))
        else:
            # それ以外の型（int, double, string等）
            pl_type = ICEBERG_TO_POLARS_TYPE.get(type_str, pl.Utf8)
            exprs.append(pl.col(src_col).cast(pl_type, strict=False).alias(tgt_col))

    return exprs
