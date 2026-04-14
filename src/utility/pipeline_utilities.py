import polars as pl

from utility.utilities import gen_uuid, get_now_utc


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
