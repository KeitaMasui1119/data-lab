"""Shared PyIceberg catalog and table management helpers."""

from __future__ import annotations

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema

from common.logging_utils import get_logger
from common.schema_builder import build_partition_spec, build_table_schema

logger = get_logger(__name__)


def get_catalog(catalog_name: str) -> Catalog:
    """Load and return a PyIceberg catalog by name."""
    try:
        catalog = load_catalog(catalog_name)
        logger.info("Successfully loaded catalog: '%s'", catalog_name)
        return catalog
    except Exception as error:  # noqa: BLE001
        raise ValueError(f"Failed to load catalog '{catalog_name}': {error}") from error


def build_namespace(catalog: Catalog, namespace: str) -> None:
    """Create a namespace if it does not exist."""
    try:
        catalog.create_namespace_if_not_exists(namespace)
        logger.info("Succeeded to create namespace: %s", namespace)
    except Exception as error:  # noqa: BLE001
        logger.error("Failed to create namespace: %s", error)


def delete_namespace(catalog: Catalog, namespace: str) -> None:
    """Delete a namespace."""
    try:
        catalog.drop_namespace(namespace)
        logger.info("Succeeded to delete namespace: %s", namespace)
    except Exception as error:  # noqa: BLE001
        logger.error("Failed to delete namespace: %s", error)


def view_namespace(catalog: Catalog, namespace: str):
    """List namespaces under the given namespace root."""
    try:
        namespaces = catalog.list_namespaces(namespace)
        logger.info("Fetched %s namespaces.", len(namespaces))
        for ns in namespaces:
            logger.info(" - %s", ns)
        return namespaces
    except Exception as error:  # noqa: BLE001
        logger.error("Failed to fetch namespaces: %s", error)
        return []


def delete_table(catalog: Catalog, table_name: str, purge: bool = False) -> None:
    """Drop or purge an Iceberg table."""
    try:
        if purge:
            catalog.purge_table(table_name)
            logger.info("Succeeded to purge table: %s", table_name)
        else:
            catalog.drop_table(table_name)
            logger.info("Succeeded to drop table: %s", table_name)
    except NoSuchTableError:
        logger.warning("Table does not exist: %s", table_name)
    except Exception as error:  # noqa: BLE001
        logger.error("Failed to drop table: %s", error)


def evolve_partition_spec(
    catalog: Catalog, identifier: str, schema_csv_path: str
) -> list[str]:
    """Add any partition fields the schema CSV declares but the live table lacks.

    provision_table() only applies partition_transform to brand-new tables and
    just warns on drift for existing ones (see docs/tasks/tasks.md §8.1),
    because adding a partition field is a metadata-only change: existing data
    files stay in their old (often unpartitioned) layout, and only files
    written after this call follow the new spec. Repartitioning historical
    data still requires rewriting it, e.g. a full-refresh silver run.

    Returns the list of column names newly added to the partition spec.
    """
    table = catalog.load_table(identifier)
    target_schema = build_table_schema(schema_csv_path)
    target_spec = build_partition_spec(schema_csv_path, target_schema)

    existing_source_ids = {field.source_id for field in table.spec().fields}

    added: list[str] = []
    for target_field in target_spec.fields:
        if target_field.source_id in existing_source_ids:
            continue
        column_name = target_schema.find_field(target_field.source_id).name
        with table.update_spec() as update:
            update.add_field(column_name, target_field.transform)
        table = catalog.load_table(identifier)
        added.append(column_name)

    if added:
        logger.info("Evolved partition spec for '%s': added %s", identifier, added)
    else:
        logger.info("Partition spec for '%s' already matches schema CSV", identifier)

    return added


def provision_table(catalog: Catalog, identifier: str, schema_csv_path: str) -> None:
    """Create or evolve an Iceberg table based on a schema CSV."""
    parts = identifier.split(".")
    namespace_parts = parts[:-1]
    for i in range(1, len(namespace_parts) + 1):
        ns = ".".join(namespace_parts[:i])
        build_namespace(catalog, ns)

    new_schema = build_table_schema(schema_csv_path)
    if new_schema is None:
        raise ValueError(f"Failed to generate schema. Check CSV: {schema_csv_path}")
    if not isinstance(new_schema, Schema):
        raise TypeError(
            f"build_table_schema returned non-Schema type: {type(new_schema)}"
        )

    new_partition_spec = build_partition_spec(schema_csv_path, new_schema)

    try:
        existing_table = catalog.load_table(identifier)
        logger.info("Table '%s' exists. Checking schema diff.", identifier)

        existing_schema = existing_table.schema()
        existing_col_names = {field.name for field in existing_schema.fields}
        new_col_names = {field.name for field in new_schema.fields}

        added_cols = new_col_names - existing_col_names
        removed_cols = existing_col_names - new_col_names

        if removed_cols:
            logger.warning(
                "Columns may be removed or modified in CSV: %s",
                sorted(removed_cols),
            )

        if added_cols:
            with existing_table.update_schema() as update:
                for col_name in added_cols:
                    new_field = new_schema.find_field(col_name)
                    update.add_column(
                        path=new_field.name,
                        field_type=new_field.field_type,
                        doc=new_field.doc,
                    )
            logger.info("Added new columns: %s", sorted(added_cols))
        else:
            logger.info("No schema changes detected")

        # Compare fields only, not the full spec (which includes spec_id):
        # evolve_partition_spec() commits via update_spec(), which bumps
        # spec_id even when the resulting fields match the CSV exactly, so a
        # spec_id-inclusive comparison would warn on every write forever.
        if existing_table.spec().fields != new_partition_spec.fields:
            logger.warning(
                "Partition spec drift for '%s': schema CSV wants %s but table "
                "has %s. provision_table() does not evolve partition specs "
                "automatically; see docs/tasks/tasks.md section 8.1.",
                identifier,
                new_partition_spec,
                existing_table.spec(),
            )
    except NoSuchTableError:
        logger.info("Table '%s' does not exist. Creating.", identifier)
        create_kwargs = {"identifier": identifier, "schema": new_schema}
        if not new_partition_spec.is_unpartitioned():
            create_kwargs["partition_spec"] = new_partition_spec
        catalog.create_table(**create_kwargs)
        logger.info("Table '%s' created successfully.", identifier)
