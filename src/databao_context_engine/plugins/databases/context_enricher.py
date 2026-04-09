import dataclasses
import logging
import re
from typing import Any

import yaml

from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
)
from databao_context_engine.serialization.yaml import to_yaml_string

logger = logging.getLogger(__name__)


def enrich_database_context(
    context: DatabaseIntrospectionResult, description_provider: DescriptionProvider
) -> DatabaseIntrospectionResult:
    enriched_catalogs = []
    for catalog in context.catalogs:
        enriched_catalogs.append(_get_enriched_catalog(description_provider, catalog))

    return dataclasses.replace(context, catalogs=enriched_catalogs)


def _get_enriched_catalog(description_provider: DescriptionProvider, catalog: DatabaseCatalog) -> DatabaseCatalog:
    enriched_schemas = []
    for schema in catalog.schemas:
        enriched_schemas.append(_get_enriched_schema(description_provider, catalog.name, schema))

    try:
        catalog_description = (
            _describe_catalog(description_provider, catalog) if not catalog.description else catalog.description
        )
    except Exception as e:
        logger.debug(str(e), exc_info=True, stack_info=True)
        logger.info(f"Failed to generate description for catalog {catalog.name}")

        catalog_description = catalog.description

    return dataclasses.replace(catalog, schemas=enriched_schemas, description=catalog_description)


def _get_enriched_schema(
    description_provider: DescriptionProvider, catalog_name: str, schema: DatabaseSchema
) -> DatabaseSchema:
    enriched_tables = []
    for table in schema.tables:
        enriched_tables.append(_get_enriched_table(description_provider, catalog_name, schema.name, table))

    try:
        schema_description = (
            _describe_schema(description_provider, catalog_name, schema)
            if not schema.description
            else schema.description
        )
    except Exception as e:
        logger.debug(str(e), exc_info=True, stack_info=True)
        logger.info(f"Failed to generate description for schema {catalog_name}.{schema.name}")

        schema_description = schema.description

    return dataclasses.replace(schema, tables=enriched_tables, description=schema_description)


def _get_enriched_table(
    description_provider: DescriptionProvider, catalog_name: str, schema_name: str, table: DatabaseTable
) -> DatabaseTable:
    enriched_columns = []
    for column in table.columns:
        enriched_columns.append(_get_enriched_column(description_provider, catalog_name, schema_name, table, column))

    # Critique pass: rewrite any column descriptions that don't disambiguate
    # the column from its siblings. Operates on the per-column drafts that
    # were just produced, comparing them against each other + sample values.
    try:
        enriched_columns = _critique_table_columns(
            description_provider, catalog_name, schema_name, table, enriched_columns
        )
    except Exception as e:
        logger.debug(str(e), exc_info=True, stack_info=True)
        logger.info(
            f"Critique pass failed for table {catalog_name}.{schema_name}.{table.name}; keeping draft descriptions"
        )

    try:
        table_description = (
            _describe_table(description_provider, catalog_name, schema_name, table)
            if not table.description
            else table.description
        )
    except Exception as e:
        logger.debug(str(e), exc_info=True, stack_info=True)
        logger.info(f"Failed to generate description for table {catalog_name}.{schema_name}.{table.name}")

        table_description = table.description

    return dataclasses.replace(table, columns=enriched_columns, description=table_description)


def _get_enriched_column(
    description_provider: DescriptionProvider,
    catalog_name: str,
    schema_name: str,
    table: DatabaseTable,
    column: DatabaseColumn,
) -> DatabaseColumn:
    if not column.description:
        try:
            column_description = _describe_column(description_provider, catalog_name, schema_name, table, column)
            return dataclasses.replace(column, description=column_description)
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(
                f"Failed to generate description for column {catalog_name}.{schema_name}.{table.name}.{column.name}"
            )
            return column

    return column


def _describe_catalog(description_provider: DescriptionProvider, catalog: DatabaseCatalog) -> str:
    return description_provider.describe(
        text=catalog.name,
        context=to_yaml_string(
            {
                "catalog": catalog,
                "schemas": [
                    {"name": schema.name, "tables": [table.name for table in schema.tables]}
                    for schema in catalog.schemas
                ],
            }
        ),
    )


def _describe_schema(description_provider: DescriptionProvider, catalog_name: str, schema: DatabaseSchema) -> str:
    return description_provider.describe(
        text=schema.name,
        context=to_yaml_string(
            {
                "catalog_name": catalog_name,
                "schema": {"name": schema.name, "tables": [table.name for table in schema.tables]},
            }
        ),
    )


def _describe_table(
    description_provider: DescriptionProvider, catalog_name: str, schema_name: str, table: DatabaseTable
) -> str:
    return description_provider.describe(
        text=table.name,
        context=to_yaml_string({"catalog_name": catalog_name, "schema_name": schema_name, "table": table}),
    )


def _describe_column(
    description_provider: DescriptionProvider,
    catalog_name: str,
    schema_name: str,
    table: DatabaseTable,
    column: DatabaseColumn,
) -> str:
    return description_provider.describe(
        text=to_yaml_string(column),
        context=to_yaml_string({"catalog_name": catalog_name, "schema_name": schema_name, "table": table}),
    )


# ── Critique pass ─────────────────────────────────────────────────────────────


_CRITIQUE_PROMPT = """You are refining column descriptions for a database table \
so that a SQL-writing agent can pick the correct column without guessing.

TABLE: {table_name}
SCHEMA: {schema_name}

COLUMNS (with current draft descriptions and sample values):
{columns_block}

TASKS:
  1. For any column whose CURRENT description does not clearly distinguish it
     from sibling columns in this table, rewrite its description to include:
     - How it differs from similarly-named or similarly-purposed columns
     - When a query would use this column vs an alternative
  2. If a column has a clear relationship to another column - a foreign-key
     name match, a matching prefix/suffix pair (e.g. start_date/end_date), or
     an identical sequence of sample values - mention the related column by
     name in the description. Do NOT invent relationships that are not visible
     in the column names or sample values shown above.

MANDATORY SAMPLE CITATION RULE:
  If the sample_values shown above for a column are DIFFERENT from the sample
  values of any sibling column you compare it to, you MUST quote at least one
  concrete sample value from BOTH columns inside the rewritten description
  (e.g. `name_given` holds the full given name like "David Allan", whereas
  `name_first` holds only the first name like "David").
  If you rewrite one column in a group of siblings, you must also rewrite
  every other column in that group so the disambiguation is symmetric.

Leave any column whose current description is already unambiguous untouched.
Return ONLY YAML with a single `rewrites` key, listing ONLY the columns you
changed. Every `name` must match a column from the input exactly.

Example output format:
rewrites:
  - name: <exact column name>
    description: <new description, 1-3 sentences>
  - name: <another column name>
    description: <another new description>

If no columns need rewriting, return:
rewrites: []

Return only the YAML. No preamble, no code fences, no commentary."""


def _build_critique_columns_block(
    columns: list[DatabaseColumn], samples: list[dict[str, Any]], max_sample_values: int = 5
) -> str:
    """Format columns + per-column sample values for the critique prompt."""
    lines: list[str] = []
    for col in columns:
        desc = (col.description or "").strip().replace("\n", " ")
        seen: list[str] = []
        for row in samples or []:
            val = row.get(col.name) if isinstance(row, dict) else None
            if val is None or val == "":
                continue
            sval = str(val)
            if sval not in seen:
                seen.append(sval)
            if len(seen) >= max_sample_values:
                break
        sample_str = ", ".join(seen) if seen else "(none)"
        lines.append(f"  - name: {col.name}")
        lines.append(f"    type: {col.type}")
        lines.append(f"    current_description: {desc}")
        lines.append(f"    sample_values: [{sample_str}]")
    return "\n".join(lines)


def _extract_yaml_block(raw: str) -> str:
    """Strip <think> blocks and code fences that some models add despite instructions."""
    raw = re.sub(r"<think>.*?</think>\s*", "", raw, flags=re.DOTALL)
    raw = re.sub(r"^```(?:yaml|yml)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())
    return raw.strip()


_DESC_LINE_RE = re.compile(r"^(\s*description:\s*)(.+?)\s*$")


def _quote_description_lines(yaml_text: str) -> str:
    """Wrap unquoted `description:` values in single quotes so backticks/colons
    inside the description don't break the YAML parser. Lines that already
    start with a quote (single, double) or a YAML block scalar marker (| or >)
    are left untouched."""
    out_lines: list[str] = []
    for line in yaml_text.splitlines():
        m = _DESC_LINE_RE.match(line)
        if not m:
            out_lines.append(line)
            continue
        prefix, value = m.group(1), m.group(2)
        if value[:1] in ("'", '"', "|", ">", "[", "{"):
            out_lines.append(line)
            continue
        # Escape any embedded single quotes by doubling them (YAML single-quote rule)
        escaped = value.replace("'", "''")
        out_lines.append(f"{prefix}'{escaped}'")
    return "\n".join(out_lines)


def _parse_critique_rewrites(raw: str) -> list[dict[str, Any]]:
    cleaned = _extract_yaml_block(raw)
    try:
        parsed = yaml.safe_load(cleaned)
    except yaml.YAMLError:
        # Common failure: model writes `description: ` followed by an unquoted
        # value that contains backticks, colons, or other YAML reserved chars.
        # Retry once after wrapping description values in single quotes.
        try:
            parsed = yaml.safe_load(_quote_description_lines(cleaned))
        except yaml.YAMLError as e:
            logger.warning("Critique YAML parse failed (after quoting retry): %s", e)
            return []
    if not isinstance(parsed, dict) or "rewrites" not in parsed:
        logger.warning("Critique response missing `rewrites` key")
        return []
    rewrites = parsed["rewrites"] or []
    if not isinstance(rewrites, list):
        logger.warning("Critique `rewrites` is not a list")
        return []
    return rewrites


def _critique_table_columns(
    description_provider: DescriptionProvider,
    catalog_name: str,
    schema_name: str,
    table: DatabaseTable,
    columns: list[DatabaseColumn],
) -> list[DatabaseColumn]:
    """Run a single critique pass over a table's draft column descriptions.

    Returns a new list of DatabaseColumn with any rewritten descriptions
    applied. Columns the model leaves untouched (or rewrites that fail
    validation) keep their original draft description.
    """
    if len(columns) < 2:
        # Nothing to disambiguate
        return columns

    columns_block = _build_critique_columns_block(columns, table.samples)
    prompt = _CRITIQUE_PROMPT.format(
        table_name=table.name,
        schema_name=schema_name,
        columns_block=columns_block,
    )

    raw = description_provider.prompt_for_description(prompt)
    rewrites = _parse_critique_rewrites(raw)

    if not rewrites:
        logger.info(
            f"Critique pass for {catalog_name}.{schema_name}.{table.name}: 0 rewrites"
        )
        return columns

    valid_names = {c.name for c in columns}
    name_to_new_desc: dict[str, str] = {}
    dropped = 0
    for entry in rewrites:
        if not isinstance(entry, dict):
            dropped += 1
            continue
        name = entry.get("name")
        new_desc = entry.get("description")
        if name not in valid_names:
            logger.debug(f"Critique dropped unknown column: {name!r}")
            dropped += 1
            continue
        if not isinstance(new_desc, str) or not new_desc.strip():
            dropped += 1
            continue
        name_to_new_desc[name] = new_desc.strip()

    logger.info(
        f"Critique pass for {catalog_name}.{schema_name}.{table.name}: "
        f"{len(name_to_new_desc)} rewrites applied, {dropped} dropped"
    )

    if not name_to_new_desc:
        return columns

    return [
        dataclasses.replace(c, description=name_to_new_desc[c.name]) if c.name in name_to_new_desc else c
        for c in columns
    ]
