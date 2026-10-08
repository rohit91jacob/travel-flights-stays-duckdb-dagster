"""Read landed CSV files as text with a stable per-file row number."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from pyarrow import csv

from travel_pipeline.ingestion.schemas import Column, validate_header


def read_text_csv(
    path: Path, columns: tuple[Column, ...], null_values: tuple[str, ...]
) -> pa.Table:
    """Return the file as an all-string Arrow table plus ``source_row_number`` (1-based).

    pyarrow keeps file order, so the row number is a stable identity for rows that have no
    natural key (both the hotel and the flight files contain exact duplicate rows). Quoted
    newlines are allowed because the flight scrape embeds them in the ``stop`` column.
    """
    table = csv.read_csv(
        path,
        parse_options=csv.ParseOptions(newlines_in_values=True),
        convert_options=csv.ConvertOptions(
            column_types={c.source: pa.string() for c in columns},
            strings_can_be_null=bool(null_values),
            null_values=list(null_values),
        ),
    )
    validate_header(table.column_names, columns, file_name=path.name)
    table = table.rename_columns([c.target for c in columns])
    row_numbers = pa.array(range(1, table.num_rows + 1), type=pa.int64())
    return table.append_column("source_row_number", row_numbers)
