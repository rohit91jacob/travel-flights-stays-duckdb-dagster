"""Render docs/data_dictionary.md from the dbt YAML (the single source of column docs).

uv run python scripts/generate_data_dictionary.py
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MODELS = ROOT / "dbt" / "models"
OUT = ROOT / "docs" / "data_dictionary.md"
LAYERS = [
    ("staging", "Staging (views)"),
    ("intermediate", "Intermediate (tables)"),
    ("marts/core", "Marts: conformed dimensions"),
    ("marts/flights", "Marts: flights"),
    ("marts/stays", "Marts: stays (hotel booking demand)"),
    ("marts/listings", "Marts: stays (India listings)"),
    ("marts/cross", "Marts: cross-subject"),
]


def _clean(text: str | None) -> str:
    return " ".join((text or "").split()).replace("|", "\\|")


def _models(folder: Path) -> list[dict]:
    models = []
    for path in sorted(folder.rglob("*.yml")):
        models.extend(yaml.safe_load(path.read_text(encoding="utf-8")).get("models", []))
    return sorted(models, key=lambda m: m["name"])


def main() -> None:
    lines = [
        "# Data dictionary",
        "",
        "Generated from the dbt YAML by `scripts/generate_data_dictionary.py`; do not edit by hand.",
        "Mart column types are enforced by dbt model contracts.",
        "",
    ]
    for folder, title in LAYERS:
        models = _models(MODELS / folder)
        if not models:
            continue
        lines += [f"## {title}", ""]
        for model in models:
            lines += [f"### `{model['name']}`", "", _clean(model.get("description")), ""]
            columns = model.get("columns", [])
            if columns:
                lines += ["| Column | Type | Description |", "| --- | --- | --- |"]
                lines += [
                    f"| `{c['name']}` | {c.get('data_type', '')} | {_clean(c.get('description'))} |"
                    for c in columns
                ]
                lines.append("")
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"wrote {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
