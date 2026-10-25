from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # Memungkinkan pemeriksaan path sebelum dependensi dipasang.
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if load_dotenv is not None:
    load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class ProjectPaths:
    project_root: Path
    dataset_root: Path
    clean_dir: Path
    contaminated_dir: Path
    source_dir: Path
    output_dir: Path
    parquet_dir: Path
    charts_dir: Path
    validation_dir: Path


def get_paths(variant: str | None = None) -> ProjectPaths:
    default_dataset_root = PROJECT_ROOT.parent / "Synthetic Retail Dataset - 10 Million Transactions"
    dataset_root = Path(os.getenv("DATASET_ROOT", str(default_dataset_root))).expanduser()
    if not dataset_root.is_absolute():
        dataset_root = (PROJECT_ROOT / dataset_root).resolve()

    selected_variant = variant or os.getenv("DATASET_VARIANT", "retail_contaminated_dataset")
    valid_variants = {"retail_clean_dataset", "retail_contaminated_dataset"}
    if selected_variant not in valid_variants:
        raise ValueError(f"DATASET_VARIANT harus salah satu dari {sorted(valid_variants)}")

    output_dir = Path(os.getenv("OUTPUT_DIR", str(PROJECT_ROOT / "output"))).expanduser()
    if not output_dir.is_absolute():
        output_dir = (PROJECT_ROOT / output_dir).resolve()
    return ProjectPaths(
        project_root=PROJECT_ROOT,
        dataset_root=dataset_root,
        clean_dir=dataset_root / "retail_clean_dataset",
        contaminated_dir=dataset_root / "retail_contaminated_dataset",
        source_dir=dataset_root / selected_variant,
        output_dir=output_dir,
        parquet_dir=output_dir / "parquet" / "sales_enriched",
        charts_dir=output_dir / "charts",
        validation_dir=output_dir / "validation",
    )


def validate_source_files(paths: ProjectPaths) -> None:
    required = {
        "sales_transactions.csv",
        "store_master.csv",
        "sku_master.csv",
        "customer_master.csv",
        "promotions.csv",
        "inventory_snapshot.csv",
        "sku_inventory_flags.csv",
    }
    missing = sorted(name for name in required if not (paths.source_dir / name).exists())
    if missing:
        raise FileNotFoundError(
            f"File dataset tidak ditemukan di {paths.source_dir}: {', '.join(missing)}"
        )

