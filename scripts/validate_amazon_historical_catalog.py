#!/usr/bin/env python3
"""Read-only preflight validation for the Amazon 2023 historical catalog."""
import argparse
import gzip
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root", type=Path,
        default=Path(os.environ.get("PIPELINE_DATA_ROOT", "/workspace/data")),
    )
    args = parser.parse_args()
    gzip_root = args.root / "raw" / "historical" / "amazon_2023"
    gzip_files = sorted(gzip_root.glob("meta_*.jsonl.gz"))
    if gzip_files:
        for source in gzip_files:
            with source.open("rb") as stream:
                if stream.read(2) != b"\x1f\x8b":
                    raise RuntimeError(f"{source}: not gzip")
            # Reading one line verifies decompression without materializing data.
            with gzip.open(source, "rt", encoding="utf-8") as stream:
                if not stream.readline().strip():
                    raise RuntimeError(f"{source}: empty metadata file")
            print(f"VALID_HISTORICAL_CATALOG_GZIP: {source}", flush=True)
    else:
        parquet_root = args.root / "raw" / "historical" / "amazon_2023_parquet"
        parquet_files = list(parquet_root.rglob("*.parquet")) if parquet_root.is_dir() else []
        if not parquet_files:
            raise RuntimeError(
                "No Amazon 2023 gzip metadata or normalized Parquet source found "
                f"under {args.root / 'raw' / 'historical'}"
            )
        print(f"VALID_HISTORICAL_CATALOG_PARQUET_FILES: {len(parquet_files)}", flush=True)
    print("NO_PRICE_FACT_OR_OBSERVATION_DATE_CREATED", flush=True)


if __name__ == "__main__":
    main()
