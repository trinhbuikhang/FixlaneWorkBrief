#!/usr/bin/env python3
"""
Gộp mọi CSV trong thư mục (giữ header file đầu, không trùng) rồi clean một lần.
Nhanh hơn merge_and_clean_folder khi có nhiều file vì:
- Gộp chỉ copy byte, không parse CSV.
- Clean chỉ chạy 1 lần trên 1 file.

Cách chạy:
  python scripts/merge_then_clean.py "C:\path\to\folder" [output_cleaned.csv]
"""

import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def merge_csv_fast(folder: Path, combined_path: Path) -> None:
    """Gộp CSV: file đầu ghi cả (header+data), các file sau chỉ append từ dòng 2. Pure I/O, rất nhanh."""
    csvs = sorted(folder.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV in {folder}")
    with open(combined_path, "wb") as out:
        for i, f in enumerate(csvs):
            with open(f, "rb") as inf:
                if i == 0:
                    out.write(inf.read())
                else:
                    inf.readline()  # bỏ header
                    out.write(inf.read())


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/merge_then_clean.py <folder> [output_cleaned.csv]")
        sys.exit(1)
    folder = Path(sys.argv[1]).resolve()
    if not folder.is_dir():
        print(f"Not a directory: {folder}")
        sys.exit(1)
    output_name = sys.argv[2] if len(sys.argv) > 2 else "combined_cleaned.csv"
    output_path = folder / output_name
    combined_path = folder / "combined_output.csv"

    print("Step 1: Merging CSVs (byte copy, no parse)...")
    merge_csv_fast(folder, combined_path)
    size_gb = combined_path.stat().st_size / (1024 ** 3)
    print(f"  Merged -> {combined_path.name} ({size_gb:.2f} GB)")

    print("Step 2: Cleaning (filter + dedup)...")
    from utils.data_processor import process_data

    def progress(msg, percent=None):
        if percent is not None:
            print(f"  [{percent}%] {msg}")
        else:
            print(f"  {msg}")

    process_data(str(combined_path), str(output_path), progress_callback=progress)
    print(f"  Done -> {output_path}")


if __name__ == "__main__":
    main()
