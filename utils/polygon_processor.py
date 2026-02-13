"""
Polygon-in-polygon processor for geospatial CSV data.
Adapted from PolygonSelector (batch_core) for use in DataCleaner.
Requires: polars, shapely.
"""
import csv
import logging
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from utils.lazy_imports import polars as pl
from shapely.geometry import MultiPolygon, Point, Polygon
from shapely.prepared import prep
from shapely.wkt import loads as wkt_loads

logger = logging.getLogger(__name__)


def _get_col_by_lower(df: pl.DataFrame, name_lower: str) -> Optional[str]:
    for c in df.columns:
        if c.lower() == name_lower.lower():
            return c
    return None


def _clean_filename(name: str) -> str:
    s = "".join(c for c in str(name) if c.isalnum() or c in (" ", "-", "_"))
    return s.strip().replace(" ", "_")


def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            v = value.strip().replace(",", "")
            if v == "":
                return None
            return float(v)
        return float(value)
    except Exception:
        return None


def read_csv_as_strings(path: Path) -> pl.DataFrame:
    """Read CSV file as strings with improved error handling."""
    path_str = str(path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    if path.stat().st_size == 0:
        logger.warning("Empty file detected: %s", path)
        raise ValueError(f"Empty file: {path}")
    try:
        with open(path_str, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = next(reader)
        schema_overrides = {c: pl.Utf8 for c in header}
        df = pl.read_csv(path_str, infer_schema_length=0, schema_overrides=schema_overrides)
        logger.info("Read CSV UTF-8: %s (%s rows)", path, df.height)
        return df
    except UnicodeDecodeError:
        for encoding in ["cp1252", "iso-8859-1", "utf-16"]:
            try:
                with open(path_str, newline="", encoding=encoding) as fh:
                    reader = csv.reader(fh)
                    header = next(reader)
                schema_overrides = {c: pl.Utf8 for c in header}
                df = pl.read_csv(
                    path_str, infer_schema_length=0, schema_overrides=schema_overrides, encoding=encoding
                )
                logger.info("Read CSV %s: %s", encoding, path)
                return df
            except Exception:
                continue
    except Exception as e:
        logger.warning("Failed to read CSV as strings: %s, error: %s", path, e)
    try:
        df = pl.read_csv(path_str)
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in df.columns])
        logger.info("Read CSV default: %s", path)
        return df
    except Exception as e:
        logger.error("Cannot read CSV: %s, error: %s", path, e)
        raise ValueError(f"Cannot read CSV file: {path}. Error: {e}") from e


def validate_polygon_file(file_path: Path, full_validate: bool = False) -> pl.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Polygon file not found: {file_path}")
    df = read_csv_as_strings(file_path)
    wkt_col = _get_col_by_lower(df, "wkt")
    if wkt_col is None:
        raise ValueError("Polygon file must contain a 'WKT' column (case-insensitive)")
    id_col = _get_col_by_lower(df, "id")
    council_col = _get_col_by_lower(df, "councilname")
    if id_col is None:
        df = df.with_columns(pl.Series("id", [str(i) for i in range(1, df.height + 1)]))
        id_col = "id"
    if council_col is None:
        df = df.with_columns(pl.Series("CouncilName", [f"Polygon_{i+1}" for i in range(df.height)]))
        council_col = "CouncilName"
    validate_count = df.height if full_validate else min(3, df.height)
    for i in range(validate_count):
        wkt = df[wkt_col][i]
        try:
            geom = wkt_loads(wkt)
        except Exception as e:
            raise ValueError(f"Polygon file row {i+1}: invalid WKT - {e}") from e
        if not isinstance(geom, (Polygon, MultiPolygon)):
            raise ValueError(f"Polygon file row {i+1}: WKT must be polygon geometry")
    rename_map = {}
    if wkt_col != "WKT":
        rename_map[wkt_col] = "WKT"
    if council_col != "CouncilName":
        rename_map[council_col] = "CouncilName"
    if id_col != "id":
        rename_map[id_col] = "id"
    if rename_map:
        df = df.rename(rename_map)
    return df


def process_single_csv_against_polygons(
    data_df: pl.DataFrame,
    polygon_df: pl.DataFrame,
    lon_col: str,
    lat_col: str,
) -> List[Tuple[str, str, pl.DataFrame]]:
    """Point-in-polygon assignment using spatial index."""
    from shapely.strtree import STRtree

    results: List[Tuple[str, str, pl.DataFrame]] = []
    raw_lons = data_df[lon_col].to_list()
    raw_lats = data_df[lat_col].to_list()
    num_lons = [_safe_float(x) for x in raw_lons]
    num_lats = [_safe_float(x) for x in raw_lats]
    n = len(num_lons)
    if n != len(num_lats):
        raise ValueError("Longitude and Latitude columns have different lengths")
    valid_indices = []
    valid_points = []
    for idx in range(n):
        lon, lat = num_lons[idx], num_lats[idx]
        if lon is not None and lat is not None:
            valid_indices.append(idx)
            valid_points.append(Point(lon, lat))
    if not valid_points:
        for i in range(polygon_df.height):
            polygon_name = polygon_df["CouncilName"][i]
            polygon_id = polygon_df["id"][i]
            results.append((str(polygon_id), _clean_filename(polygon_name), data_df.head(0)))
        return results
    polygons = []
    polygon_info = []
    for i in range(polygon_df.height):
        wkt = polygon_df["WKT"][i]
        polygon_name = polygon_df["CouncilName"][i]
        polygon_id = polygon_df["id"][i]
        try:
            geom = wkt_loads(wkt)
        except Exception:
            results.append((str(polygon_id), _clean_filename(polygon_name), data_df.head(0)))
            continue
        if isinstance(geom, MultiPolygon):
            polygon = list(geom.geoms)[0]
        else:
            polygon = geom
        polygons.append(polygon)
        polygon_info.append((str(polygon_id), _clean_filename(polygon_name)))
    if not polygons:
        return results
    try:
        tree = STRtree(polygons)
        polygon_points: Dict[int, List[int]] = {i: [] for i in range(len(polygons))}
        for point_idx, point in enumerate(valid_points):
            for poly_idx in tree.query(point):
                try:
                    if polygons[poly_idx].contains(point):
                        polygon_points[poly_idx].append(valid_indices[point_idx])
                        break
                except Exception:
                    continue
        for poly_idx, (pid, pname) in enumerate(polygon_info):
            indices = polygon_points[poly_idx]
            results.append((pid, pname, data_df[indices] if indices else data_df.head(0)))
    except Exception as e:
        logger.warning("Spatial index failed, using fallback: %s", e)
        return _process_single_csv_fallback(data_df, polygon_df, lon_col, lat_col)
    return results


def _process_single_csv_fallback(
    data_df: pl.DataFrame,
    polygon_df: pl.DataFrame,
    lon_col: str,
    lat_col: str,
) -> List[Tuple[str, str, pl.DataFrame]]:
    results: List[Tuple[str, str, pl.DataFrame]] = []
    raw_lons = data_df[lon_col].to_list()
    raw_lats = data_df[lat_col].to_list()
    num_lons = [_safe_float(x) for x in raw_lons]
    num_lats = [_safe_float(x) for x in raw_lats]
    n = len(num_lons)
    all_indices = list(range(n))
    for i in range(polygon_df.height):
        wkt = polygon_df["WKT"][i]
        polygon_name = polygon_df["CouncilName"][i]
        polygon_id = polygon_df["id"][i]
        try:
            geom = wkt_loads(wkt)
        except Exception:
            continue
        if isinstance(geom, MultiPolygon):
            polygon = list(geom.geoms)[0]
        else:
            polygon = geom
        prepared = prep(polygon)
        minx, miny, maxx, maxy = polygon.bounds
        candidate_idx = [
            idx for idx in all_indices
            if num_lons[idx] is not None and num_lats[idx] is not None
            and minx <= num_lons[idx] <= maxx and miny <= num_lats[idx] <= maxy
        ]
        inside_idx = [
            idx for idx in candidate_idx
            if prepared.contains(Point(num_lons[idx], num_lats[idx]))
        ]
        results.append((
            str(polygon_id),
            _clean_filename(polygon_name),
            data_df[inside_idx] if inside_idx else data_df.head(0),
        ))
    return results


def save_per_polygon_results(
    results: List[Tuple[str, str, pl.DataFrame]],
    csv_file_path: Path,
    batch_output_dir: Path,
    output_cb=None,
) -> None:
    csv_basename = csv_file_path.stem
    out_dir_for_file = batch_output_dir / csv_basename
    out_dir_for_file.mkdir(parents=True, exist_ok=True)
    for polygon_id_str, polygon_name_clean, df in results:
        fname = f"{csv_basename}_id{polygon_id_str}_{polygon_name_clean}.csv"
        out_path = out_dir_for_file / fname
        df.write_csv(str(out_path), line_terminator="\r\n")
        if output_cb:
            output_cb(f"Saved {out_path} ({df.height} rows)")


def merge_per_polygon_files(
    batch_output_dir: Path, polygon_csv_path: Path, output_cb=None
) -> None:
    import re
    csv_files = [f for f in batch_output_dir.rglob("*.csv") if not f.name.startswith("merged_")]
    polygon_df = read_csv_as_strings(polygon_csv_path)
    id_col = _get_col_by_lower(polygon_df, "id")
    name_col = (
        _get_col_by_lower(polygon_df, "councilname")
        or _get_col_by_lower(polygon_df, "name")
        or _get_col_by_lower(polygon_df, "polygonname")
    )
    if id_col is None:
        if output_cb:
            output_cb("Warning: Polygon CSV missing 'id' column.")
        return
    if name_col is None:
        name_col = id_col
    all_polygons = [
        (str(polygon_df[id_col][i]).strip(), _clean_filename(str(polygon_df[name_col][i])))
        for i in range(polygon_df.height)
    ]
    polygon_groups: Dict[Tuple[str, str], List[Path]] = {}
    for f in csv_files:
        m = re.match(r".+_id([^_]+)_(.+)\.csv$", f.name)
        if m:
            polygon_id, polygon_name = m.group(1), m.group(2)
        else:
            polygon_id, polygon_name = f.stem, "unknown"
        polygon_groups.setdefault((polygon_id, polygon_name), []).append(f)
    for (polygon_id, polygon_name) in all_polygons:
        files = polygon_groups.get((polygon_id, polygon_name), [])
        dfs = [read_csv_as_strings(fp) for fp in files]
        merged_df = pl.concat(dfs, how="vertical") if dfs else pl.DataFrame()
        merged_path = batch_output_dir / f"merged_id{polygon_id}_{polygon_name}.csv"
        merged_df.write_csv(str(merged_path), line_terminator="\r\n")
        if output_cb:
            output_cb(f"[OK] Merged {len(files)} files -> {merged_path}")


def process_folder_batch(
    folder_path: Path,
    polygon_file_path: Path,
    validate_all: bool = False,
    output_cb=None,
    progress_cb=None,
) -> None:
    """Process a folder of CSV files against polygons.
    progress_cb(percent: int) is called with 0-100 if provided.
    """
    from datetime import datetime

    folder_path = Path(folder_path)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    csv_files = sorted(folder_path.glob("*.csv"))
    if not csv_files:
        msg = f"No CSV files found in folder: {folder_path}"
        logger.warning(msg)
        if output_cb:
            output_cb(msg)
        return
    if progress_cb:
        progress_cb(5)
    polygon_df = validate_polygon_file(polygon_file_path, full_validate=validate_all)
    output_dir = folder_path / "batch_results"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_output_dir = output_dir / f"batch_{timestamp}"
    batch_output_dir.mkdir(parents=True, exist_ok=True)
    total_files = len(csv_files)
    processed_count = skipped_count = error_count = 0
    for i, csv_file in enumerate(csv_files):
        try:
            if progress_cb:
                progress_cb(min(88, 5 + int((i / total_files) * 83)))
            if output_cb:
                output_cb(f"\nProcessing file: {csv_file}")
            data_df = read_csv_as_strings(csv_file)
            if data_df.height == 0:
                if output_cb:
                    output_cb(f"  -> Skipping {csv_file.name}: empty file")
                skipped_count += 1
                continue
            lon_col = _get_col_by_lower(data_df, "longitude") or _get_col_by_lower(data_df, "lon")
            lat_col = _get_col_by_lower(data_df, "latitude") or _get_col_by_lower(data_df, "lat")
            if not lon_col or not lat_col:
                missing = []
                if not lon_col:
                    missing.append("longitude/lon")
                if not lat_col:
                    missing.append("latitude/lat")
                if output_cb:
                    output_cb(f"  -> Skipping {csv_file.name}: missing {', '.join(missing)}")
                skipped_count += 1
                continue
            results = process_single_csv_against_polygons(data_df, polygon_df, lon_col, lat_col)
            save_per_polygon_results(results, csv_file, batch_output_dir, output_cb=output_cb)
            processed_count += 1
            if progress_cb:
                progress_cb(min(88, 5 + int(((i + 1) / total_files) * 83)))
        except Exception as e:
            error_count += 1
            logger.exception("Error processing %s", csv_file.name)
            if output_cb:
                output_cb(f"  -> [ERROR] {str(e)}")
    if processed_count == 0:
        if progress_cb:
            progress_cb(100)
        if output_cb:
            output_cb("No files were successfully processed!")
        return
    if progress_cb:
        progress_cb(90)
    if output_cb:
        output_cb("\nMerging per-polygon files...")
    merge_per_polygon_files(batch_output_dir, polygon_file_path, output_cb=output_cb)
    if progress_cb:
        progress_cb(100)
    if output_cb:
        output_cb(f"\n✅ Processed: {processed_count}, Skipped: {skipped_count}, Errors: {error_count}")
        output_cb(f"Results saved in: {batch_output_dir}")
