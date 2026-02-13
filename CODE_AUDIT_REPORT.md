# Báo cáo Audit Codebase - Data Processing Tool (DataCleaner)

**Ngày tạo:** 12/02/2026  
**Phạm vi:** Toàn bộ project (config, gui, utils, main)  
**Mục tiêu:** Lỗi tiềm ẩn, bảo mật, hiệu năng, chất lượng code  

---

## 1. Danh sách file và module

| Thư mục / File | Mô tả |
|----------------|--------|
| **Gốc** | |
| `main.py` | Entry point, lazy import GUI, exception handler |
| `requirements.txt` | PyQt6, polars, PyInstaller |
| **config/** | |
| `app_config.py` | Cấu hình tập trung (chunk, memory, timeout, paths) |
| `laneFix_config.py` | Cấu hình Lane Fix (column mappings, messages) |
| **gui/** | |
| `main_window.py` | Cửa sổ chính, lazy tab widget |
| `lazy_loader.py` | LazyTabWidget – load tab khi click |
| `icons.py`, `rose_gold_styles.py`, `styles.py`, `modern_styles.py` | UI styling |
| **gui/tabs/** | |
| `lmd_cleaner_tab.py` | Tab xử lý LMD (single file / folder, merge + clean) |
| `laneFix_tab.py` | Tab Lane Fix / Workbrief |
| `add_columns_tab.py` | Tab thêm cột từ file ngoài |
| `client_feedback_tab.py` | Tab client feedback |
| `help_tab.py` | Tab trợ giúp |
| **utils/** | |
| `data_processor.py` | Xử lý CSV lớn (chunk, parallel, memory monitor) |
| `data_merge_multi.py` | Script CLI gộp CSV (Polars, streaming) |
| `security.py` | SecurityValidator, UserFriendlyError, InputValidator |
| `file_lock.py` | FileLock, safe_write_csv |
| `safe_writer.py` | BackupManager, safe_write_dataframe |
| `db_pool.py` | Connection pool SQLite |
| `base_processor.py` | BaseProcessor, CancellableProcessor |
| `data_validator.py` | DataValidator (CSV structure, columns, integrity) |
| `cancellation_handler.py` | CancellationToken, CancellableWorker |
| `logger_setup.py` | ApplicationLogger, setup_application_logging |
| `lazy_imports.py` | LazyImporter, polars/pandas/numpy lazy |
| `memory_efficient_processor.py` | MemoryEfficientAddColumnsProcessor |
| `add_columns_processor.py` | AddColumnsProcessor |
| `laneFix_polar_data_processor.py` | PolarsCombinedProcessor, LaneFix, Workbrief |
| `resource_limiter.py` | ResourceLimiter (memory, timeout, file size) |
| `rate_limiter.py` | RateLimiter (sliding window) |
| `context_logger.py` | Context logger helpers |

---

## 2. Tổng quan mức độ ảnh hưởng

| Mức độ | Số lượng | Mô tả ngắn |
|--------|----------|------------|
| Critical | 2 | Bare except nuốt lỗi, cancellation chưa thực sự hoạt động |
| High | 6 | Exception handling quá rộng, race/cleanup, hardcode path |
| Medium | 12 | Duplicate code, thiếu validation, log path |
| Low | 10 | Naming, readability, dependency, style |

---

## 3. Lỗi tiềm ẩn (Bugs & Exception)

### 3.1 Critical

| ID | File:Line | Mô tả | Đề xuất |
|----|-----------|--------|---------|
| C1 | `main.py:79` | `except: pass` khi đóng splash – nuốt mọi exception | Bắt cụ thể `ImportError`/`ModuleNotFoundError` và log. |
| C2 | `lmd_cleaner_tab.py:408–411` | Nút Cancel không thực sự dừng worker (streaming) | Dùng `CancellationToken`/`_is_cancelled` trong vòng lặp của `data_processor` và `ProcessingWorker.run()`; khi cancel → dừng sớm và emit done(False, "Cancelled"). |

**Ví dụ sửa C1 (main.py):**

```python
# Trước (tránh):
try:
    import pyi_splash
    pyi_splash.close()
    logger.info("Splash screen closed")
except:
    pass

# Sau (đề xuất):
except (ImportError, ModuleNotFoundError) as e:
    logger.debug("PyInstaller splash not available: %s", e)
except Exception as e:
    logger.warning("Could not close splash: %s", e)
```

**Ví dụ hướng sửa C2:** Trong `utils/data_processor.py`, các hàm `_process_memory_safe_ultra_fast`, `_process_chunk_worker_safe_to_file` và vòng lặp batch cần nhận `progress_callback` hoặc một object có `is_cancelled()`. Mỗi batch/chunk kiểm tra `is_cancelled()`; nếu True thì raise một exception (ví dụ `ProcessingCancelled`) và worker bắt để emit `done(False, "Cancelled")`.

---

### 3.2 High

| ID | File:Line | Mô tả | Đề xuất |
|----|-----------|--------|---------|
| H1 | `utils/data_processor.py` (nhiều dòng) | Nhiều `except: pass` hoặc `except: continue` (vd. 190, 196, 209, 220, 242, 607) | Thay bằng `except Exception as e:` và log (logger.warning/error); với lỗi nghiêm trọng có thể re-raise. |
| H2 | `utils/data_processor.py:381–384` | `_process_chunk_worker_safe`: callback cleanup gọi `except: pass` | Ghi log exception và có thể ghi vào stats để debug. |
| H3 | `gui/tabs/lmd_cleaner_tab.py:134, 215, 222, 233` | `_read_header` và fallback đọc header dùng bare `except:` | Chỉ bắt `Exception` (hoặc cụ thể hơn), log và return []/None rõ ràng. |
| H4 | `utils/file_lock.py:73, 93` | Trong lock acquire/release: `except: pass` | Bắt cụ thể (OSError, PermissionError) và log; không nuốt lỗi khi remove lock file. |
| H5 | `utils/db_pool.py:138, 148, 262` | Rollback/commit/close trong pool dùng bare `except: pass` | Log exception và có thể re-raise khi commit/rollback thất bại để caller biết. |
| H6 | `utils/data_merge_multi.py` (88, 96, 111, 129) | Bare except trong đọc header / CSV | Giống H3: bắt Exception, log, trả về None hoặc giá trị mặc định. |

---

### 3.3 Medium (Exception & logic)

| ID | File:Line | Mô tả | Đề xuất |
|----|-----------|--------|---------|
| M1 | `config/app_config.py:225–227` | `load_from_file` mở file không chỉ định encoding | Dùng `open(..., encoding='utf-8')` khi đọc JSON. |
| M2 | `config/app_config.py:282–285` | Validate config khi import – chỉ warning, không fail | Tùy môi trường: production có thể sys.exit(1) hoặc raise khi invalid. |
| M3 | `gui/lazy_loader.py:153` | `_loaded_tabs[index] = None` tạm thời – nếu factory raise, tab có thể ở trạng thái lỗi | Đã có khối except gắn error widget; đảm bảo luôn set lại `_loaded_tabs[index]` (error widget hoặc remove) để tránh re-entry sai. |
| M4 | `utils/logger_setup.py` | Mỗi lần setup tạo log file mới theo timestamp | Cân nhắc RotatingFileHandler hoặc 1 file cố định + rotation để dễ theo dõi. |
| M5 | `utils/safe_writer.py:232` | Trong cleanup temp: bare `except: pass` | Bắt OSError và log. |
| M6 | `utils/memory_efficient_processor.py`, `add_columns_processor.py` | Các khối except rộng hoặc bare | Thu hẹp loại exception và log đầy đủ. |

---

## 4. Bảo mật

### 4.1 Đã làm tốt

- **Path & file:** `SecurityValidator.sanitize_file_path`, `validate_output_path` – giới hạn extension, kích thước, quyền đọc/ghi.
- **Input:** `InputValidator.validate_chunk_size`, `validate_column_list`; `SecurityValidator.sanitize_column_name` (regex whitelist).
- **SQL:** `db_pool` dùng tham số hóa (`cursor.execute(query, params)`), không nối chuỗi SQL trực tiếp từ input.
- **Lỗi hiển thị:** `UserFriendlyError` che path và chi tiết nội bộ trước khi hiển thị cho user.

### 4.2 Cần cải thiện

| ID | File | Mô tả | Đề xuất |
|----|------|--------|---------|
| S1 | `gui/tabs/lmd_cleaner_tab.py:361` | Hardcode path `"J:/Processing"` | Lấy từ config (AppConfig) hoặc biến môi trường; fallback về thư mục hiện tại hoặc user home. |
| S2 | `config/laneFix_config.py:89` | Log ghi ra file cố định `fixlane_app.log` (có thể ghi đè giữa các instance) | Dùng cơ chế log tập trung (logger_setup) hoặc path có timestamp/process id. |
| S3 | Toàn project | Đầu vào từ CSV không được validate số dòng/cột tối đa trước khi đọc hết | Đã có AppConfig.MAX_CSV_ROWS/MAX_CSV_COLUMNS; đảm bảo data_processor và các tab gọi DataValidator/AppConfig trước khi load lớn. |

**Ví dụ S1:**

```python
# Trước
default_dir = "J:/Processing" if os.path.exists("J:/Processing") else ""

# Sau (đề xuất)
from config.app_config import AppConfig
default_dir = os.environ.get("DATA_CLEANER_INPUT_DIR", "")
if not default_dir or not os.path.isdir(default_dir):
    default_dir = os.path.expanduser("~")  # hoặc ""
```

---

## 5. Hiệu năng

### 5.1 Điểm tốt

- Lazy import (main.py, lazy_imports, LazyTabWidget) giảm thời gian khởi động.
- Data processing: chunk, ProcessPoolExecutor, streaming, memory monitor, giới hạn worker.
- Polars (lazy, scan_csv, streaming) phù hợp file lớn.

### 5.2 Cần tối ưu

| ID | File | Mô tả | Đề xuất |
|----|------|--------|---------|
| P1 | `utils/data_processor.py` | `_read_header` gọi nhiều lần `pl.read_csv(..., n_rows=0)` với nhiều delimiter | Cache header theo (input_file, mtime/size) trong một run; hoặc đọc một lần với encoding/delimiter đã detect. |
| P2 | `gui/tabs/lmd_cleaner_tab.py:418–420` | `os.listdir` + filter `.csv` mỗi lần mở dialog folder | Chỉ cần khi user đã chọn folder và bấm Process; không cần đổi trước đó. |
| P3 | `utils/data_processor.py` | `global_seen_dates` set tăng theo số dòng unique TestDateUTC | Với file rất lớn, cân nhắc bloom filter hoặc giới hạn size set + fallback (vd. bỏ dedup khi quá lớn) để tránh OOM. |
| P4 | `utils/data_merge_multi.py:316` | Trong streaming vẫn gọi `lf.select(pl.count()).collect()` từng file → full scan | Nếu không bắt buộc đếm từng file, bỏ hoặc làm optional (flag) để tránh double scan. |
| P5 | `config/app_config.py:259–266` | `to_dict()` dùng `dir(cls)` và `attr.isupper()` – có thể lấy cả attribute kế thừa | Chỉ lấy key trong một whitelist hoặc từ một list tên constant đã định nghĩa trong class. |

**Ví dụ P4 (data_merge_multi.py):**

```python
# Trước: luôn đếm từng file (scan 2 lần khi streaming)
row_count = lf.select(pl.count()).collect().item()
log(f"  ✓ Loaded {row_count:,} rows (lazy)")
total_rows += row_count

# Sau: không bắt buộc đếm từng file
if log_row_counts:
    row_count = lf.select(pl.count()).collect().item()
    total_rows += row_count
    log(f"  ✓ Loaded {row_count:,} rows (lazy)")
else:
    all_dataframes.append(lf)
```

---

## 6. Cấu trúc code & chất lượng

### 6.1 Duplicate code / Anti-pattern

| ID | Vị trí | Mô tả | Đề xuất |
|----|--------|--------|---------|
| D1 | `data_processor.py` vs `lmd_cleaner_tab.py` | Logic filter (TrailingFactor, Slope, Lane, Ignore, TestDateUTC) và đọc header lặp lại | Trích hàm dùng chung trong `data_processor` (vd. `apply_lmd_filters(df, columns)`, `read_csv_header(path)`) và gọi từ cả data_processor và lmd_cleaner_tab. |
| D2 | `lmd_cleaner_tab._read_header` vs `data_processor._read_header` | Hai implementation gần giống nhau | Dùng một hàm trong `utils/data_processor.py` (hoặc utils/headers.py) và import vào tab. |
| D3 | `QTextEditHandler` | Định nghĩa trong cả `lmd_cleaner_tab` và `laneFix_tab` | Đưa vào `gui/widgets.py` hoặc `utils/logging_helper.py` và import lại. |
| D4 | `laneFix_config.LogConfig.setup_logging` | Ghi file `fixlane_app.log` tách với ApplicationLogger | Chỉ dùng một hệ thống log (logger_setup) cho toàn app. |

### 6.2 Naming & readability

| ID | File | Mô tả | Đề xuất |
|----|------|--------|---------|
| N1 | `data_processor.py` | Hằng số MAX_RAM_USAGE_GB = 100, EMERGENCY_GC_THRESHOLD = 90 | Đưa vào AppConfig hoặc env; đặt tên rõ (vd. MEMORY_EMERGENCY_GC_PERCENT). |
| N2 | `lmd_cleaner_tab` | Biến `is_folder` và logic “single file” vs “folder” rải rác | Có thể dùng enum hoặc dataclass “InputMode” (single_file / folder) để code rõ hơn. |

### 6.3 Dependency & thư viện

| ID | Mô tả | Đề xuất |
|----|--------|---------|
| L1 | `requirements.txt` chỉ có PyQt6, polars, PyInstaller | Thêm `psutil` (đã dùng trong data_processor, resource_limiter). |
| L2 | `laneFix_config` / một số tab import `pandas` | Kiểm tra: nếu không dùng pandas thì bỏ; nếu dùng thì thêm vào requirements. |

---

## 7. Bảng tổng hợp ưu tiên

| Ưu tiên | ID | Mức độ | Hành động |
|---------|-----|--------|-----------|
| 1 | C1, C2 | Critical | Sửa bare except main.py; triển khai cancellation thực sự trong worker + data_processor |
| 2 | H1–H6 | High | Thay toàn bộ bare `except:` bằng bắt cụ thể + log trong data_processor, file_lock, db_pool, lmd_cleaner_tab, data_merge_multi |
| 3 | S1, S2 | Security | Bỏ hardcode path; thống nhất logging |
| 4 | D1, D2, D3 | Medium | Refactor filter + header dùng chung; gom QTextEditHandler |
| 5 | P1, P3, P4 | Performance | Cache header; xem xét giới hạn dedup set; tùy chọn đếm dòng khi merge |
| 6 | M1–M6, N1, L1 | Low | Encoding config, exception cụ thể, naming, requirements |

---

## 8. Ví dụ refactor (D1/D2) – Logic filter và header dùng chung

**Bước 1 – Hàm đọc header dùng chung (utils/data_processor.py):**

```python
def read_csv_header(input_file: str) -> list[str]:
    """Single place for CSV header reading. Used by both batch processor and LMD tab."""
    # ... (logic hiện tại từ _read_header, với except Exception + log)
    return columns  # or []
```

**Bước 2 – Hàm filter dùng chung:**

```python
def apply_lmd_filters(df: pl.DataFrame, columns: list[str], first_col: str) -> pl.DataFrame:
    """Apply standard LMD filters. Returns filtered DataFrame."""
    df = df.filter(df[first_col].is_not_null() & (df[first_col] != ""))
    if "RawSlope170" in columns and "RawSlope270" in columns:
        df = df.filter(~((df["RawSlope170"].is_null() | (df["RawSlope170"] == ""))
                       & (df["RawSlope270"].is_null() | (df["RawSlope270"] == ""))))
    # ... TrailingFactor, tsdSlopeMinY/MaxY, Lane, Ignore
    return df
```

**Bước 3 – LMD tab gọi:**

```python
# lmd_cleaner_tab.py
from utils.data_processor import read_csv_header, apply_lmd_filters

def _clean_single_file(self, input_file: str, output_file: str):
    columns = read_csv_header(input_file)
    if not columns:
        raise RuntimeError(f"Failed to read header from {input_file}")
    # ...
    df = apply_lmd_filters(df, columns, columns[0])
```

---

## 9. Kết luận

- **Critical:** 2 – cần sửa ngay (bare except trong main, cancellation không hoạt động).
- **High:** 6 – exception handling quá rộng ở data_processor, file_lock, db_pool, lmd_cleaner_tab, data_merge_multi.
- **Security:** Path validation và SQL đã ổn; cần bỏ hardcode path và thống nhất log.
- **Performance:** Đã có chunk/streaming/memory monitor; có thể thêm cache header, tối ưu dedup và đếm dòng.
- **Chất lượng:** Nên gom logic filter/header, gom QTextEditHandler, bổ sung psutil vào requirements và kiểm tra pandas.

Nên xử lý theo thứ tự: Critical → High (exception) → Security (path, log) → Refactor (D1–D3) → Performance (P1, P3, P4) → Low (M, N, L).
