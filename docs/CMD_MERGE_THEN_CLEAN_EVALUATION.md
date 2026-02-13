# Đánh giá: Gộp CSV bằng CMD rồi clean một file

## 1. Lỗi trong lệnh CMD hiện tại

Lệnh của bạn:

```cmd
(for %f in (*.csv) do @type "%f" & goto done) > combined_output.csv
& for %f in (*.csv) do (more +1 "%f") >> combined_output.csv
```

- **Phần 1:** Ghi **toàn bộ file CSV đầu tiên** (header + data) vào `combined_output.csv` → đúng.
- **Phần 2:** Với **mọi** file `*.csv` (kể cả file đầu), append `more +1 "%f"` (bỏ dòng 1 = header) vào `combined_output.csv`.

Kết quả: Dữ liệu của **file đầu tiên bị append thêm một lần** (chỉ phần data, không header) → **trùng dòng** so với phần đã ghi ở bước 1. Các file sau thì đúng (chỉ data, không header).

**Cách sửa:** Chỉ append “bỏ dòng 1” cho **từ file thứ 2 trở đi**, hoặc dùng script dưới đây.

---

## 2. So sánh hiệu năng: CMD gộp + clean 1 file vs merge_and_clean_folder

Giả định: N file CSV, tổng dung lượng ~T GB, cùng cấu trúc cột, cần **gộp + clean (lọc + dedup TestDateUTC)**.

### Cách A: CMD gộp rồi clean một file (merge xong mới clean)

| Bước | Công việc | I/O | Ghi chú |
|------|-----------|-----|--------|
| 1 | CMD gộp CSV | Đọc N file, ghi 1 file combined | Chỉ copy byte, không parse CSV |
| 2 | Clean 1 file (process_data) | Đọc 1 file T GB, ghi 1 file output | Một lần đọc/ghi, chunked nếu T lớn |

- **Merge (CMD):** Rất nhanh — chỉ copy dữ liệu, không tốn CPU parse CSV, không tốn RAM của Python/Polars.
- **Clean:** Một lần xử lý một file lớn T GB: đọc theo chunk → lọc → dedup (bộ nhớ + SQLite nếu cần) → ghi. Giống xử lý “single file” trong app.

**Tổng I/O (đơn giản hóa):**  
- Đọc: N file (gộp) + 1 lần đọc file combined (clean) = **N + 1 lần đọc**.  
- Ghi: 1 file combined + 1 file output = **2 lần ghi** (2 file lớn).

### Cách B: merge_and_clean_folder (trong app hiện tại)

| Bước | Công việc | I/O |
|------|-----------|-----|
| Với từng file | Clean từng file → ghi temp cleaned | N lần đọc + N lần ghi temp |
| Merge | Đọc lần lượt từng temp cleaned, dedup cross-file, ghi output | N lần đọc temp + 1 ghi output |

**Tổng I/O:**  
- Đọc: N file gốc + N file temp = **2N lần đọc**.  
- Ghi: N file temp + 1 file output = **N + 1 lần ghi**.

### Kết luận hiệu năng

- **Số lần đọc:** CMD + clean = **N + 1**; app = **2N** → CMD + clean **ít đọc hơn** (rõ khi N lớn).
- **Merge:** CMD chỉ copy byte → **nhanh hơn nhiều** so với “đọc CSV → clean → ghi temp” rồi mới merge.
- **Clean:** Cả hai đều chỉ **một lần** chạy logic clean (lọc + dedup) trên toàn bộ dữ liệu; CMD + clean chạy trên 1 file combined, app chạy trên N file qua merge từng file.

**Kỳ vọng thời gian (khi N lớn, T lớn):**

- **CMD gộp + clean 1 file** thường **nhanh hơn** vì:
  1. Bước gộp rất nhanh (copy thuần).
  2. Tổng số lần đọc/ghi ít hơn (N+1 đọc vs 2N đọc).
  3. Clean chỉ một lần trên một file, tận dụng tối đa pipeline đọc chunk → xử lý → ghi đã tối ưu trong `process_data`.

**Đổi lại:**

- Cần **đủ dung lượng đĩa** cho file combined (≈ T GB).
- Phải **sửa lệnh CMD** (hoặc dùng script) để không trùng dữ liệu file đầu.

---

## 3. Script mẫu: CMD gộp (đúng) + gọi clean

Dưới đây là cách gộp **đúng** (chỉ header từ file đầu, không trùng data file đầu), rồi gọi `process_data` để clean một file.

```python
import subprocess
import sys
from pathlib import Path

# Thêm project root vào path để import utils
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from utils.data_processor import process_data

def merge_csv_cmd_then_clean(
    folder: Path,
    combined_name: str = "combined_output.csv",
    output_name: str = "combined_cleaned.csv",
) -> Path:
    folder = Path(folder).resolve()
    combined = folder / combined_name
    output = folder / output_name

    # Bước 1: Gộp CSV bằng CMD (đúng: header file đầu, các file sau bỏ dòng 1)
    # PowerShell hoặc CMD: file đầu ghi cả file; từ file thứ 2 chỉ append từ dòng 2
    csvs = sorted(folder.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV in {folder}")

    with open(combined, "wb") as out:
        for i, f in enumerate(csvs):
            with open(f, "rb") as inf:
                if i == 0:
                    out.write(inf.read())
                else:
                    # Bỏ dòng đầu (header)
                    inf.readline()
                    out.write(inf.read())

    print("Đã gộp CSV xong (header file đầu, không trùng).")

    # Bước 2: Clean một file (process_data)
    process_data(str(combined), str(output), progress_callback=print)
    print("Đã clean xong:", output)
    return output
```

Nếu bạn **cố ý** dùng CMD thuần (để tận dụng copy nhanh của OS), có thể dùng PowerShell từ Python để gộp đúng (ví dụ đọc file đầu ghi nguyên, các file sau `Get-Content -Skip 1` append). Phần clean vẫn gọi `process_data(combined, output)` như trên.

---

## 4. Tóm tắt

| Tiêu chí | CMD gộp + clean 1 file | merge_and_clean_folder (app) |
|----------|------------------------|------------------------------|
| Tốc độ gộp | Rất nhanh (copy byte) | Chậm hơn (đọc/parse/clean/ghi từng file) |
| Số lần đọc đĩa | N + 1 | 2N |
| Dung lượng đĩa tạm | 1 file combined ≈ T GB | N file temp (nhưng xóa từng cái sau khi merge) |
| Hiệu năng tổng thể | Thường **tốt hơn** khi N lớn, T lớn | An toàn bộ nhớ, ít đĩa tạm hơn |

**Khuyến nghị:** Nếu bạn chấp nhận có thêm 1 file combined (≈ tổng dung lượng folder) và sửa đúng lệnh gộp (không trùng file đầu), **cách “gộp bằng CMD (hoặc script copy) rồi clean một file” sẽ cho hiệu năng tốt hơn** so với gộp + clean từng file rồi merge trong app. Có thể bọc bước gộp + gọi `process_data` vào một script/hàm như trên để dùng nhất quán.
