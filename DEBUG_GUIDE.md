# Debug Guide for DataProcessingTool

## Vấn đề: EXE crash nhưng Python script chạy bình thường

Khi ứng dụng chạy tốt với `python main.py` nhưng crash khi chạy file EXE, đây là các bước để debug:

## 1. Hệ thống Logging mới

Ứng dụng giờ đây có hệ thống logging toàn diện:
- **Log files**: Tự động tạo trong thư mục `logs/` cùng cấp với file EXE
- **Tên file**: `DataCleaner_YYYYMMDD_HHMMSS.log`
- **Nội dung**: Chi tiết về startup, errors, exceptions, system info

## 2. Các phiên bản Build

### Build Debug (Khuyến nghị cho troubleshooting)
```bash
python build_with_debug.py
# Chọn option 1
```
- Tạo file: `DataProcessingTool_Debug.exe`
- **Hiển thị console window** để xem lỗi real-time
- Log cả ra file VÀ console
- Dễ debug hơn

### Build Release (Cho production)
```bash
python build_with_debug.py
# Chọn option 2
```
- Tạo file: `DataProcessingTool.exe`
- Không hiển thị console window
- Chỉ log ra file
- Giao diện clean cho user

## 3. Cách Debug EXE crash

### Bước 1: Build Debug version
```bash
python build_with_debug.py
```
Chọn option 1 để build version có console.

### Bước 2: Chạy Debug EXE
- Chạy `DataProcessingTool_Debug.exe`
- Console window sẽ hiện ra
- Xem lỗi trực tiếp trong console
- **Không đóng console window** khi app crash

### Bước 3: Kiểm tra Log files
Vị trí log files:
```
[Thư mục chứa EXE]/logs/DataCleaner_YYYYMMDD_HHMMSS.log
```

Log chứa thông tin:
- Startup sequence chi tiết
- System information
- Import errors
- Exception traceback đầy đủ
- Module loading issues

### Bước 4: Phân tích các lỗi thường gặp

#### Lỗi Import/Module
```
ImportError: No module named 'xxx'
ModuleNotFoundError: No module named 'xxx'
```
**Giải pháp**: Thêm module vào `hidden_imports` trong build script

#### Lỗi File Path
```
FileNotFoundError: [Errno 2] No such file or directory
```
**Giải pháp**: Sử dụng relative paths, kiểm tra working directory

#### Lỗi PyQt6/GUI
```
AttributeError: module 'PyQt6' has no attribute 'xxx'
```
**Giải pháp**: Kiểm tra PyQt6 installation, thêm specific imports

## 4. Test Scripts

### Test Logging
```bash
python test_logging.py
```
Kiểm tra logging system hoạt động chưa.

### Test Main App
```bash
python main.py
```
Kiểm tra app hoạt động trong Python environment.

## 5. Common Solutions

### Missing Dependencies
Nếu EXE thiếu dependencies:
1. Cài đặt lại requirements: `pip install -r requirements.txt`
2. Update build script với hidden imports
3. Rebuild EXE

### Path Issues
Nếu EXE không tìm được files:
1. Kiểm tra working directory trong log
2. Sử dụng absolute paths cho critical files
3. Copy cần thiết files vào cùng thư mục với EXE

### PyQt6 Issues
Nếu GUI không hiển thị:
1. Kiểm tra PyQt6 version compatibility
2. Test với `python -c "from PyQt6.QtWidgets import QApplication; print('OK')"`
3. Rebuild với đầy đủ PyQt6 imports

## 6. Log Analysis Guide

### Startup Logs
```
DataCleaner - Application Starting
Running as: EXE
Python version: ...
Working directory: ...
```

### Success Indicators
```
Application logging initialized successfully
Creating QApplication...
Creating main window...
UI initialization completed successfully
```

### Error Indicators
```
Failed to import logging setup: ...
Fatal error in main(): ...
Exception in [function]: ...
Traceback (most recent call last):
```

## 7. Quick Debugging Checklist

1. ✅ Build debug version: `python build_with_debug.py` (option 1)
2. ✅ Run debug EXE và xem console output
3. ✅ Kiểm tra log file trong thư mục `logs/`
4. ✅ Note down exact error message
5. ✅ Check working directory và file paths
6. ✅ Verify Python dependencies
7. ✅ Test individual components nếu cần

## 8. Liên hệ Support

Khi báo bug, hãy cung cấp:
- **Console output** từ debug EXE
- **Log file** content
- **System info** (OS, Python version, etc.)
- **Steps to reproduce** the crash

Điều này sẽ giúp identify root cause nhanh hơn rất nhiều!