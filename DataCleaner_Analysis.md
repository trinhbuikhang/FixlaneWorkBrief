# 1. MÔ TẢ TỔNG QUAN ỨNG DỤNG

**Tên app + mục đích chính:**  
Data Cleaner - Một ứng dụng PyQt6 để làm sạch dữ liệu CSV từ binviewer combined_lmd bằng cách loại bỏ các hàng dựa trên tiêu chí cụ thể.

**Người dùng chính của app:**  
Kỹ sư dữ liệu, nhà phân tích dữ liệu, hoặc nhân viên kỹ thuật làm việc với dữ liệu binviewer trong lĩnh vực kỹ thuật (có thể liên quan đến giao thông, lane detection, hoặc xử lý dữ liệu cảm biến).

**Vấn đề app giải quyết:**  
Xử lý tự động dữ liệu thô từ binviewer để loại bỏ các hàng không mong muốn dựa trên quy tắc nghiệp vụ, giúp tiết kiệm thời gian và đảm bảo chất lượng dữ liệu đầu vào cho các hệ thống downstream.

**Tóm tắt chức năng cốt lõi:**  
- Giao diện tabbed với 4 công cụ xử lý chính: LMD Cleaner, Lane Fix, Client Feedback Processor, Add Columns  
- Sử dụng Polars cho xử lý dữ liệu hiệu suất cao  
- Hỗ trợ xử lý file lớn với memory-efficient streaming  
- Logging chi tiết và error handling robust  

# 2. DANH SÁCH TÍNH NĂNG HIỆN CÓ

**Tính năng: LMD Data Cleaner**  
**Mục đích:** Làm sạch dữ liệu LMD từ binviewer bằng cách áp dụng các bộ lọc để loại bỏ dữ liệu không hợp lệ.  
**Người sử dụng:** Kỹ sư xử lý dữ liệu LMD.  
**Workflow chi tiết:**  
1. User chọn tab LMD Cleaner  
2. Chọn file input CSV (binviewer combined_lmd output)  
3. Chọn đường dẫn output  
4. Click "Process Data" → gọi process_data() từ utils/data_processor.py  
5. Hiển thị progress và log chi tiết  
6. Xuất file cleaned CSV  
**Input:** File CSV với các cột như RawSlope170, RawSlope270, trailingFactor, tsdSlopeMinY, Lane, Ignore.  
**Output:** File CSV đã lọc + log hiển thị số hàng loại bỏ cho mỗi tiêu chí.  

**Tính năng: Lane Fix Processor**  
**Mục đích:** Xử lý WorkBrief CSV với lane fixes, duplicate removal và polar data processing.  
**Người sử dụng:** Người xử lý dữ liệu WorkBrief.  
**Workflow chi tiết:**  
1. User chọn tab Lane Fix  
2. Chọn file input WorkBrief CSV  
3. Cấu hình options (lane fixes, duplicate removal)  
4. Click process → gọi PolarsLaneFixProcessor/PolarsWorkbriefProcessor/PolarsCombinedProcessor  
5. Hiển thị progress và log  
6. Xuất file processed  
**Input:** WorkBrief CSV với lane data.  
**Output:** File CSV đã xử lý lane + log chi tiết.  

**Tính năng: Client Feedback Processor**  
**Mục đích:** Xử lý và phân tích client feedback data.  
**Người sử dụng:** Team support/client management.  
**Workflow chi tiết:**  
1. User chọn tab Client Feedback  
2. Load feedback data  
3. Áp dụng filters và analysis  
4. Click process → gọi ClientFeedbackProcessor  
5. Hiển thị results và statistics  
6. Export processed feedback  
**Input:** Client feedback CSV/data.  
**Output:** Processed feedback + analysis reports.  

**Tính năng: Add Columns Processor**  
**Mục đích:** Thêm cột mới vào dữ liệu kết hợp từ LMD và Details files, với hỗ trợ memory-efficient cho file lớn.  
**Người sử dụng:** Data engineers xử lý large datasets.  
**Workflow chi tiết:**  
1. User chọn tab Add Columns  
2. Chọn LMD file và Details file  
3. Cấu hình chunk size và options  
4. Click process → tự động chọn processor (AddColumnsProcessor hoặc MemoryEfficientAddColumnsProcessor) dựa trên file size  
5. Monitor memory usage và progress  
6. Xuất file với cột mới  
**Input:** LMD CSV + Details CSV (có thể lên đến 25GB+5GB).  
**Output:** Combined CSV với cột mới + memory usage logs.  

**Tính năng: Memory-Efficient Processing**  
**Mục đích:** Xử lý file lớn mà không crash do hết memory.  
**Người sử dụng:** Users xử lý datasets >10GB.  
**Workflow chi tiết:**  
1. App tự động detect file size  
2. Chọn processor phù hợp (standard/hybrid/streaming)  
3. Tạo temporary index cho LMD file  
4. Process Details file theo chunks  
5. Monitor memory và cleanup  
**Input:** Large CSV files.  
**Output:** Processed data với memory safety.  

**Tính năng: Comprehensive Logging & Error Handling**  
**Mục đích:** Theo dõi quá trình xử lý và handle errors gracefully.  
**Người sử dụng:** All users để debug và monitor.  
**Workflow chi tiết:**  
1. Setup logging ở main.py  
2. Mỗi processor log chi tiết  
3. UI hiển thị logs realtime  
4. Exception handler catch và log errors  
**Input:** Processing operations.  
**Output:** Detailed logs + error reports.  

**Tính năng: Modern UI với Custom Styling**  
**Mục đích:** Giao diện đẹp, consistent, dễ sử dụng.  
**Người sử dụng:** All users.  
**Workflow chi tiết:**  
1. Áp dụng MODERN_STYLESHEET từ gui/modern_styles.py  
2. Sử dụng standardized layouts từ UI_STANDARDS.md  
3. Responsive design với auto-sizing  
**Input:** User interactions.  
**Output:** Beautiful, consistent UI.  

# 3. SƠ ĐỒ LUỒNG LÀM VIỆC (WORKFLOW)

## 3.1. Luồng tổng quát (High-level workflow)

```
App Startup → Load Main Window (QTabWidget) → User Select Tab → Configure Options → Select Input Files → Click Process → 
Processor Execute (with Progress Updates) → Show Results/Logs → Export Output → User Close/Continue
```

## 3.2. Luồng chi tiết cho từng tính năng

**LMD Cleaner Workflow:**  
User click "LMD Cleaner" tab → initUI() tạo widgets → User browse input file → User browse output path → Click "Process Data" → validate inputs → call process_data() → read CSV with Polars → apply 5 filtering criteria → write cleaned CSV → update UI with logs → show completion message.

**Lane Fix Workflow:**  
User click "Lane Fix" tab → load config from config/laneFix_config.py → User select input file → configure processing options (radio buttons) → Click process → instantiate PolarsLaneFixProcessor → process data with lane fixes → handle duplicates → export result → display logs.

**Client Feedback Workflow:**  
User click "Client Feedback" tab → initUI() tạo splitter layout → User load feedback data → apply filters → Click process → ClientFeedbackProcessor.process() → analyze feedback → display results in list/tree → export processed data.

**Add Columns Workflow:**  
User click "Add Columns" tab → initUI() tạo dual file selection → User select LMD file → User select Details file → configure chunk size → Click process → detect file sizes → auto-select processor (AddColumnsProcessor hoặc MemoryEfficientAddColumnsProcessor) → create index if needed → process in chunks → monitor memory → combine data → export.

## 3.3. Data Flow (luồng dữ liệu)

**Input đi vào đâu:**  
- Files: QFileDialog chọn files → stored in QLineEdit widgets → passed to processor classes  
- Config: Hard-coded criteria trong processors hoặc loaded từ config/ folder  
- User options: UI widgets (checkboxes, radio buttons) → passed as parameters to processors  

**Logic xử lý ở đâu:**  
- Data reading/parsing: utils/ processors (Polars operations)  
- Business rules: Hard-coded trong processor methods (filtering criteria)  
- Memory management: MemoryEfficientAddColumnsProcessor với chunking và index creation  
- Validation: UI validation + processor error handling  

**Output hiển thị ở đâu:**  
- Files: Written to user-selected paths via Polars.write_csv()  
- UI feedback: QTextEdit logs, QProgressBar, QLabel status  
- Errors: QMessageBox popups + logged to files  

# 4. PHÂN TÍCH KIẾN TRÚC (Code Architecture Review)

**Ngôn ngữ sử dụng:** Python 3.8+  

**Framework / libraries:**  
- PyQt6: Desktop GUI framework  
- Polars: High-performance DataFrame library cho data processing  
- PyInstaller: Packaging cho executable  
- psutil: Memory monitoring  
- Standard library: logging, os, sys, datetime  

**File structure:**  
```
main.py (entry point)
gui/
├── main_window.py (QTabWidget container)
├── modern_styles.py (QSS styling)
├── styles.py (additional styling)
└── tabs/
    ├── lmd_cleaner_tab.py
    ├── laneFix_tab.py  
    ├── client_feedback_tab.py
    └── add_columns_tab.py
utils/
├── data_processor.py (LMD processing)
├── laneFix_polar_data_processor.py
├── client_feedback_processor.py
├── add_columns_processor.py
├── memory_efficient_processor.py
├── logger_setup.py
└── timestamp_handler.py
config/
└── laneFix_config.py
```

**Các module chính:**  
- gui/: UI components, separated by tabs  
- utils/: Business logic processors  
- config/: Configuration data  
- main.py: Application bootstrap  

**Cách các module nói chuyện với nhau:**  
- Direct imports: gui tabs import utils processors directly  
- Callback functions: Progress callbacks từ processors lên UI  
- Config loading: Tabs load config objects at import time  
- Logging: Centralized logger setup, processors use logging module  

**Chỗ nào tightly coupled:**  
- UI và business logic: Tabs instantiate và call processors directly  
- Config và processors: Hard dependency trên config objects  
- Main window và tabs: Direct instantiation trong initUI()  

**Chỗ nào khó bảo trì:**  
- Mixed concerns: UI code lẫn với processing logic trong tabs  
- Hard-coded criteria: Filtering rules embedded trong processor code  
- No abstraction layers: Direct Polars calls throughout  
- Testing difficulty: UI-dependent processors  

**Chỗ nào có thể tái cấu trúc:**  
- Extract business logic vào separate service layer  
- Create data models cho input/output  
- Abstract data operations vào repository pattern  
- Add dependency injection cho processors  

# 5. ĐÁNH GIÁ ĐIỂM MẠNH / ĐIỂM YẾU

## 5.1. Điểm mạnh

**Code nào ổn:**  
- Polars usage hiệu quả cho data processing performance  
- Memory-efficient processor cho large files (25GB+)  
- Comprehensive logging system  
- Error handling với custom exception handlers  
- UI standards consistency  

**Tư duy nào tốt:**  
- Modular tab-based architecture  
- Separation of concerns (UI vs utils)  
- Memory-aware processing với auto-selection  
- Progress feedback và user experience  
- Config-driven approach cho lane fix  

**Module nào có thể giữ nguyên khi migrate:**  
- utils/ processors (sẽ thành backend services)  
- config/ files (sẽ thành backend config)  
- logger_setup.py (cross-platform logging)  

## 5.2. Điểm yếu

**Vấn đề performance:**  
- UI blocking operations (no threading cho long-running tasks)  
- Memory spikes với large files trong standard processor  
- No caching hoặc optimization cho repeated operations  

**Vấn đề UI:**  
- Desktop-only (PyQt6), không responsive  
- No keyboard shortcuts hoặc advanced UX  
- Hard-coded styling, khó customize  
- No internationalization  

**Vấn đề xử lý dữ liệu:**  
- Hard-coded filtering criteria, khó modify  
- No data validation schemas  
- Limited error recovery (fail-fast approach)  
- No data quality metrics  

**Vấn đề khó migrate:**  
- Tight coupling giữa UI và business logic  
- PyQt6 specific code throughout  
- Synchronous processing model  
- No API abstraction layer  

**Nợ kỹ thuật tích tụ:**  
- Mixed UI/business logic trong tab files  
- No unit tests  
- Hard dependencies trên file system paths  
- No configuration management  
- Code duplication across processors  

## 5.3. Rủi ro khi migrate

- **Architecture mismatch:** Desktop GUI logic không phù hợp với web API model  
- **State management:** Desktop state (files, UI state) cần convert sang RESTful API  
- **Performance expectations:** Web users expect faster response, current sync processing sẽ bottleneck  
- **Data transfer:** Large files (25GB+) qua HTTP sẽ challenging  
- **UI rebuild:** Entire PyQt6 UI cần rewrite trong web technologies  
- **Testing gap:** No existing tests, hard to validate migration  
- **Dependency management:** Polars và PyQt6 dependencies cần replace/adapt  

# 6. ĐỀ XUẤT TÁI CẤU TRÚC (CHO MỤC TIÊU MIGRATE SAU NÀY)

**Tách biệt rõ ràng Backend (Python) và Frontend (Tauri):**

**Backend Architecture (Python với FastAPI):**
- `api/` folder với FastAPI app
- Endpoints cho từng processor: `/api/lmd-clean`, `/api/lane-fix`, `/api/client-feedback`, `/api/add-columns`
- Request/Response models với Pydantic
- Background task processing với Celery cho large files
- File upload/download handling
- Centralized config management
- Database cho job tracking (nếu cần)

**Frontend Architecture (Tauri + Web Tech):**
- Rust backend cho Tauri (file system access, native dialogs)
- Web frontend (React/Vue/Svelte) thay thế PyQt6 tabs
- API client layer gọi backend endpoints
- File picker với native OS dialogs qua Tauri
- Progress indicators cho async operations
- Responsive design cho multiple screen sizes

**Migration Strategy:**
1. **Phase 1:** Extract processors vào standalone Python services (no UI dependencies)
2. **Phase 2:** Create FastAPI wrapper around existing processors
3. **Phase 3:** Build Tauri frontend với web UI calling API
4. **Phase 4:** Migrate config và logging sang backend
5. **Phase 5:** Add authentication và user management nếu cần

**Key Benefits:**
- Scalable: Backend có thể serve multiple frontend clients
- Testable: Business logic isolated từ UI
- Maintainable: Clear separation of concerns
- Cross-platform: Tauri supports Windows/Mac/Linux
- Future-proof: Web technologies dễ update hơn PyQt6

**Potential Challenges:**
- File handling: Large file uploads/downloads qua HTTP
- Real-time feedback: WebSocket cho progress updates
- Native integration: File dialogs, system notifications
- Performance: Ensure Polars operations không bottleneck API response times