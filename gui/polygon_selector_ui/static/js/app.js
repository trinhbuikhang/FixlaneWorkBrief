// PolygonApp Main Class

class PolygonApp {
    constructor() {
        this.bridge = null;
        this.currentMode = 'batch';
        this.startTime = null;
        this.currentProgress = 0;
        this.isProcessing = false;
        this.currentTheme = getStoredTheme();
        this.progressConnected = false;
        this.completedConnected = false;
        this.errorConnected = false;
        
        // Bind event handlers to preserve 'this' context
        this.handleModeButtonClick = this.handleModeButtonClick.bind(this);
        
        this.init();
    }

    async init() {
        try {
            await this.initBridge();
            this.initializeUI();
            this.updateTime();
            
            // Update time every second
            setInterval(() => this.updateTime(), 1000);
            
            console.log('PolygonApp initialized successfully');
        } catch (error) {
            console.error('Failed to initialize PolygonApp:', error);
            handleError(error, 'PolygonApp.init');
        }
    }

    async initBridge() {
        return new Promise((resolve) => {
            // Wait for the bridge to be available (set by template initialization)
            const checkBridge = () => {
                if (window.bridge) {
                    this.bridge = window.bridge;
                    console.log('Bridge connected successfully');
                    
                    // Connect signals
                    this.connectSignals();
                    
                    console.log('Bridge object:', this.bridge);
                    console.log('Available methods:', Object.getOwnPropertyNames(this.bridge));
                    resolve();
                } else {
                    console.log('Waiting for bridge...');
                    setTimeout(checkBridge, 50);
                }
            };
            checkBridge();
        });
    }

    connectSignals() {
        if (!this.bridge) return;
        
        // Connect file selection signal
        if (this.bridge.file_selected) {
            this.bridge.file_selected.connect(this.handleFileSelected.bind(this));
            console.log('file_selected signal connected');
        } else {
            console.warn('file_selected signal not found on bridge');
        }
        
        // Connect processing signals
        if (this.bridge.processing_completed) {
            this.bridge.processing_completed.connect(this.onProcessingCompleted.bind(this));
            console.log('processing_completed signal connected');
        }
        
        if (this.bridge.error_occurred) {
            this.bridge.error_occurred.connect(this.onError.bind(this));
            console.log('error_occurred signal connected');
        }
        
        if (this.bridge.progress_updated) {
            this.bridge.progress_updated.connect(this.updateProgress.bind(this));
            console.log('progress_updated signal connected');
        }
    }

    initializeUI() {
        // Set initial theme
        applyTheme(this.currentTheme);
        
        // Initialize event listeners
        this.initModeButtons();
        this.initKeyboardShortcuts();
        
        // Set initial mode
        this.switchMode(this.currentMode);
        
        console.log('UI initialized');
    }

    initModeButtons() {
        // Remove any existing event listeners to prevent duplicates
        document.querySelectorAll('.mode-btn').forEach(btn => {
            btn.removeEventListener('click', this.handleModeButtonClick);
        });
        
        // Add event listeners
        document.querySelectorAll('.mode-btn').forEach(btn => {
            btn.addEventListener('click', this.handleModeButtonClick);
        });
    }

    handleModeButtonClick(e) {
        const mode = e.currentTarget.dataset.mode;
        if (mode) {
            this.switchMode(mode);
        }
    }

    initKeyboardShortcuts() {
        document.addEventListener('keydown', (e) => {
            if (e.ctrlKey || e.metaKey) {
                switch (e.key) {
                    case '1': 
                        e.preventDefault(); 
                        this.switchMode('batch'); 
                        break;
                    case '2': 
                        e.preventDefault(); 
                        this.switchMode('single'); 
                        break;
                    case 'r': 
                        e.preventDefault(); 
                        this.clearAllFiles(); 
                        break;
                }
            }
        });
    }

    // Theme Management
    toggleTheme() {
        const newTheme = this.currentTheme === 'light' ? 'dark' : 'light';
        this.setTheme(newTheme);
    }

    setTheme(theme) {
        this.currentTheme = theme;
        applyTheme(theme);
        this.showToast(`Theme: ${theme}`, 'info', 1500);
    }

    // Mode Management
    switchMode(mode) {
        if (this.isProcessing) {
            this.showToast('Cannot switch while processing', 'warning');
            return;
        }

        this.currentMode = mode;
        
        // Update mode buttons
        document.querySelectorAll('.mode-btn').forEach(btn => {
            toggleClass(btn, 'active', btn.dataset.mode === mode);
        });
        
        // Update mode panels
        document.querySelectorAll('.mode-panel').forEach(panel => {
            panel.classList.remove('active');
        });
        
        const activePanel = document.getElementById(mode + 'Mode');
        if (activePanel) {
            activePanel.classList.add('active');
        }
        
        // Don't show toast here - prevents duplicate notifications
    }

    // File Selection Methods
    selectPolygonFile() {
        if (this.bridge && this.bridge.select_polygon_file) {
            this.bridge.select_polygon_file('');
        } else {
            this.mockFileSelection('polygon');
        }
    }

    selectDataFolder() {
        if (this.bridge && this.bridge.select_data_folder) {
            this.bridge.select_data_folder('');
        } else {
            this.mockFileSelection('folder');
        }
    }

    selectSingleFile() {
        if (this.bridge && this.bridge.select_single_file) {
            this.bridge.select_single_file('');
        } else {
            this.mockFileSelection('single');
        }
    }

    clearAllFiles() {
        if (this.isProcessing) {
            this.showToast('Cannot clear while processing', 'warning');
            return;
        }

        // Clear UI
        this.updateFileInfo('polygonFileInfo', null);
        this.updateFileInfo('dataFolderInfo', null);
        this.updateFileInfo('singleFileInfo', null);
        
        // Clear bridge
        if (this.bridge && this.bridge.clear_files) {
            this.bridge.clear_files();
        }
        
        this.showToast('All files cleared', 'success', 2000);
    }

    // File Selection Handler
    handleFileSelected(eventType, data) {
        console.log('handleFileSelected called:', eventType, data);
        
        // Ignore null/empty data
        if (!data || !data.name) {
            console.log('Ignoring null/empty data');
            return;
        }
        
        // Log to bridge for debugging
        if (this.bridge && this.bridge.log_message) {
            this.bridge.log_message(`JS: handleFileSelected - ${eventType} - ${JSON.stringify(data)}`);
        }
        
        switch (eventType) {
            case 'polygon_file_selected':
                this.updateFileInfo('polygonFileInfo', data);
                this.showToast('Polygon file selected', 'success');
                break;
            case 'data_folder_selected':
                this.updateFileInfo('dataFolderInfo', data);
                const count = data && data.csv_count !== undefined ? data.csv_count : 0;
                this.showToast(`Folder selected (${count} files)`, 'success');
                break;
            case 'single_file_selected':
                this.updateFileInfo('singleFileInfo', data);
                this.showToast('Data file selected', 'success');
                break;
            default:
                console.warn('Unknown event type:', eventType);
        }
    }

    updateFileInfo(elementId, data) {
        const element = document.getElementById(elementId);
        if (!element) {
            console.error(`Element not found: ${elementId}`);
            return;
        }
        
        // Make element visible
        element.style.display = 'flex';
        element.style.visibility = 'visible';
        element.style.opacity = '1';
        
        if (data && data.name) {
            element.classList.remove('empty');
            element.classList.add('selected');
            
            const icon = elementId.includes('Folder') ? '📂' : '📄';
            const count = data.csv_count !== undefined ? ` (${data.csv_count})` : '';
            
            // Format name
            const pathInfo = formatFilePath(data.name);
            const displayName = truncatePath(pathInfo.display, 30);
            
            element.innerHTML = `<span>${icon} ${displayName}${count}</span>`;
        } else {
            element.classList.add('empty');
            element.classList.remove('selected');
            
            const placeholder = elementId.includes('Folder') ? 'No folder selected' : 'No file selected';
            element.innerHTML = `<span>${placeholder}</span>`;
        }
    }

    // Processing Methods
    startBatchProcessing() {
        const validateAll = document.getElementById('validateAll');
        const options = { 
            validateAll: validateAll ? validateAll.checked : false 
        };
        this.startProcessing('batch', options);
    }

    startSingleProcessing() {
        this.startProcessing('single', {});
    }

    startProcessing(mode, options) {
        console.log(`[DEBUG] startProcessing called with mode: ${mode}`);
        
        if (this.isProcessing) {
            this.showToast('Already processing...', 'warning');
            return;
        }

        this.isProcessing = true;
        this.startTime = Date.now();
        this.currentProgress = 0;
        
        console.log('[DEBUG] About to call setProcessingState(true)');
        this.setProcessingState(true);
        this.updateStatus('Starting...');
        
        // Connect processing signals
        this.connectProcessingSignals();
        
        // Start processing
        if (this.bridge) {
            try {
                if (mode === 'batch') {
                    console.log('Starting batch processing...');
                    this.bridge.process_batch_files_action();
                } else if (mode === 'single') {
                    console.log('Starting single file processing...');
                    this.bridge.process_single_file_action();
                }
            } catch (error) {
                console.error('Error starting processing:', error);
                this.onError('Failed to start processing: ' + error.message);
            }
        } else {
            console.log('Running mock processing (no bridge)');
            this.mockProcessing(mode);
        }
    }

    connectProcessingSignals() {
        if (!this.bridge) return;

        // Connect progress signal
        if (this.bridge.progress_updated && !this.progressConnected) {
            this.bridge.progress_updated.connect((message) => {
                console.log('Progress update:', message);
                this.updateProgress(message);
            });
            this.progressConnected = true;
        }
        
        // Connect completion signal
        if (this.bridge.processing_completed && !this.completedConnected) {
            this.bridge.processing_completed.connect((message) => {
                console.log('Processing completed:', message);
                this.onProcessingCompleted(message);
            });
            this.completedConnected = true;
        }
        
        // Connect error signal
        if (this.bridge.error_occurred && !this.errorConnected) {
            this.bridge.error_occurred.connect((message) => {
                console.log('Error occurred:', message);
                this.onError(message);
            });
            this.errorConnected = true;
        }
    }

    setProcessingState(processing) {
        const batchBtn = document.getElementById('batchProcessBtn');
        const singleBtn = document.getElementById('singleProcessBtn');
        
        console.log(`[DEBUG] setProcessingState: ${processing}, batchBtn exists: ${!!batchBtn}, singleBtn exists: ${!!singleBtn}`);
        
        // Disable/enable buttons
        if (batchBtn) batchBtn.disabled = processing;
        if (singleBtn) singleBtn.disabled = processing;
        
        // Update button content
        if (processing) {
            if (batchBtn) {
                batchBtn.innerHTML = '<span class="hourglass-icon">⏳</span><span>Processing...</span>';
                batchBtn.classList.add('processing-hourglass');
                console.log('[DEBUG] Added processing-hourglass to batch button');
            }
            if (singleBtn) {
                singleBtn.innerHTML = '<span class="hourglass-icon">⏳</span><span>Processing...</span>';
                singleBtn.classList.add('processing-hourglass');
                console.log('[DEBUG] Added processing-hourglass to single button');
            }
        } else {
            if (batchBtn) {
                batchBtn.innerHTML = '<span>🚀</span><span>Start Processing</span>';
                batchBtn.classList.remove('processing-hourglass');
                console.log('[DEBUG] Removed processing-hourglass from batch button');
            }
            if (singleBtn) {
                singleBtn.innerHTML = '<span>🎯</span><span>Process File</span>';
                singleBtn.classList.remove('processing-hourglass');
                console.log('[DEBUG] Removed processing-hourglass from single button');
            }
        }
    }

    updateProgress(message) {
        // Simple progress update - no detailed progress bar
        if (this.currentProgress < 90) {
            this.currentProgress += 10;
        }
        this.updateStatus('Processing...');
    }

    onProcessingCompleted(message) {
        this.isProcessing = false;
        this.updateStatus('Completed');
        this.setProcessingState(false);
        
        // Extract output path from message
        let outputPath = this.extractOutputPath(message);
        
        // Show success modal
        this.showSuccessModal(outputPath || 'Check your data folder for results');
        
        // Show completion toast
        this.showToast('Processing completed successfully!', 'success', 3000);
    }

    onError(message) {
        this.isProcessing = false;
        this.updateStatus('Error');
        this.setProcessingState(false);
        
        this.showToast(`Error: ${message}`, 'error', 5000);
    }

    extractOutputPath(message) {
        if (!message) return '';
        
        // Try to extract path from various message formats
        const patterns = [
            /(?:saved to|Results saved in):\s*([^\n\r]+)/i,
            /(?:Output|Results?):\s*([^\n\r]+)/i,
            /([A-Za-z]:[\\\/][^\n\r]+)/  // Windows path pattern
        ];
        
        for (const pattern of patterns) {
            const match = message.match(pattern);
            if (match) {
                return match[1].trim();
            }
        }
        
        // If message looks like a path itself
        if (message.includes('\\') || message.includes('/')) {
            return message;
        }
        
        return '';
    }

    // UI Update Methods
    updateStatus(status) {
        const element = document.getElementById('statusValue');
        
        // If no status element, just log instead of showing toast
        if (!element) {
            console.log(`[DEBUG] Status update: ${status} (no statusValue element)`);
            // Don't show toast for processing states to avoid popup spam
            return;
        }
        
        // Clear previous classes
        element.classList.remove('completed-status');
        
        switch (status) {
            case 'Processing...':
                element.innerHTML = '<span class="hourglass-flip">⏳</span> ' + status;
                break;
            case 'Starting...':
                element.innerHTML = '<span class="hourglass-bounce">⏳</span> Initializing...';
                break;
            case 'Completed':
                element.innerHTML = '<span class="success-icon">✅</span> ' + status;
                setTimeout(() => element.classList.add('completed-status'), 100);
                break;
            case 'Error':
                element.innerHTML = '<span style="color: var(--error)">❌</span> ' + status;
                break;
            case 'Ready':
                element.innerHTML = '<span>⚡</span> ' + status;
                break;
            default:
                element.textContent = status;
        }
    }

    updateTime() {
        const element = document.getElementById('timeValue');
        if (!element) return;
        
        if (this.startTime && this.isProcessing) {
            const elapsed = Date.now() - this.startTime;
            const seconds = Math.floor(elapsed / 1000);
            const minutes = Math.floor(seconds / 60);
            
            element.textContent = minutes > 0 
                ? `${minutes}:${(seconds % 60).toString().padStart(2, '0')}`
                : `${seconds}s`;
        } else {
            element.textContent = '--';
        }
    }

    // Toast Notifications
    showToast(message, type = 'info', duration = 2500) {
        const container = document.getElementById('toastContainer');
        if (!container) return;
        
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        
        const icons = { 
            info: '💡', 
            success: '✅', 
            error: '❌', 
            warning: '⚠️' 
        };
        const icon = icons[type] || icons.info;
        
        toast.innerHTML = `
            <span class="toast-icon">${icon}</span>
            <span class="toast-message">${message}</span>
        `;
        
        container.appendChild(toast);
        
        // Auto remove
        setTimeout(() => {
            if (toast.parentElement) {
                toast.style.opacity = '0';
                setTimeout(() => toast.remove(), 300);
            }
        }, duration);
    }

    showSuccessModal(outputPath) {
        const modal = document.getElementById('successModal');
        const pathElement = document.getElementById('modalPath');
        
        if (modal && pathElement) {
            pathElement.textContent = outputPath;
            modal.classList.add('show');
            
            // Store path for potential folder opening
            window.currentOutputPath = outputPath;
        }
    }

    // Development/Mock Methods
    mockFileSelection(type) {
        setTimeout(() => {
            const mockData = {
                polygon: { 
                    name: 'sample_polygons.csv', 
                    path: '/mock/sample_polygons.csv' 
                },
                folder: { 
                    name: 'data_folder', 
                    path: '/mock/data_folder', 
                    csv_count: 5 
                },
                single: { 
                    name: 'data_file.csv', 
                    path: '/mock/data_file.csv' 
                }
            };
            
            const data = mockData[type];
            if (data) {
                const eventTypes = {
                    polygon: 'polygon_file_selected',
                    folder: 'data_folder_selected',
                    single: 'single_file_selected'
                };
                this.handleFileSelected(eventTypes[type], data);
            }
        }, 300);
    }

    mockProcessing(mode) {
        let progress = 0;
        const steps = ['Initializing...', 'Reading files...', 'Validating...', 'Processing...', 'Saving results...'];
        
        const interval = setInterval(() => {
            if (progress < steps.length) {
                this.updateProgress(steps[progress]);
                progress++;
            } else {
                clearInterval(interval);
                this.onProcessingCompleted('Mock processing completed! Results saved to /mock/output/results.csv');
            }
        }, 1000);
    }

    // File Drop Handler (future enhancement)
    handleDroppedFiles(files) {
        console.log('Files dropped:', files);
        
        // Basic implementation - can be enhanced
        for (const file of files) {
            if (file.name.endsWith('.csv')) {
                this.showToast(`File dropped: ${file.name}`, 'info');
                // Handle file based on current mode
                break;
            }
        }
    }
}

// Export for module systems (if needed)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = PolygonApp;
}
