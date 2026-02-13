// UI Event Handlers

// Global event handlers that will be called from HTML
function selectPolygonFile() { 
    if (app) app.selectPolygonFile(); 
}

function selectDataFolder() { 
    if (app) app.selectDataFolder(); 
}

function selectSingleFile() { 
    if (app) app.selectSingleFile(); 
}

function startBatchProcessing() { 
    if (app) app.startBatchProcessing(); 
}

function startSingleProcessing() { 
    if (app) app.startSingleProcessing(); 
}

function toggleTheme() { 
    if (app) app.toggleTheme(); 
}

function clearAllFiles() { 
    if (app) app.clearAllFiles(); 
}

function onProcessingCompleted(message) {
    if (app && typeof app.onProcessingCompleted === 'function') {
        app.onProcessingCompleted(message);
    } else {
        console.log('Processing completed:', message);
    }
}

function onError(message) {
    if (app && typeof app.onError === 'function') {
        app.onError(message);
    } else {
        console.error('Processing error:', message);
    }
}

function updateProgress(message) {
    if (app && typeof app.updateProgress === 'function') {
        app.updateProgress(message);
    } else {
        console.log('Progress update:', message);
    }
}

function clearCache() {
    if (app) app.clearCache();
    else {
        // Fallback for direct execution
        console.log('Clearing cache...');
        // Clear localStorage
        localStorage.clear();
        // Clear sessionStorage  
        sessionStorage.clear();
        // Show confirmation
        alert('Cache cleared successfully!');
    }
}

function showAbout() {
    const modal = document.getElementById('aboutModal');
    if (modal) {
        modal.classList.add('show');
        // Prevent body scrolling when modal is open
        document.body.style.overflow = 'hidden';
    }
}

function closeAboutModal() {
    const modal = document.getElementById('aboutModal');
    if (modal) {
        modal.classList.remove('show');
        // Restore body scrolling
        document.body.style.overflow = '';
    }
}

function showHelp() {
    const modal = document.getElementById('helpModal');
    if (modal) {
        modal.classList.add('show');
        // Prevent body scrolling when modal is open
        document.body.style.overflow = 'hidden';
    }
}

function closeHelpModal() {
    const modal = document.getElementById('helpModal');
    if (modal) {
        modal.classList.remove('show');
        // Restore body scrolling
        document.body.style.overflow = '';
    }
}

function handleFileSelected(eventType, data) { 
    console.log('Global handleFileSelected called:', eventType, data);
    if (app) {
        app.handleFileSelected(eventType, data); 
    } else {
        console.warn('App not ready, storing for later');
        // Store for when app is ready
        window.pendingFileSelection = { eventType, data };
    }
}

// Settings panel functions
function toggleSettings() {
    console.log('toggleSettings called from ui-handlers.js');
    const panel = document.getElementById('settingsPanel');
    const overlay = document.getElementById('settingsOverlay');
    const btn = document.getElementById('hamburgerBtn');
    
    console.log('Elements found:', { panel: !!panel, overlay: !!overlay, btn: !!btn });
    
    if (panel && overlay && btn) {
        const isOpen = panel.classList.contains('open');
        console.log('Panel is open:', isOpen);
        
        if (isOpen) {
            console.log('Closing settings...');
            closeSettings();
        } else {
            console.log('Opening settings...');
            openSettings();
        }
    } else {
        console.log('Missing elements for settings toggle!');
    }
}

function openSettings() {
    console.log('openSettings called');
    const panel = document.getElementById('settingsPanel');
    const overlay = document.getElementById('settingsOverlay');
    const btn = document.getElementById('hamburgerBtn');
    
    if (panel && overlay && btn) {
        panel.classList.add('open');
        overlay.classList.add('show');
        btn.classList.add('active');
        // Keep the hamburger icon (☰) instead of changing to X
        
        // Add body class to prevent scrolling
        document.body.classList.add('settings-open');
        console.log('Settings opened successfully');
    } else {
        console.log('Failed to open settings - missing elements');
    }
}

function closeSettings() {
    console.log('closeSettings called');
    const panel = document.getElementById('settingsPanel');
    const overlay = document.getElementById('settingsOverlay');
    const btn = document.getElementById('hamburgerBtn');
    
    if (panel && overlay && btn) {
        panel.classList.remove('open');
        overlay.classList.remove('show');
        btn.classList.remove('active');
        // No need to change icon as we're keeping it consistent
        
        // Remove body class to allow scrolling
        document.body.classList.remove('settings-open');
        console.log('Settings closed successfully');
    } else {
        console.log('Failed to close settings - missing elements');
    }
}

// Modal functions
function closeSuccessModal() {
    const modal = document.getElementById('successModal');
    if (modal) {
        modal.classList.remove('show');
    }
}

function showSuccessModal(outputPath) {
    const modal = document.getElementById('successModal');
    const pathElement = document.getElementById('outputPath');
    
    if (modal && pathElement) {
        pathElement.textContent = outputPath;
        modal.classList.add('show');
    }
}

// Mode switching
function switchMode(mode) {
    // Show toast notification first so it only appears once
    if (app && typeof app.showToast === 'function') {
        app.showToast(`Mode: ${mode}`, 'info', 1500);
    }
    
    if (app && typeof app.switchMode === 'function') {
        app.switchMode(mode);
    } else {
        // Fallback direct DOM manipulation
        switchModeDirectly(mode);
    }
}

function switchModeDirectly(mode) {
    // Update mode buttons
    const modeButtons = document.querySelectorAll('.mode-btn');
    modeButtons.forEach(btn => {
        const btnMode = btn.getAttribute('data-mode');
        toggleClass(btn, 'active', btnMode === mode);
    });
    
    // Update mode panels
    const modePanels = document.querySelectorAll('.mode-panel');
    modePanels.forEach(panel => {
        const panelMode = panel.id.replace('Mode', '').toLowerCase();
        toggleClass(panel, 'active', panelMode === mode);
    });
    
    // Store current mode
    if (app && app.currentMode !== undefined) {
        app.currentMode = mode;
    }
}

// Keyboard event handlers
function handleKeyboardEvents() {
    document.addEventListener('keydown', function(e) {
        // ESC key to close settings or modals
        if (e.key === 'Escape') {
            // Close settings if open
            const settingsPanel = document.getElementById('settingsPanel');
            if (settingsPanel && settingsPanel.classList.contains('open')) {
                closeSettings();
                return;
            }
            
            // Close success modal if open
            const successModal = document.getElementById('successModal');
            if (successModal && successModal.classList.contains('show')) {
                closeSuccessModal();
                return;
            }
        }
        
        // Ctrl+Shift+S to toggle settings (Windows/Linux)
        // Cmd+Shift+S to toggle settings (Mac)
        if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'S') {
            e.preventDefault();
            toggleSettings();
            return;
        }
        
        // Ctrl+Enter or Cmd+Enter to start processing
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            const activeMode = document.querySelector('.mode-btn.active');
            if (activeMode) {
                const mode = activeMode.getAttribute('data-mode');
                if (mode === 'batch') {
                    startBatchProcessing();
                } else if (mode === 'single') {
                    startSingleProcessing();
                }
            }
            return;
        }
        
        // Ctrl+T or Cmd+T to toggle theme
        if ((e.ctrlKey || e.metaKey) && e.key === 't') {
            e.preventDefault();
            toggleTheme();
            return;
        }
        
        // Ctrl+R or Cmd+R to clear/reset (override default refresh)
        if ((e.ctrlKey || e.metaKey) && e.key === 'r') {
            e.preventDefault();
            clearAllFiles();
            return;
        }
    });
}

// Mouse/Touch event handlers
function handleClickOutside(event) {
    const settingsPanel = document.getElementById('settingsPanel');
    const hamburgerBtn = document.getElementById('hamburgerBtn');
    
    if (settingsPanel && settingsPanel.classList.contains('open')) {
        // Check if click is outside settings panel and not on hamburger button
        if (!settingsPanel.contains(event.target) && 
            !hamburgerBtn.contains(event.target)) {
            closeSettings();
        }
    }
}

// File drop handlers
function handleDragOver(e) {
    e.preventDefault();
    e.stopPropagation();
    e.dataTransfer.dropEffect = 'copy';
    
    // Add visual feedback
    document.body.classList.add('drag-over');
}

function handleDragLeave(e) {
    e.preventDefault();
    e.stopPropagation();
    
    // Remove visual feedback if leaving the window
    if (e.clientX === 0 && e.clientY === 0) {
        document.body.classList.remove('drag-over');
    }
}

function handleDrop(e) {
    e.preventDefault();
    e.stopPropagation();
    
    // Remove visual feedback
    document.body.classList.remove('drag-over');
    
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0 && app && typeof app.handleDroppedFiles === 'function') {
        app.handleDroppedFiles(files);
    }
}

// Window resize handler
function handleResize() {
    const settingsPanel = document.getElementById('settingsPanel');
    
    // Close settings panel on mobile landscape mode for better UX
    if (window.innerWidth < 768 && window.innerHeight < 500) {
        if (settingsPanel && settingsPanel.classList.contains('open')) {
            closeSettings();
        }
    }
}

// Initialize all event listeners
function initializeEventListeners() {
    // Keyboard events
    handleKeyboardEvents();
    
    // Click outside to close settings
    document.addEventListener('click', handleClickOutside);
    
    // File drag and drop (optional - for future enhancement)
    document.addEventListener('dragover', handleDragOver);
    document.addEventListener('dragleave', handleDragLeave);
    document.addEventListener('drop', handleDrop);
    
    // Window resize
    window.addEventListener('resize', throttle(handleResize, 250));
    
    // Prevent default drag behaviors on the document
    document.addEventListener('dragover', e => e.preventDefault());
    document.addEventListener('drop', e => e.preventDefault());
    
    console.log('Event listeners initialized');
}

// Theme system integration
function initializeTheme() {
    // Apply stored theme or system preference
    const savedTheme = getStoredTheme();
    applyTheme(savedTheme);
    
    // Listen for system theme changes
    if (window.matchMedia) {
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
            // Only auto-switch if user hasn't manually set a theme
            const storedTheme = localStorage.getItem('theme');
            if (!storedTheme) {
                applyTheme(e.matches ? 'dark' : 'light');
            }
        });
    }
}

// Export for module systems (if needed)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        selectPolygonFile,
        selectDataFolder,
        selectSingleFile,
        startBatchProcessing,
        startSingleProcessing,
        toggleTheme,
        clearAllFiles,
        handleFileSelected,
        toggleSettings,
        openSettings,
        closeSettings,
        closeSuccessModal,
        showSuccessModal,
        switchMode,
        switchModeDirectly,
        initializeEventListeners,
        initializeTheme
    };
}
