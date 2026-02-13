// Utility Functions

// Copy to clipboard functionality
function copyToClipboard(text) {
    if (navigator.clipboard) {
        navigator.clipboard.writeText(text).then(() => {
            if (app) app.showToast('📋 Path copied!', 'success', 1500);
        }).catch(() => {
            fallbackCopyToClipboard(text);
        });
    } else {
        fallbackCopyToClipboard(text);
    }
}

function fallbackCopyToClipboard(text) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.top = '0';
    textArea.style.left = '0';
    textArea.style.width = '2em';
    textArea.style.height = '2em';
    textArea.style.padding = '0';
    textArea.style.border = 'none';
    textArea.style.outline = 'none';
    textArea.style.boxShadow = 'none';
    textArea.style.background = 'transparent';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        const successful = document.execCommand('copy');
        if (successful && app) {
            app.showToast('📋 Path copied!', 'success', 1500);
        }
    } catch (err) {
        console.error('Could not copy text: ', err);
        if (app) {
            app.showToast('❌ Copy failed', 'error', 2000);
        }
    }
    
    document.body.removeChild(textArea);
}

// Theme utilities
function getSystemTheme() {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    const themeIcon = document.getElementById('themeIcon');
    if (themeIcon) {
        themeIcon.textContent = theme === 'dark' ? '☀️' : '🌙';
    }
    
    // Store theme preference - silently handle localStorage errors
    try {
        localStorage.setItem('theme', theme);
    } catch (e) {
        // localStorage not available in QWebEngine context - ignore silently
    }
}

function getStoredTheme() {
    try {
        return localStorage.getItem('theme') || getSystemTheme();
    } catch (e) {
        // localStorage not available in QWebEngine context - use system theme
        return getSystemTheme();
    }
}

// File path utilities
function formatFilePath(path) {
    if (!path) return '';
    
    // Normalize path separators
    const normalizedPath = path.replace(/\\/g, '/');
    
    // Get filename for display
    const filename = normalizedPath.split('/').pop();
    const directory = normalizedPath.substring(0, normalizedPath.lastIndexOf('/'));
    
    return {
        full: path,
        filename: filename,
        directory: directory,
        display: filename || path
    };
}

function truncatePath(path, maxLength = 50) {
    if (!path || path.length <= maxLength) return path;
    
    const start = Math.floor((maxLength - 3) / 2);
    const end = Math.ceil((maxLength - 3) / 2);
    
    return path.substring(0, start) + '...' + path.substring(path.length - end);
}

// DOM utilities
function createElement(tag, className = '', textContent = '') {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (textContent) element.textContent = textContent;
    return element;
}

function removeElement(selector) {
    const element = document.querySelector(selector);
    if (element) {
        element.remove();
        return true;
    }
    return false;
}

function toggleClass(element, className, force = null) {
    if (!element) return false;
    
    if (force !== null) {
        if (force) {
            element.classList.add(className);
        } else {
            element.classList.remove(className);
        }
    } else {
        element.classList.toggle(className);
    }
    
    return element.classList.contains(className);
}

// Animation utilities
function fadeIn(element, duration = 300) {
    if (!element) return Promise.resolve();
    
    return new Promise(resolve => {
        element.style.opacity = '0';
        element.style.display = 'block';
        
        const startTime = performance.now();
        
        function animate(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);
            
            element.style.opacity = progress;
            
            if (progress < 1) {
                requestAnimationFrame(animate);
            } else {
                resolve();
            }
        }
        
        requestAnimationFrame(animate);
    });
}

function fadeOut(element, duration = 300) {
    if (!element) return Promise.resolve();
    
    return new Promise(resolve => {
        const startOpacity = parseFloat(element.style.opacity) || 1;
        const startTime = performance.now();
        
        function animate(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);
            
            element.style.opacity = startOpacity * (1 - progress);
            
            if (progress < 1) {
                requestAnimationFrame(animate);
            } else {
                element.style.display = 'none';
                resolve();
            }
        }
        
        requestAnimationFrame(animate);
    });
}

// Event utilities
function debounce(func, wait, immediate = false) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            timeout = null;
            if (!immediate) func(...args);
        };
        const callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func(...args);
    };
}

function throttle(func, limit) {
    let inThrottle;
    return function(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// Validation utilities
function isValidPath(path) {
    if (!path || typeof path !== 'string') return false;
    
    // Basic path validation - could be enhanced based on requirements
    const invalidChars = /[<>:"|?*]/;
    return !invalidChars.test(path) && path.trim().length > 0;
}

function sanitizeFileName(filename) {
    if (!filename) return '';
    
    // Remove or replace invalid characters
    return filename
        .replace(/[<>:"|?*]/g, '')
        .replace(/\s+/g, ' ')
        .trim();
}

// Browser detection
function getBrowserInfo() {
    const ua = navigator.userAgent;
    let browser = 'Unknown';
    
    if (ua.includes('Chrome') && !ua.includes('Edge')) browser = 'Chrome';
    else if (ua.includes('Firefox')) browser = 'Firefox';
    else if (ua.includes('Safari') && !ua.includes('Chrome')) browser = 'Safari';
    else if (ua.includes('Edge')) browser = 'Edge';
    
    return {
        name: browser,
        userAgent: ua,
        supportsClipboard: !!navigator.clipboard,
        supportsFileAPI: !!(window.File && window.FileReader && window.FileList && window.Blob)
    };
}

// Error handling utilities
function handleError(error, context = 'Unknown') {
    console.error(`Error in ${context}:`, error);
    
    if (app && typeof app.showToast === 'function') {
        const message = error.message || 'An unexpected error occurred';
        app.showToast(`❌ ${message}`, 'error', 3000);
    }
}

// Export for module systems (if needed)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        copyToClipboard,
        fallbackCopyToClipboard,
        getSystemTheme,
        applyTheme,
        getStoredTheme,
        formatFilePath,
        truncatePath,
        createElement,
        removeElement,
        toggleClass,
        fadeIn,
        fadeOut,
        debounce,
        throttle,
        isValidPath,
        sanitizeFileName,
        getBrowserInfo,
        handleError
    };
}
