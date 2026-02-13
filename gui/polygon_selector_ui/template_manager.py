"""
Template Manager for Polygon Selector tab UI.
Loads and combines modular template components into final HTML.
"""
from pathlib import Path
from typing import Dict, List, Optional


class TemplateManager:
    """Manages loading and combining of modular template components."""

    def __init__(self, ui_dir: Optional[str] = None):
        if ui_dir is None:
            self.ui_dir = Path(__file__).parent
        else:
            self.ui_dir = Path(ui_dir)
        self.templates_dir = self.ui_dir / "templates"
        self.static_dir = self.ui_dir / "static"
        self.css_dir = self.static_dir / "css"
        self.js_dir = self.static_dir / "js"
        self._validate_directories()

    def _validate_directories(self) -> None:
        for directory in [self.templates_dir, self.css_dir, self.js_dir]:
            if not directory.exists():
                raise FileNotFoundError(f"Required directory not found: {directory}")

    def load_file(self, file_path: Path) -> str:
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    def load_css_files(self, css_files: Optional[List[str]] = None) -> str:
        if css_files is None:
            css_files = ["variables.css", "components.css", "animations.css", "responsive.css"]
        parts = []
        for css_file in css_files:
            css_path = self.css_dir / css_file
            if css_path.exists():
                parts.append(f"/* {css_file} */\n")
                parts.append(self.load_file(css_path))
                parts.append("\n")
        return f"<style>\n{''.join(parts)}\n</style>" if parts else ""

    def load_js_files(self, js_files: Optional[List[str]] = None) -> str:
        if js_files is None:
            js_files = ["utils.js", "ui-handlers.js", "app.js"]
        parts = []
        for js_file in js_files:
            js_path = self.js_dir / js_file
            if js_path.exists():
                parts.append(f"// {js_file}\n")
                parts.append(self.load_file(js_path))
                parts.append("\n")
        return f"<script>\n{''.join(parts)}\n</script>" if parts else ""

    def load_template(self, template_name: str) -> str:
        if not template_name.endswith(".html"):
            template_name += ".html"
        return self.load_file(self.templates_dir / template_name)

    def render_template(
        self,
        template_name: str = "base.html",
        title: str = "Polygon Selector",
        css_files: Optional[List[str]] = None,
        js_files: Optional[List[str]] = None,
        body_template: str = "compact.html",
        additional_head: str = "",
        additional_scripts: str = "",
    ) -> str:
        base_html = self.load_template(template_name)
        body_content = self.load_template(body_template)
        css_imports = self.load_css_files(css_files)
        js_imports = self.load_js_files(js_files)
        initialization_script = self._get_initialization_script()
        replacements = {
            "{{title}}": title,
            "{{css_imports}}": css_imports,
            "{{js_imports}}": js_imports,
            "{{body_content}}": body_content,
            "{{head_content}}": additional_head,
            "{{scripts}}": additional_scripts + initialization_script,
        }
        html = base_html
        for placeholder, content in replacements.items():
            html = html.replace(placeholder, content)
        return html

    def _get_initialization_script(self) -> str:
        return """
<script>
let app;
let channel = null;
function initializeQWebChannel() {
    if (typeof QWebChannel !== 'undefined') {
        try {
            channel = new QWebChannel(qt.webChannelTransport, function(channel) {
                window.bridge = channel.objects.bridge;
                setupBridgeListeners();
            });
        } catch (error) {
            console.error('QWebChannel init failed:', error);
        }
    } else {
        setTimeout(initializeQWebChannel, 100);
    }
}
function setupBridgeListeners() {
    if (!window.bridge) return;
}
document.addEventListener('DOMContentLoaded', () => {
    try {
        initializeQWebChannel();
        if (typeof initializeTheme === 'function') initializeTheme();
        app = new PolygonApp();
        window.app = app;
        if (window.pendingFileSelection) {
            const { eventType, data } = window.pendingFileSelection;
            app.handleFileSelected(eventType, data);
            window.pendingFileSelection = null;
        }
    } catch (error) {
        console.error('Init error:', error);
    }
});
window.addEventListener('error', (e) => {
    if (app && typeof app.showToast === 'function') app.showToast('An error occurred', 'error', 5000);
});
window.addEventListener('unhandledrejection', (e) => {
    if (app && typeof app.showToast === 'function') app.showToast('An error occurred', 'error', 5000);
});
</script>
"""


def get_html_template() -> str:
    """Return the compiled HTML template for the Polygon Selector tab."""
    try:
        manager = TemplateManager()
        return manager.render_template()
    except Exception as e:
        return f"""<!DOCTYPE html><html><head><title>Error</title></head><body>
<h1>Template loading error</h1><p>{e}</p><p>Check gui/polygon_selector_ui/ files.</p></body></html>"""
