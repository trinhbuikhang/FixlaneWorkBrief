from PyQt6.QtWidgets import QCheckBox
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QMouseEvent


class RobustCheckBox(QCheckBox):
    """
    QCheckBox variant that reinforces mouse handling on Windows 11.

    - Ensures touch events don't interfere with mouse clicks.
    - Forces a toggle + signal emit if Qt's default handling fails to change state
      (e.g. when running from a network drive and events are filtered).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Prefer keyboard focus and disable touch events that can swallow clicks
        attr = getattr(Qt, "WidgetAttribute", None)
        if attr is not None and hasattr(attr, "WA_AcceptTouchEvents"):
            try:
                self.setAttribute(attr.WA_AcceptTouchEvents, False)
            except Exception:
                pass

        try:
            self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        except Exception:
            pass

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:
        """
        On left-button release, let Qt handle the event first.
        If the checked state did not change, explicitly toggle and emit signals.
        """
        if event.button() == Qt.MouseButton.LeftButton:
            before = self.isChecked()

            # Let the base class handle the event normally first
            super().mouseReleaseEvent(event)

            after = self.isChecked()
            if before == after:
                # Qt did not toggle (e.g. platform/network-drive quirk) -> force toggle
                new_state = not before
                self.setChecked(new_state)
                try:
                    self.clicked.emit(new_state)
                except Exception:
                    pass
                try:
                    self.toggled.emit(new_state)
                except Exception:
                    pass
        else:
            super().mouseReleaseEvent(event)

