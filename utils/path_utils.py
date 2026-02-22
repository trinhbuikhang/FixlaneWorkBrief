"""
Path utilities for cross-platform and network-path handling.
Used to avoid Permission denied when writing temp files on network shares.
"""
import os


def is_network_path(path: str) -> bool:
    """
    Return True if *path* is on a network share (UNC or network drive).
    Writing temp files on network shares often fails with Permission denied,
    so callers should use system temp or write-then-move for such paths.
    """
    try:
        resolved = os.path.abspath(path)
        # UNC: \\server\share or //server/share
        if resolved.startswith("\\\\") or resolved.startswith("//"):
            return True
        # Windows: check drive type (e.g. DRIVE_REMOTE)
        if os.name == "nt" and len(resolved) >= 2 and resolved[1] == ":":
            import ctypes
            drive = resolved[:2] + "\\"
            if os.path.exists(drive):
                try:
                    drive_type = ctypes.windll.kernel32.GetDriveTypeW(drive)  # type: ignore[attr-defined]
                    # DRIVE_REMOTE = 4
                    if drive_type == 4:
                        return True
                except Exception:
                    pass
    except Exception:
        pass
    return False
