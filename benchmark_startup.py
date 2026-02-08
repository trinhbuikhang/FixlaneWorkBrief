#!/usr/bin/env python3
"""
Startup Benchmark Tool

Measures application startup time with detailed breakdown for each step.
Compares performance before/after optimization.
"""

import sys
import time
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


class StartupBenchmark:
    """Benchmark startup performance"""
    
    def __init__(self):
        self.timestamps = {}
        self.start_time = None
    
    def start(self):
        """Start the benchmark"""
        self.start_time = time.perf_counter()
        self.mark("START")
        print("=" * 60)
        print("STARTUP BENCHMARK")
        print("=" * 60)
    
    def mark(self, label: str):
        """Mark a timestamp"""
        if self.start_time is None:
            self.start_time = time.perf_counter()
        
        elapsed = time.perf_counter() - self.start_time
        self.timestamps[label] = elapsed
        
        if label != "START":
            # Calculate time since previous mark
            keys = list(self.timestamps.keys())
            prev_label = keys[-2] if len(keys) > 1 else "START"
            prev_time = self.timestamps[prev_label]
            delta = elapsed - prev_time
            
            print(f"[{elapsed:6.3f}s] {label:40s} (+{delta:.3f}s)")
    
    def finish(self):
        """Finish benchmark and print summary"""
        total = time.perf_counter() - self.start_time
        
        print("=" * 60)
        print(f"TOTAL STARTUP TIME: {total:.3f}s")
        print("=" * 60)
        
        # Breakdown
        print("\nBREAKDOWN:")
        keys = list(self.timestamps.keys())
        for i in range(1, len(keys)):
            label = keys[i]
            prev_label = keys[i-1]
            delta = self.timestamps[label] - self.timestamps[prev_label]
            percentage = (delta / total) * 100
            print(f"  {label:40s}: {delta:6.3f}s ({percentage:5.1f}%)")
        
        print("\n" + "=" * 60)
        
        return total


def benchmark_imports():
    """Benchmark import times"""
    print("\n📦 IMPORT BENCHMARK")
    print("-" * 60)
    
    imports = [
        ("PyQt6.QtWidgets", "PyQt6 Widgets"),
        ("PyQt6.QtCore", "PyQt6 Core"),
        ("PyQt6.QtGui", "PyQt6 GUI"),
        ("polars", "Polars (if not lazy)"),
        ("gui.main_window", "Main Window"),
        ("gui.modern_styles", "Stylesheets"),
    ]
    
    for module, label in imports:
        start = time.perf_counter()
        try:
            __import__(module)
            elapsed = time.perf_counter() - start
            print(f"  {label:40s}: {elapsed:.3f}s")
        except ImportError as e:
            print(f"  {label:40s}: FAILED ({e})")


def benchmark_full_startup():
    """Benchmark complete startup process"""
    bench = StartupBenchmark()
    bench.start()
    
    # Step 1: Import PyQt6
    bench.mark("Import PyQt6")
    from PyQt6.QtWidgets import QApplication
    
    # Step 2: Create QApplication
    bench.mark("Create QApplication")
    app = QApplication(sys.argv)
    app.setApplicationName("Data Processing Tool")
    
    # Step 3: Import main window
    bench.mark("Import main_window module")
    from gui.main_window import DataCleanerApp
    
    # Step 4: Import and apply stylesheet
    bench.mark("Import stylesheet")
    from gui.modern_styles import MODERN_STYLESHEET
    
    bench.mark("Apply stylesheet")
    app.setStyleSheet(MODERN_STYLESHEET)
    
    # Step 5: Create main window
    bench.mark("Create main window")
    window = DataCleanerApp()
    
    # Step 6: Show window
    bench.mark("Show window")
    window.show()
    
    # Step 7: Render first frame (process events)
    bench.mark("Process first frame")
    app.processEvents()
    
    bench.mark("WINDOW VISIBLE")
    
    total = bench.finish()
    
    # Cleanup
    window.close()
    app.quit()
    
    return total


def benchmark_lazy_vs_eager():
    """Compare lazy loading vs eager loading"""
    print("\n⚡ LAZY vs EAGER LOADING")
    print("-" * 60)
    
    # Test 1: Only import lazy_loader
    start = time.perf_counter()
    from gui.lazy_loader import LazyTabWidget
    lazy_time = time.perf_counter() - start
    print(f"  Import LazyTabWidget:     {lazy_time:.4f}s")
    
    # Test 2: Import all tabs
    start = time.perf_counter()
    from gui.tabs.lmd_cleaner_tab import LMDCleanerTab
    from gui.tabs.laneFix_tab import LaneFixTab
    from gui.tabs.client_feedback_tab import ClientFeedbackTab
    from gui.tabs.add_columns_tab import AddColumnsTab
    eager_time = time.perf_counter() - start
    print(f"  Import ALL tabs (eager):  {eager_time:.4f}s")
    
    print(f"\n  ⚡ Lazy loading saves:    {eager_time - lazy_time:.4f}s")
    print(f"  ⚡ Speed improvement:      {(eager_time/lazy_time):.1f}x faster")


def analyze_module_sizes():
    """Analyze module sizes"""
    print("\n📊 MODULE SIZE ANALYSIS")
    print("-" * 60)
    
    import sys
    
    # Import everything
    from gui.main_window import DataCleanerApp
    from gui.tabs.lmd_cleaner_tab import LMDCleanerTab
    from gui.tabs.laneFix_tab import LaneFixTab
    from gui.tabs.client_feedback_tab import ClientFeedbackTab
    from gui.tabs.add_columns_tab import AddColumnsTab
    
    modules_to_check = [
        'PyQt6.QtWidgets',
        'PyQt6.QtCore',
        'PyQt6.QtGui',
        'polars',
        'gui.main_window',
        'gui.tabs.lmd_cleaner_tab',
        'gui.tabs.laneFix_tab',
        'gui.tabs.client_feedback_tab',
        'gui.tabs.add_columns_tab',
    ]
    
    for module_name in modules_to_check:
        if module_name in sys.modules:
            module = sys.modules[module_name]
            # Estimate size by counting attributes
            attrs = len(dir(module))
            print(f"  {module_name:40s}: ~{attrs:4d} attributes")


def main():
    """Run all benchmarks"""
    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + " " * 10 + "DATA CLEANER STARTUP BENCHMARK" + " " * 17 + "║")
    print("╚" + "═" * 58 + "╝")
    
    # 1. Import benchmark
    benchmark_imports()
    
    # 2. Lazy vs Eager
    benchmark_lazy_vs_eager()
    
    # 3. Module sizes
    analyze_module_sizes()
    
    # 4. Full startup benchmark
    print("\n\n🚀 FULL STARTUP BENCHMARK")
    print("=" * 60)
    total_time = benchmark_full_startup()
    
    # Summary
    print("\n\n📝 SUMMARY")
    print("=" * 60)
    print(f"Total startup time: {total_time:.3f}s")
    
    if total_time < 2.0:
        print("✅ EXCELLENT - Startup time < 2s")
    elif total_time < 3.0:
        print("✅ GOOD - Startup time < 3s")
    elif total_time < 5.0:
        print("⚠️  OK - Startup time < 5s (có thể cải thiện)")
    else:
        print("❌ SLOW - Startup time > 5s (cần optimize)")
    
    print("\n💡 TIPS:")
    print("  - Use lazy loading for tabs")
    print("  - Defer heavy imports until needed")
    print("  - Minimize initial stylesheet size")
    print("  - Profile with: python -m cProfile -s cumtime main.py")
    print("\n")


if __name__ == "__main__":
    main()
