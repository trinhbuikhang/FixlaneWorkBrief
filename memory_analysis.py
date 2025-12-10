#!/usr/bin/env python3
"""
Memory Analysis and Testing Tool for Large File Processing
"""

import os
import psutil
import time
from pathlib import Path

def get_memory_info():
    """Get current memory information."""
    process = psutil.Process()
    memory_info = process.memory_info()
    virtual_memory = psutil.virtual_memory()
    
    return {
        'process_memory_mb': memory_info.rss / 1024 / 1024,
        'available_memory_mb': virtual_memory.available / 1024 / 1024,
        'total_memory_mb': virtual_memory.total / 1024 / 1024,
        'memory_percent': virtual_memory.percent
    }

def analyze_file_size(file_path):
    """Analyze file size and estimate memory requirements."""
    if not os.path.exists(file_path):
        return None
        
    size_bytes = os.path.getsize(file_path)
    size_gb = size_bytes / (1024 ** 3)
    
    # Estimate memory usage (Polars typically uses 2-3x file size in memory)
    estimated_memory_gb = size_gb * 2.5
    
    return {
        'size_gb': size_gb,
        'estimated_memory_gb': estimated_memory_gb,
        'file_exists': True
    }

def recommend_processing_strategy(lmd_file=None, details_file=None):
    """Recommend processing strategy based on file sizes and available memory."""
    print("🔍 MEMORY AND FILE SIZE ANALYSIS")
    print("=" * 60)
    
    # Get current memory info
    memory_info = get_memory_info()
    available_gb = memory_info['available_memory_mb'] / 1024
    total_gb = memory_info['total_memory_mb'] / 1024
    
    print(f"💾 System Memory:")
    print(f"   • Total RAM: {total_gb:.2f} GB")
    print(f"   • Available: {available_gb:.2f} GB")
    print(f"   • Currently used: {memory_info['memory_percent']:.1f}%")
    print(f"   • Process usage: {memory_info['process_memory_mb']:.1f} MB")
    print()
    
    total_file_size = 0
    total_estimated_memory = 0
    
    # Analyze LMD file
    if lmd_file and os.path.exists(lmd_file):
        lmd_info = analyze_file_size(lmd_file)
        total_file_size += lmd_info['size_gb']
        total_estimated_memory += lmd_info['estimated_memory_gb']
        
        print(f"📄 LMD File Analysis:")
        print(f"   • File: {os.path.basename(lmd_file)}")
        print(f"   • Size: {lmd_info['size_gb']:.2f} GB")
        print(f"   • Estimated memory needed: {lmd_info['estimated_memory_gb']:.2f} GB")
        print()
    
    # Analyze Details file
    if details_file and os.path.exists(details_file):
        details_info = analyze_file_size(details_file)
        total_file_size += details_info['size_gb']
        total_estimated_memory += details_info['estimated_memory_gb']
        
        print(f"📄 Details File Analysis:")
        print(f"   • File: {os.path.basename(details_file)}")
        print(f"   • Size: {details_info['size_gb']:.2f} GB")
        print(f"   • Estimated memory needed: {details_info['estimated_memory_gb']:.2f} GB")
        print()
    
    if total_file_size > 0:
        print(f"📊 Combined Analysis:")
        print(f"   • Total file size: {total_file_size:.2f} GB")
        print(f"   • Estimated memory needed: {total_estimated_memory:.2f} GB")
        print(f"   • Available memory: {available_gb:.2f} GB")
        print()
        
        # Make recommendation
        print("🎯 PROCESSING RECOMMENDATION:")
        print("-" * 40)
        
        if total_estimated_memory > available_gb * 0.8:
            recommendation = "MEMORY-EFFICIENT STREAMING"
            risk = "HIGH RISK"
            color = "🔴"
            explanation = "Files are too large for available memory. Use streaming processing."
        elif total_estimated_memory > available_gb * 0.6:
            recommendation = "MEMORY-EFFICIENT HYBRID"
            risk = "MEDIUM RISK"
            color = "🟡"
            explanation = "Files are large. Use memory-efficient processing for safety."
        elif total_file_size > 10:
            recommendation = "CONSIDER MEMORY-EFFICIENT"
            risk = "LOW RISK"
            color = "🟢"
            explanation = "Files are large but should fit in memory. Consider streaming for very large datasets."
        else:
            recommendation = "STANDARD IN-MEMORY"
            risk = "NO RISK"
            color = "🟢"
            explanation = "Files are small enough for standard in-memory processing."
        
        print(f"{color} Strategy: {recommendation}")
        print(f"   • Memory Risk: {risk}")
        print(f"   • Explanation: {explanation}")
        print()
        
        # Performance estimates
        if total_file_size < 5:
            estimated_time = "5-15 minutes"
        elif total_file_size < 15:
            estimated_time = "15-45 minutes"
        elif total_file_size < 30:
            estimated_time = "45-90 minutes"
        else:
            estimated_time = "1.5-3 hours"
        
        print(f"⏱️ Estimated Processing Time: {estimated_time}")
        print()
        
        # Optimization suggestions
        print("💡 OPTIMIZATION SUGGESTIONS:")
        print("-" * 40)
        
        if total_estimated_memory > available_gb * 0.5:
            print("• Close other applications to free up memory")
            print("• Use smaller chunk sizes (10,000-25,000 rows)")
            print("• Enable memory-efficient streaming processing")
            
        if total_file_size > 20:
            print("• Consider processing during off-peak hours")
            print("• Ensure sufficient disk space for temporary files")
            print("• Monitor system performance during processing")
            
        if available_gb < 8:
            print("• Consider upgrading system RAM")
            print("• Use very small chunk sizes (5,000 rows)")
            print("• Process files separately if possible")
    
    print("\n🔧 TECHNICAL DETAILS:")
    print("-" * 40)
    print("• Memory-efficient processing uses temporary index files")
    print("• Streaming processing handles unlimited file sizes")
    print("• Chunk size auto-adjusts based on available memory")
    print("• Progress monitoring prevents memory leaks")

def main():
    print("DataCleaner - Memory Analysis Tool")
    print("=" * 50)
    print()
    
    # Example usage
    print("Usage Examples:")
    print("-" * 20)
    print("1. Analyze system only:")
    print("   python memory_analysis.py")
    print()
    print("2. Analyze specific files:")
    print("   Edit the file paths below and run")
    print()
    
    # You can specify file paths here for analysis
    lmd_file = None  # r"C:\path\to\your\lmd_file.csv"
    details_file = None  # r"C:\path\to\your\details_file.csv"
    
    # If files specified, analyze them
    if lmd_file or details_file:
        recommend_processing_strategy(lmd_file, details_file)
    else:
        # Just show system info
        recommend_processing_strategy()
    
    print("\n" + "=" * 60)
    print("READY TO PROCESS LARGE FILES!")
    print("The application will automatically choose the best processing method.")
    print("=" * 60)

if __name__ == "__main__":
    main()