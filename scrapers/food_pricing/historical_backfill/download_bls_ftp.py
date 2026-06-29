#!/usr/bin/env python3
"""
download_bls_ftp.py
Helper script to download and validate BLS FTP files

The BLS server blocks automated downloads, so this script:
1. Checks if files exist
2. Validates they're not HTML error pages
3. Provides clear instructions for manual download
"""

import sys
from pathlib import Path

BLS_DATA_FILE = Path("./bls_ftp_data/ap.data.1.AllData")
BLS_ITEM_FILE = Path("./bls_ftp_data/ap.item")

def check_file(filepath: Path, expected_min_size_mb: float = 1.0) -> bool:
    """
    Check if file exists and is valid (not HTML error page).
    
    Args:
        filepath: Path to file
        expected_min_size_mb: Minimum expected file size in MB
    
    Returns:
        True if valid, False otherwise
    """
    if not filepath.exists():
        print(f"❌ File not found: {filepath}")
        return False
    
    # Check file size
    file_size_mb = filepath.stat().st_size / (1024 * 1024)
    
    if file_size_mb < expected_min_size_mb:
        print(f"⚠️  File too small: {filepath}")
        print(f"   Size: {file_size_mb:.2f} MB (expected >{expected_min_size_mb:.0f} MB)")
        print(f"   This is likely an HTML error page (Access Denied)")
        return False
    
    # Check if HTML
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline().strip()
        if first_line.startswith('<!DOCTYPE') or first_line.startswith('<html'):
            print(f"❌ File is HTML error page: {filepath}")
            return False
    
    print(f"✅ Valid file: {filepath} ({file_size_mb:.2f} MB)")
    return True


def main():
    """Main validation and download helper."""
    print("=" * 70)
    print("BLS FTP FILE DOWNLOAD & VALIDATION HELPER")
    print("=" * 70)
    
    # Create directory if needed
    BLS_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n📁 Directory: {BLS_DATA_FILE.parent.absolute()}")
    
    # Check files
    print("\n🔍 Checking files...")
    data_valid = check_file(BLS_DATA_FILE, expected_min_size_mb=100)  # Expect ~500MB
    item_valid = check_file(BLS_ITEM_FILE, expected_min_size_mb=0.01)  # Expect ~50KB
    
    if data_valid and item_valid:
        print("\n" + "=" * 70)
        print("✅ ALL FILES VALID - Ready to run bls_ftp_backfill.py")
        print("=" * 70)
        return 0
    
    # Provide download instructions
    print("\n" + "=" * 70)
    print("❌ MANUAL DOWNLOAD REQUIRED")
    print("=" * 70)
    print("\nThe BLS server blocks automated downloads (curl, wget).")
    print("You MUST download the files manually through a web browser.")
    print("\n📋 STEP-BY-STEP INSTRUCTIONS:")
    print("\n1. Open this URL in your browser:")
    print("   https://download.bls.gov/pub/time.series/ap/")
    print("\n2. Download ap.data.1.AllData:")
    print("   - Right-click on 'ap.data.1.AllData'")
    print("   - Select 'Save Link As...' or 'Download Linked File'")
    print(f"   - Save to: {BLS_DATA_FILE.absolute()}")
    print("   - Expected size: ~500 MB")
    print("\n3. Download ap.item:")
    print("   - Right-click on 'ap.item'")
    print("   - Select 'Save Link As...' or 'Download Linked File'")
    print(f"   - Save to: {BLS_ITEM_FILE.absolute()}")
    print("   - Expected size: ~50 KB")
    print("\n4. Run this script again to validate:")
    print("   python download_bls_ftp.py")
    print("\n5. Once validated, run the backfill:")
    print("   python bls_ftp_backfill.py")
    
    # If files exist but are HTML, offer to delete them
    if BLS_DATA_FILE.exists() or BLS_ITEM_FILE.exists():
        print("\n" + "-" * 70)
        print("⚠️  HTML error pages detected. Delete them? (y/n): ", end="")
        response = input().strip().lower()
        if response == 'y':
            if BLS_DATA_FILE.exists():
                BLS_DATA_FILE.unlink()
                print(f"   Deleted: {BLS_DATA_FILE}")
            if BLS_ITEM_FILE.exists():
                BLS_ITEM_FILE.unlink()
                print(f"   Deleted: {BLS_ITEM_FILE}")
            print("   ✅ Old files removed. Please download manually now.")
    
    print("\n" + "=" * 70)
    return 1


if __name__ == "__main__":
    sys.exit(main())
