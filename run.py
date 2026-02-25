#!/usr/bin/env python3
"""
Quick launcher for FinGuard AI streaming pipeline.
Simple wrapper to start the fraud detection system.
"""

import sys
import subprocess


def main():
    """Launch the streaming pipeline."""
    
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║                FinGuard AI                             ║
    ║      Real-Time Fraud Detection (Pathway Streaming)       ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    print(" Launching streaming pipeline...\n")
    
    try:
        # Simply exec the pipeline
        subprocess.run([sys.executable, "pipeline.py"] + sys.argv[1:])
    except KeyboardInterrupt:
        print("\n\n  Stopped by user.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
