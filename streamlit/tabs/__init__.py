"""
Streamlit tab modules for Customer 360 Analytics Platform
"""

# Add parent directory to path so tabs can import utils
import sys
import os
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
