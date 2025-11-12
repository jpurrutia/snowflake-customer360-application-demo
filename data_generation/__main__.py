"""
Entry point for data_generation CLI module.

Usage:
    python -m data_generation generate-customers --count 1000 --output test.csv
"""

from .cli import cli

if __name__ == "__main__":
    cli()
