#!/usr/bin/env python
"""bq2dbt package entry point."""

import sys

from bq2dbt.cli import main

if __name__ == "__main__":
    sys.exit(main())
