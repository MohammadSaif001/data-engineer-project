"""Backward-compatible Bronze entrypoint.

Production-ready implementation lives in `python/bronze/jobs.py`.
"""

from bronze.jobs import run_bronze_pipeline


def main() -> None:
    run_bronze_pipeline()


if __name__ == "__main__":
    main()
