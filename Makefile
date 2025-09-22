.PHONY: help dev test pytest smoke dags

help:
	@echo "Targets: dev, test, pytest, smoke, dags"

dev:
	astro dev start

test:
	astro dev pytest

pytest:
	astro dev pytest --args "-q"

smoke:
	astro dev bash -s --scheduler -- python dags/scripts/duckdb_smoke.py

dags:
	astro dev run dags list
