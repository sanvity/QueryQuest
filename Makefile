# QueryQuest
#
#   make up && make demo      -- from nothing to a benchmark report
#
PY ?= python
SCALE ?= demo
REPS ?= 20

.PHONY: help up down init generate load refresh bench loadtest report status test demo full clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

up:  ## start PostgreSQL and Redis in Docker
	docker compose up -d
	@echo "waiting for health checks..." && sleep 5

down:  ## stop the containers (data survives in the named volume)
	docker compose down

init:  ## create schemas, partitions, indexes, materialized views
	$(PY) -m queryquest.cli init

generate:  ## write source feeds into data/
	$(PY) scripts/generate_data.py --scale $(SCALE)

load:  ## run the ETL into both schemas
	$(PY) -m queryquest.cli load

refresh:  ## refresh materialized views and ANALYZE
	$(PY) -m queryquest.cli refresh

bench:  ## baseline vs optimized query latency
	$(PY) -m queryquest.cli bench --reps $(REPS)

loadtest:  ## cached vs uncached read throughput
	$(PY) -m queryquest.cli loadtest --duration 30 --concurrency 16

report:  ## regenerate docs/BENCHMARKS.md from recorded measurements
	$(PY) -m queryquest.cli report

status:  ## row counts, recent batches, quarantine summary
	$(PY) -m queryquest.cli status

test:  ## run the test suite
	$(PY) -m pytest -v

demo: init generate load refresh bench loadtest report  ## whole pipeline at demo scale
	@echo "done -- see docs/BENCHMARKS.md"

full:  ## whole pipeline at 10M+ rows (takes a while)
	$(MAKE) SCALE=full generate
	$(MAKE) load refresh bench loadtest report

clean:  ## drop every schema (destructive)
	$(PY) -m queryquest.cli reset --yes
