.PHONY: demo up down logs elt report test integration bench clean

# One command: build, start everything, stream for ~3 minutes, run the ELT DAG, print a report.
demo:
	./scripts/demo.sh

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f consumer producer

elt:   ## trigger the Airflow DAG now instead of waiting for the 15-minute schedule
	docker compose exec airflow airflow dags trigger ridestream_elt

report:
	docker compose exec -T postgres psql -U platform -d warehouse -f /dev/stdin < scripts/report.sql

test:
	pytest -q tests/unit

integration:
	docker compose up -d redpanda topics postgres
	pytest -q -m integration tests/integration

bench:
	python scripts/bench.py throughput
	python scripts/bench.py chaos
	python scripts/bench.py latency

clean:
	docker compose down -v --rmi local
