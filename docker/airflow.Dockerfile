# syntax=docker/dockerfile:1
FROM apache/airflow:3.1.0-python3.11
# dbt lives in its own venv so its pins never fight Airflow's constraints.
USER root
RUN python -m venv /opt/dbt-venv && chown -R airflow: /opt/dbt-venv
USER airflow
# Optional `--secret id=ca_bundle,src=...` for builds behind a TLS-inspecting proxy.
RUN --mount=type=secret,id=ca_bundle,required=false,uid=50000 \
    if [ -f /run/secrets/ca_bundle ]; then export SSL_CERT_FILE=/run/secrets/ca_bundle PIP_CERT=/run/secrets/ca_bundle; fi; \
    /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-postgres==1.10.2
