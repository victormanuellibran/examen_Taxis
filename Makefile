PYTHON ?= python
MES ?= 2025-06

.PHONY: all calidad test insights dashboard dagster monitoreo dbt-docs
all:
	$(PYTHON) -m src.pipeline --mes $(MES)

calidad:
	$(PYTHON) -m src.pipeline --mes $(MES) --hasta-calidad

test:
	$(PYTHON) -m pytest -q

insights:
	$(PYTHON) -m src.insights

dashboard:
	$(PYTHON) -m streamlit run dashboard/app.py

dagster:
	dagster dev -m src.orchestration

monitoreo:
	docker compose --profile monitoreo up -d prometheus grafana dozzle

dbt-docs:
	$(PYTHON) -m src.dbt_runner docs --mes $(MES) --serve
