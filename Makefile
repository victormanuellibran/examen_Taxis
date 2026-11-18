PYTHON ?= python
MES ?= 2025-06

.PHONY: all calidad test
all:
	$(PYTHON) -m src.pipeline --mes $(MES)

calidad:
	$(PYTHON) -m src.pipeline --mes $(MES) --hasta-calidad

test:
	$(PYTHON) -m pytest -q
