.PHONY: format lint check fix clean clean-pyc clean-cache clean-logs clean help migrate

# Variabili
PYTHON := python3
VENV := .venv
PYCACHE := __pycache__
LOGS_DIR := logs
CACHE_DIR := .cache

# Target principali
format:
	ruff format .

lint:
	ruff check .

fix:
	ruff check --fix .

check: format lint 

fix-all: format fix 

migrate:
	$(PYTHON) main.py

# Target per la pulizia
clean-pyc:
	find . -type d -name "$(PYCACHE)" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete

clean-cache:
	rm -rf $(CACHE_DIR)
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +

clean-logs:
	rm -rf $(LOGS_DIR)
	find . -type f -name "*.log" -delete

clean: clean-pyc clean-cache clean-logs

# Target di utilità
help:
	@echo "Target disponibili:"
	@echo "  format      - Formatta il codice usando ruff"
	@echo "  lint        - Esegue il linting del codice"
	@echo "  fix         - Corregge automaticamente i problemi di linting"
	@echo "  check       - Esegue format e lint"
	@echo "  fix-all     - Esegue format e fix"
	@echo "  migrate     - Esegue la migrazione del database"
	@echo "  clean-pyc   - Rimuove i file Python compilati"
	@echo "  clean-cache - Rimuove le directory di cache"
	@echo "  clean-logs  - Rimuove i file di log"
	@echo "  clean-all   - Esegue tutte le operazioni di pulizia"
	@echo "  help        - Mostra questo messaggio di aiuto" 