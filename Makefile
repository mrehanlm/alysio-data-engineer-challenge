# Variables
VENV=venv
PYTHON=$(VENV)/bin/python
ALEMBIC=$(VENV)/bin/alembic
UVX=$(VENV)/bin/uvx
PRECOMMIT=$(VENV)/bin/pre-commit

.git/hooks/pre-commit: ## Install pre-commit hooks
	python -m venv $(VENV)
	$(PYTHON) -m pip install pre-commit
	$(PRECOMMIT) install

check-lint-and-formatting: .git/hooks/pre-commit ## Execute check of lint and formatting using existing pre-commit hooks
	$(PRECOMMIT) run -a

# Upgrade to the latest migration (head)
alembic-upgrade: ## Upgrade to the latest migration (head)
	@echo "\nSetting up the database schema"
	$(ALEMBIC) upgrade head
	@echo "Done"

# Downgrade to the previous migration
alembic-downgrade: ## Downgrade to the previous migration
	$(ALEMBIC) downgrade -1

alembic-history: ## Show the migration history
	$(ALEMBIC) history

alembic-current: ## Show the current migration
	$(ALEMBIC) current

generate-revision: ## Generate a new migration. Example usage: `make generate-revision NAME="add_new_table"`
	@if [ -z "$(NAME)" ]; then \
		echo "Error: Please specify the NAME=<migration_name>"; \
		exit 1; \
	fi
	$(ALEMBIC) revision --autogenerate -m "$(NAME)"

alembic-downgrade-all: ## Downgrade to the base migration
	$(ALEMBIC) downgrade base


.venv: ## Setup venv and install dependencies
	@echo "\nSetting up Python environment with dependencies"
	rm -rf $(VENV)
	python -m venv $(VENV)
	$(PYTHON) -m pip install --requirement requirements.txt
	@echo "Done"

run-etl: .venv alembic-upgrade ## Run ETL pipeline
	@echo "\nRunning ETL pipeline"
	$(PYTHON) -m etl.pipeline
	@echo "Done"

tests: .venv ## Run tests
	DATABASE_URL=sqlite:///test_data.db $(PYTHON) -m pytest -vv

run-sample-queries: ## Run sample queries
	$(PYTHON) -m src.sample_queries

.install_uv: ## Install uv
	$(PYTHON) -m pip install uv

docs: .install_uv ## Build and serve docs
	$(UVX) --with mkdocs-material mkdocs serve -a localhost:8001

## Help
help: ## Show the list of all the commands and their help text
	@awk 'BEGIN 	{ FS = ":.*##"; target="";printf "\nUsage:\n  make \033[36m<target>\033[33m\n\nTargets:\033[0m\n" } \
		/^[a-zA-Z_-]+:.*?##/ { if(target=="")print ""; target=$$1; printf " \033[36m%-10s\033[0m %s\n\n", $$1, $$2 } \
		/^([a-zA-Z_-]+):/ {if(target=="")print "";match($$0, "(.*):"); target=substr($$0,RSTART,RLENGTH) } \
		/^\t## (.*)/ { match($$0, "[^\t#:\\\\]+"); txt=substr($$0,RSTART,RLENGTH);printf " \033[36m%-10s\033[0m", target; printf " %s\n", txt ; target=""} \
		/^## .*/ {match($$0, "## (.+)$$"); txt=substr($$0,4,RLENGTH);printf "\n\033[33m%s\033[0m\n", txt ; target=""} \
	' $(MAKEFILE_LIST)

.PHONY: help check-lint-and-formatting migrate-upgrade migrate-downgrade alembic-history alembic-current generate-revision alembic-downgrade-all run-etl tests run-sample-queries docs

.DEFAULT_GOAL := help
