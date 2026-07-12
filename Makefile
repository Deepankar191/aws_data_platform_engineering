# Credit Decision Data Platform — developer entrypoints
# See docs/SPEC.md for conventions. Targets are thin wrappers; details live in each component's README.

ENV ?= dev

.PHONY: help
help:
	@echo "Targets:"
	@echo "  make cdc-up            - start local Postgres+Kafka+Connect (Debezium CDC path)"
	@echo "  make cdc-down          - stop the local CDC stack"
	@echo "  make register-connectors - register Debezium source + S3 sink connectors"
	@echo "  make diagram           - render the AWS deployment PNG from diagrams-as-code"
	@echo "  make plan-pdf          - (re)build the execution-plan PDF"
	@echo "  make tf-plan ENV=dev   - terraform plan for the given env"
	@echo "  make soda              - run Soda Core checks against decision_input / marts"

.PHONY: cdc-up cdc-down register-connectors
cdc-up:
	cd ingestion/debezium && docker compose up -d
cdc-down:
	cd ingestion/debezium && docker compose down -v
register-connectors:
	cd ingestion/debezium && ./register-connectors.sh

.PHONY: diagram plan-pdf
diagram:
	cd docs/architecture && python3 generate_diagram.py
plan-pdf:
	cd docs/execution-plan && python3 build_pdf.py

.PHONY: tf-plan
tf-plan:
	cd infra/terraform && terraform init -input=false && terraform plan -var env=$(ENV)

.PHONY: soda
soda:
	soda scan -d credit -c dq/soda/configuration.yml dq/soda/decision_input_checks.yml
