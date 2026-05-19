# Makefile

.PHONY: setup lint format test integration-test up down \
	tf-init tf-plan tf-apply tf-output \
	upload-spark-jobs dbt-run dbt-test eval

# -------------------------------------------------------------------
# Terraform Outputs
# -------------------------------------------------------------------

TF_OUTPUTS_FILE=infra/terraform/generated/outputs.json

TLC_S3_BUCKET=$(shell python -c "import json;print(json.load(open('$(TF_OUTPUTS_FILE)'))['datalake_bucket_name']['value'])")

EMR_APPLICATION_ID=$(shell python -c "import json;print(json.load(open('$(TF_OUTPUTS_FILE)'))['emr_application_id']['value'])")

EMR_EXECUTION_ROLE_ARN=$(shell python -c "import json;print(json.load(open('$(TF_OUTPUTS_FILE)'))['emr_execution_role_arn']['value'])")
# -------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------

setup:
	pip install -e ".[dev]"
	pre-commit install

# -------------------------------------------------------------------
# Lint / Format
# -------------------------------------------------------------------

lint:
	ruff check .
	black --check .

format:
	ruff check --fix .
	black .

# -------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------

test:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

integration-test:
	pytest tests/integration/ -v -m integration

# -------------------------------------------------------------------
# Docker Services
# -------------------------------------------------------------------

up:
	docker-compose -f infra/docker-compose.yml up -d

	@echo "Airflow:  http://localhost:8080"
	@echo "Superset: http://localhost:8088"
	@echo "MLflow:   http://localhost:5000"
	@echo "RAG API:  http://localhost:8000/docs"

down:
	docker-compose -f infra/docker-compose.yml down

# -------------------------------------------------------------------
# Terraform
# -------------------------------------------------------------------

tf-init:
	cd infra/terraform && terraform init

tf-plan:
	cd infra/terraform && terraform plan -var-file=vars.tfvars

tf-apply:
	cd infra/terraform && terraform apply -var-file=vars.tfvars

tf-output:
	cd infra/terraform && terraform output -json > generated/outputs.json
generate-env:
	python infra/terraform/scripts/generate_env.py
# -------------------------------------------------------------------
# Spark Jobs
# -------------------------------------------------------------------

upload-spark-jobs:
	@echo "Uploading Spark jobs..."
	@echo "Bucket: $(TLC_S3_BUCKET)"

	aws s3 sync spark_jobs/ s3://$(TLC_S3_BUCKET)/spark-scripts/ \
		--exclude "__pycache__/*" \
		--exclude "*.pyc"

	@echo "Spark jobs uploaded successfully."

list-spark-jobs:
	aws s3 ls s3://$(TLC_S3_BUCKET)/spark-scripts/

# -------------------------------------------------------------------
# dbt
# -------------------------------------------------------------------

dbt-run:
	cd dbt_project && dbt run --profiles-dir .

dbt-test:
	cd dbt_project && dbt test --profiles-dir .

# -------------------------------------------------------------------
# Evaluation
# -------------------------------------------------------------------

eval:
	python serving/rag_app/eval_harness.py

# -------------------------------------------------------------------
# Debug
# -------------------------------------------------------------------

show-tf-vars:
	@echo "TLC_S3_BUCKET=$(TLC_S3_BUCKET)"
	@echo "EMR_APPLICATION_ID=$(EMR_APPLICATION_ID)"
	@echo "EMR_EXECUTION_ROLE_ARN=$(EMR_EXECUTION_ROLE_ARN)"