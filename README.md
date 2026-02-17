# Telco Customer Churn Prediction

Multi-horizon (30/60/90 day) churn prediction for UK Telecoms LTD broadband customers.

## Architecture

- **Data Platform:** Databricks Lakehouse (Unity Catalog, Delta Live Tables, MLflow)
- **ML:** 3x LightGBM models with SHAP explainability
- **Serving:** FastAPI REST API + Streamlit Dashboard on AKS
- **Infrastructure:** Terraform on Azure (UK South)
- **CI/CD:** Azure DevOps Pipelines (4 pipelines, 3 environments)

## Pipeline

```
Bronze (raw ingest) → Silver (clean/type) → Gold (features) → Train → Score → Monitor
```

## Risk Tiers

| Tier | Horizon | Action | Owner |
|------|---------|--------|-------|
| RED | 30-day | Emergency outbound call | Loyalty |
| AMBER | 60-day | Digital retention campaign | Marketing |
| YELLOW | 90-day | Proactive check-in | Customer Success |
| GREEN | — | Monitor | — |

## Quick Start

See [docs/LLD.md](docs/LLD.md) for the full Low-Level Design document.

```bash
# Prerequisites
az login
terraform -v  # >= 1.5.0
databricks --version

# Deploy infrastructure
cd infrastructure
terraform init -backend-config=environments/dev/backend.tfvars
terraform apply -var-file=environments/dev/terraform.tfvars

# Deploy notebooks + workflows
./deploy/deploy_notebooks.sh
./deploy/deploy_workflows.sh
```

## Project Structure

```
├── .azure-devops/     # CI/CD pipeline definitions
├── infrastructure/    # Terraform modules (10 modules, 3 environments)
├── notebooks/         # Databricks notebooks (00-06)
├── serving/           # FastAPI API + Streamlit Dashboard
├── k8s/               # Kubernetes manifests (Kustomize)
├── deploy/            # Deployment scripts
├── workflows/         # Databricks Workflow JSON
├── tests/             # Unit + integration tests
└── src/               # Shared Python modules
```
