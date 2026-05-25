# Real-Time Crypto Market Analytics Pipeline on Kubernetes

An end-to-end streaming data pipeline that ingests live cryptocurrency prices, processes them in real time, persists analytics, and visualizes the results on a live dashboard — all orchestrated on Kubernetes.

## Architecture

```
[CoinGecko API]
      ↓
[Producer]      ── Deployment (stateless)
      ↓
[Kafka]         ── StatefulSet + PVC
      ↓
[Consumer]      ── Deployment (stateless, scalable)
      ↓
[PostgreSQL]    ── StatefulSet + PVC
      ↓
[Dashboard]     ── Deployment + Service + Ingress
```

## Tech Stack

- **Containers**: Docker
- **Orchestration**: Kubernetes (local: Minikube)
- **Streaming**: Apache Kafka
- **Storage**: PostgreSQL
- **Processing**: Python (kafka-python, psycopg2, pandas)
- **Dashboard**: Streamlit
- **Data source**: CoinGecko public API

## Kubernetes Concepts Demonstrated

| Concept | Used For |
|---|---|
| Deployment | Stateless workloads (producer, consumer, dashboard) |
| StatefulSet | Stateful workloads (Kafka, Postgres) |
| Service | Stable internal DNS between components |
| Ingress | External access to the dashboard |
| ConfigMap | Non-secret configuration (API URLs, topic names) |
| Secret | Database credentials |
| PersistentVolumeClaim | Durable storage for Kafka and Postgres |

## Project Status

🚧 In active development. See commit history for progress.

## Local Development (Phase 1)

Run the pipeline locally with Docker Compose before deploying to Kubernetes:

```bash
docker compose up --build
```

## Kubernetes Deployment (Phase 2)

Coming soon.
