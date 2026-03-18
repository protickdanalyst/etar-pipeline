<div align="center">

# 🔄 ETAR Pipeline

### End-to-End Streaming Data Pipeline

**Ingest → Store → Analyse → Visualise — fully automated, every 60 seconds**

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.9-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-3.0-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-25.6-FFCC01?style=flat&logo=clickhouse&logoColor=black)](https://clickhouse.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com)
[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?style=flat&logo=githubactions&logoColor=white)](https://github.com/features/actions)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

<br/>

> A production-hardened data pipeline that simulates e-commerce user events, streams them through Kafka into ClickHouse, runs per-minute Spark analysis, and displays live results on a Streamlit dashboard — all with one command.

<br/>

[**Quick Start**](#%EF%B8%8F-installation--setup) · [**Architecture**](#-architecture) · [**Tech Stack**](#%EF%B8%8F-tech-stack) · [**What I Built**](#-key-engineering-decisions)

</div>

---

## 📌 Overview

ETAR Pipeline is a **complete, production-grade data engineering system** built to demonstrate real-world skills beyond "data gets into a database."

It covers the full lifecycle:

| Stage | What happens |
|---|---|
| **Simulate** | Python multi-process producer generates synthetic e-commerce events (view, cart, checkout, payment, search) |
| **Ingest** | Events flow through Kafka with Avro schema validation — malformed messages go to a dead-letter queue, never block the pipeline |
| **Store** | ClickHouse persists every event with millisecond precision, day-level partitioning, and a 90-day TTL |
| **Orchestrate** | Airflow DAG fires every minute, extracts the previous minute's data, and triggers the Spark job |
| **Analyse** | PySpark reads a Parquet file from MinIO, computes success/error rates per event type, writes results atomically |
| **Visualise** | Streamlit dashboard auto-refreshes with a live bar chart, event counts, and processing time |

**96 files · 3,000+ lines of Python · 17 test files · 5 isolated test suites · GitHub Actions CI**

---

## 🏗 Architecture

```
                         ┌─────────────────────────────────────┐
                         │           INGESTION LAYER            │
   ┌────────────┐  Avro  │  ┌──────────┐      ┌─────────────┐ │
   │  Producer  │───────▶│  │  Kafka   │─────▶│   Kafka     │ │
   │ (4 workers)│        │  │  Broker  │      │   Connect   │ │
   └────────────┘        │  └──────────┘      │  + DLQ      │ │
                         │  ┌──────────┐      └──────┬──────┘ │
                         │  │ Schema   │             │ Sink   │
                         │  │Registry  │             │        │
                         │  └──────────┘             │        │
                         └─────────────────────────--│--------┘
                                                      │
                                                      ▼
                         ┌─────────────────────────────────────┐
                         │          STORAGE LAYER               │
                         │  ┌──────────────────────────────┐   │
                         │  │  ClickHouse (MergeTree)      │   │
                         │  │  • Partitioned by day        │   │
                         │  │  • ORDER BY event_type,      │   │
                         │  │    user_id, event_timestamp  │   │
                         │  │  • 90-day TTL                │   │
                         │  └──────────────────────────────┘   │
                         └──────────────────┬──────────────────┘
                                            │ COUNT guard
                                            │ → Parquet extract
                                            ▼
                         ┌─────────────────────────────────────┐
                         │        ORCHESTRATION LAYER           │
                         │  ┌──────────────────────────────┐   │
                         │  │     Airflow (Celery)          │   │
                         │  │  stream → spark → dashboard  │   │
                         │  │  schedule: every 60 seconds  │   │
                         │  └──────────┬───────────────────┘   │
                         └────────────-│-──────────────---------┘
                                       │ s3a://
                              ┌────────▼──────────┐
                              │  MinIO (S3)        │
                              │  Parquet + JSON    │
                              │  atomic writes     │
                              └────────┬──────────┘
                                       │ PySpark
                         ┌─────────────▼────────────────────────┐
                         │         PROCESSING LAYER              │
                         │  ┌────────────────────────────────┐  │
                         │  │  Spark (2 workers)             │  │
                         │  │  • Reads Parquet from MinIO    │  │
                         │  │  • pivot() by event_type       │  │
                         │  │  • Writes JSON atomically      │  │
                         │  └────────────────────────────────┘  │
                         └──────────────────┬────────────────────┘
                                            │ HTTP POST
                         ┌──────────────────▼────────────────────┐
                         │         PRESENTATION LAYER             │
                         │  ┌──────────────┐  ┌───────────────┐  │
                         │  │  FastAPI     │  │   Streamlit   │  │
                         │  │  /report     │◀─│   Dashboard   │  │
                         │  │  /health     │  │  (auto-polls) │  │
                         │  └──────────────┘  └───────────────┘  │
                         └───────────────────────────────────────┘
```

---

## 🚀 Features

### Data Pipeline
- ✅ **Multi-process Kafka producer** — configurable worker count, graceful SIGTERM shutdown, guaranteed buffer flush before exit
- ✅ **Avro schema enforcement** — Schema Registry validates every message; schema auto-registers on first run
- ✅ **Dead-letter queue (DLQ)** — connector set to `errors.tolerance: all`; bad messages park in `<topic>.dlq`, pipeline never stalls
- ✅ **COUNT guard before S3 write** — checks ClickHouse row count before opening any writer; eliminates empty-file write-then-delete race condition
- ✅ **Atomic MinIO writes** — upload to staging key → `copy_object` to final key → delete staging; readers never see a partial file
- ✅ **Minutely Airflow DAG** — `max_active_runs=1`, `fail_fast=True`, `retry_delay=30s`, webhook alerting on failure

### Infrastructure
- ✅ **All application code baked into Docker images** — no source volume mounts in production; fully immutable containers
- ✅ **Healthchecks on every service** — including Kafka Connect (absent in most tutorials)
- ✅ **Dependency ordering enforced** — `condition: service_healthy` throughout; startup races eliminated
- ✅ **MinIO init uses retry loop** — not `sleep 5`; exits cleanly when MinIO is actually ready
- ✅ **2 Spark workers deployed** — matches `num_executors=2` in SparkSubmitOperator (most setups get this wrong)

### Developer Experience
- ✅ **Single-command lifecycle** — `make init-env` → `make build` → `make up` → `make test` → `make down`
- ✅ **10 env.example files** — every required variable documented with generation instructions
- ✅ **5 isolated test suites** — each runs in its own Docker Compose stack with its own network
- ✅ **GitHub Actions CI** — build → parallel test suites → airflow last; images tagged by commit SHA

---

## 🛠️ Tech Stack

<table>
<tr>
<td valign="top" width="50%">

**Core Pipeline**
| Component | Technology |
|---|---|
| Event streaming | Apache Kafka 7.9 (KRaft) |
| Schema registry | Confluent Schema Registry |
| Connector | Kafka Connect ClickHouse Sink |
| Data format | Apache Avro / Parquet |
| Data warehouse | ClickHouse 25.6 |
| Object storage | MinIO (S3-compatible) |
| Orchestration | Apache Airflow 3.0 |
| Processing | Apache Spark 3.5 + PySpark |

</td>
<td valign="top" width="50%">

**Application Layer**
| Component | Technology |
|---|---|
| Dashboard API | FastAPI + Pydantic |
| Dashboard UI | Streamlit + Matplotlib |
| Task queue | Redis |
| Metadata store | PostgreSQL 17 |
| Language | Python 3.11 |
| Serialisation | PyArrow |
| Containerisation | Docker + Compose |
| CI/CD | GitHub Actions |

</td>
</tr>
</table>

---

## 📷 Screenshots / Demo

> **Live Dashboard** — refreshes every ~45 seconds with the latest Spark analysis

```
┌─────────────────────────────────────────────────────────────────────┐
│  Event Analysis Dashboard                                           │
│                                                                     │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                          │
│  │ █   │ │ █   │ │ █   │ │ █   │ │ █   │  ■ Success                │
│  │ █ ▓ │ │ █ ▓ │ │ █ ▓ │ │ █ ▓ │ │ █ ▓ │  ▓ Error                 │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                          │
│  Add to  Checkout Payment  Search  View                             │
│  Cart                              Product                          │
│                                                                     │
│  Total Events: 5,805  |  Total Errors: 1,398  |  TS: 2025/08/04    │
│  Spark process took 22.15 seconds                                   │
└─────────────────────────────────────────────────────────────────────┘
```

> **Airflow DAG** — three tasks, 60-second schedule, `max_active_runs=1`

```
stream_from_clickhouse_to_minio  ──▶  spark_analysis  ──▶  send_to_dashboard
       (PyArrow + S3A)                 (SparkSubmit)          (FastAPI POST)
```

> **Kafka Connect DLQ in action**

```
Topic: user_interactions          → ClickHouse (✓ good messages)
Topic: user_interactions.dlq      → parked (✗ malformed messages, pipeline unblocked)
```

---

## ⚙️ Installation & Setup

**Requirements:** Docker ≥ 24 · Docker Compose ≥ 2.20 · `make` · 8 GB RAM · 4 CPU cores

```bash
# 1. Clone
git clone https://github.com/your-username/etar-pipeline.git
cd etar-pipeline

# 2. Create env files from templates
make init-env
# → Edit env/ files and replace all REPLACE_WITH_* placeholders (see table below)

# 3. Build all Docker images
make build

# 4. Start the full stack
make up

# 5. Open the dashboard
open http://localhost:8501
```

**Key variables to set after `make init-env`:**

| File | Variable | Command to generate |
|---|---|---|
| `env/airflow.creds` | `AIRFLOW__CORE__FERNET_KEY` | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `env/airflow.creds` | `AIRFLOW__API__SECRET_KEY` | any 32+ character random string |
| `env/kafka.env` | `CLUSTER_ID` | `python -c "import uuid; print(uuid.uuid4())"` |
| `env/spark.env` | `ALERT_WEBHOOK_URL` | your Slack/PagerDuty webhook — leave empty to disable |

**Service URLs once running:**

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | `airflow` / value from `env/airflow.creds` |
| Dashboard | http://localhost:8501 | — |
| Spark Master | http://localhost:8182 | — |
| MinIO Console | http://localhost:9001 | values from `env/minio.creds` |

```bash
# Stop and remove everything (including volumes)
make down
```

---

## 💡 Usage

Once running, everything is fully automatic. Here is what you can observe:

```bash
# Watch all logs in real time
make logs

# Check health of every container
make ps

# Run the full test suite (all 5 suites sequentially)
make test

# Run a specific suite
make test-producer    # Kafka producer: unit + integration
make test-airflow     # DAG: unit + integration + e2e
make test-dashboard   # API + UI: unit + integration
make test-spark       # Spark job: unit + integration
make test-db          # ClickHouse schema + MinIO bucket setup
```

**What you will see on the dashboard:**

1. Nothing for the first ~2 minutes while data accumulates in ClickHouse
2. A bar chart appears showing **SUCCESS vs ERROR counts** for each of the 5 event types
3. The chart updates every ~45 seconds (with ±20% jitter to prevent thundering herd on restart)
4. The footer shows total events, total errors, data timestamp, and Spark processing time

---

## 🧠 Key Engineering Decisions

These are the non-obvious choices that separate this from a tutorial pipeline:

**1. COUNT guard before opening S3 writers**
The original approach opened a `ParquetWriter` and S3 stream eagerly, then deleted the file if no data arrived — leaving a race window where concurrent readers could see a stale object. The fix runs a `COUNT(*)` against ClickHouse first. Zero rows → return immediately, nothing written to MinIO.

**2. ClickHouse partition granularity**
Partitioning by minute creates ~1,440 parts/day. ClickHouse's internal limit is ~1,000 active parts — this would trigger `TOO_MANY_PARTS` errors and halt all ingestion within days. Day-level partitioning (`toYYYYMMDD`) is the correct default, with `event_minute` kept as a cheap `MATERIALIZED` column for query filtering.

**3. Jinja template context ≠ Python context**
`conn.{id}.extra_dejson` looks accessible in code but is silently `None` inside Jinja-evaluated `SparkSubmitOperator` conf strings. This caused a silent empty S3A endpoint — Spark started but couldn't read MinIO. The fix stores the endpoint in `MINIO_ENDPOINT` (an env var) and uses `conn.{id}.login`/`.password` which are valid Jinja-accessible fields.

**4. `producer.poll()` is not `time.sleep()`**
Using `producer.poll(interval)` as a rate limiter works accidentally when the delivery callback queue is busy — but silently produces at full speed when it isn't, because `poll()` returns immediately if the queue is empty. Correct rate limiting requires `time.sleep()` followed by `producer.poll(0)`.

**5. Atomic MinIO writes via staging key**
Writing directly to the final object name means a consumer polling during upload receives a truncated Parquet or JSON file. The pattern: upload to `<n>.tmp` → `copy_object` to `<n>` → `remove_object` on staging. Readers always see either the previous complete object or the new complete one.

**6. Forked worker logging**
Python's logging handlers are not safe to reuse across `fork()`. Without re-initialising logging in each child process, workers share the parent's file handle — producing interleaved output or silently dropping lines on rotating handlers.

**7. Test image pinning**
Without `ARG IMAGE_TAG`, test Dockerfiles inherit from `lp/<service>:latest` — whatever was last built locally. On CI runners that build in parallel, tests can silently run against a different image than the one just built. Passing `IMAGE_TAG=$(git rev-parse HEAD)` through compose `build.args` guarantees every test run targets its own build.

---

## 🚧 Challenges & Solutions

**"The pipeline looked fine in tests but failed silently in production"**
The Kafka Connect `tasks.max` was set to 6, but the topic had only 1 partition. Five tasks were created with nothing to consume — no error, no warning, just five idle threads and one overloaded task doing all the work.
**Fix:** Match `tasks.max` to the actual partition count.

---

**"Airflow retried the task but produced wrong results"**
With a 3-second `retry_delay`, a retry fired while the ClickHouse connection was still timing out — the retry failed for the same reason and marked the DAG failed. At 3 seconds, the retry is just a faster failure.
**Fix:** `retry_delay = 30s` gives infra a realistic recovery window.

---

**"minio-init failed on the second developer's machine"**
`sleep 5` is not a readiness check — it's a guess. On a loaded laptop or slow CI runner, MinIO takes longer than 5 seconds to become ready.
**Fix:** Replaced `sleep 5` with a retry loop that calls `mc alias set` until it succeeds or a maximum attempt cap is reached.

---

**"Spark ran with one executor instead of two"**
`num_executors=2` tells Spark how many executors to *request*, not how many workers exist. With only one `spark-worker` container defined, the second executor had nowhere to schedule.
**Fix:** `deploy: replicas: 2` on the `spark-worker` service.

---

## 📈 Future Improvements

| Priority | Improvement |
|---|---|
| 🔴 High | **SASL/SSL on Kafka** — all traffic currently uses `PLAINTEXT`; switch to `SASL_SSL` for production security |
| 🔴 High | **`run_id`-prefixed MinIO objects** — closes the XCom retry-overlap gap where a retried DAG task can overwrite a file a downstream task already consumed |
| 🟡 Medium | **Redis-backed dashboard store** — the in-memory `deque` is lost on pod restart; Redis gives persistence and enables horizontal scaling |
| 🟡 Medium | **Prometheus + Grafana** — add metrics for Kafka consumer lag, Spark job duration, Airflow SLA breaches, and MinIO storage growth |
| 🟢 Nice-to-have | **CD pipeline** — extend CI to push images to a registry (ECR / GCR) and deploy to staging on merge to `main` |
| 🟢 Nice-to-have | **Event-driven reporting** — replace Airflow POST → API → Streamlit poll with Server-Sent Events for zero-latency dashboard updates |
| 🟢 Nice-to-have | **Dynamic Kafka partition scaling** — document how to scale `tasks.max` and topic partitions together as throughput grows |

---

## 🤝 Contribution

Contributions are welcome. Open an issue to discuss the change first, then submit a pull request against the `develop` branch. All PRs must pass `make test` before review.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

<div align="center">

## 👤 Author

**Your Name** — Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/your-profile)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=flat&logo=github&logoColor=white)](https://github.com/your-username)
[![Email](https://img.shields.io/badge/Email-Contact-EA4335?style=flat&logo=gmail&logoColor=white)](mailto:your.email@example.com)

---

*Built with Python 3.11 · Kafka · Spark · Airflow · ClickHouse · MinIO · FastAPI · Streamlit*

</div>
