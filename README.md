### Airflow ETL Pipeline with Postgres and API Integration

---

# 🚀 Airflow ETL Pipeline with Postgres & NASA API

This project showcases a simple ETL pipeline built using **Apache Airflow**, which extracts data from the **NASA Astronomy Picture of the Day (APOD) API**, transforms it, and loads it into a **PostgreSQL** database. The entire workflow runs inside Docker for easy reproducibility.

---

## 🔧 Tech Stack

* **Apache Airflow** – Workflow orchestration
* **PostgreSQL** – Target database
* **Docker** – Containerized environment
* **NASA APOD API** – External data source [https://api.nasa.gov/]

---

## 🧩 Project Structure

* `DAG`: Defines a daily ETL pipeline using Airflow.
* `Extract`: Fetches APOD data via `HttpOperator`.
* `Transform`: Parses and formats JSON response using Airflow’s `@task`.
* `Load`: Inserts the data into Postgres using `PostgresHook`.

---

## 🔄 Workflow Summary

1. **🛰 Extract**
   Pull daily APOD metadata (title, image URL, date, etc.) from NASA API.

2. **🔧 Transform**
   Clean and structure the data for storage.

3. **💾 Load**
   Store results into a PostgreSQL table (`apod_data`). The table is auto-created if not present.

---

## 📦 Setup Instructions

1. Clone the repo:

   ```bash
   git clone <repo_url>
   cd project_folder
   ```

2. Start services using Astro CLI or Docker:

   ```bash
   astro dev start  # or docker-compose up
   ```

3. Access the Airflow UI at:
   [http://localhost:8080](http://localhost:8080)
   *(default creds: `admin` / `admin`)*

4. Trigger the `nasa_apod_postgres` DAG manually or let it run daily.

---

## 🧪 Example Output (Postgres Table)

| id | title       | explanation     | url           | date       | media\_type |
| -- | ----------- | --------------- | ------------- | ---------- | ----------- |
| 1  | Cosmic View | NASA's deep ... | `https://...` | 2025-05-02 | image       |

---
## ✅ Screenshots
* Airflow UI
<img width="613" alt="image" src="https://github.com/user-attachments/assets/acb020a0-e483-4db7-b262-6f161ac76fcd" />

--------------------------------------------------------------------------------------------------------------------------
* Database 
<img width="667" alt="image" src="https://github.com/user-attachments/assets/2e96e357-6699-4d99-9f6d-502d0363994f" />



