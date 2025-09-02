# airflow_env-using-Postgres

Using PostgreSQL as the metadata database for Apache Airflow in a production environment is a recommended best practice due to its robustness, scalability, and data integrity features, 
which are crucial for a stable and reliable data orchestration platform.

## 1. **Installing PostgreSQL “in WSL system” (via apt)**

* You’re installing **system-wide PostgreSQL** using Ubuntu’s package manager (`apt`).
* Binaries (like `psql`, `postgres`) go to `/usr/bin/`.
* The service runs under **systemd/service manager** as background daemon.
* Data lives in `/var/lib/postgresql/...`.
* Port: defaults to **5432**, accessible across the whole WSL instance.
* Managed by root (`sudo` needed).

✅ Good if:

* You want PostgreSQL available for all projects on that WSL machine.
* You’re okay with “real service” semantics (system-wide, always running).

⚠️ Risks:

* Port conflicts (5432 already in use).
* Harder to isolate from other projects (all use the same Postgres instance).
* Mixing system-level packages and conda/pip packages can get messy.

---

## 2. **Installing PostgreSQL “inside airflow\_env” (via conda)**

* PostgreSQL is installed **only inside your conda environment**.
* Binaries live under `~/miniconda3/envs/airflow_env/bin/`.
* No systemd service; you start/stop manually with `pg_ctl`.
* Data directory is wherever you initialize it (e.g., `~/airflow/pgdata`).
* Port: you can pick (e.g., **5433**) so it won’t conflict with any system Postgres.
* Runs only when you activate and start it — fully isolated.

✅ Good if:

* You want everything for Airflow self-contained in one env (no system conflicts).
* You might have multiple environments with different Postgres versions.
* You don’t want root privileges or touching `/usr/lib/postgresql`.

⚠️ Risks:

* You must remember to start/stop manually (`pg_ctl -D ... start`).
* It’s “userland” — not integrated with system services.
* Slightly less common setup, so some guides won’t match exactly.

---

## 3. **Key difference**

* **WSL apt install** → “system PostgreSQL”, shared, always-on, root-managed.
* **Conda airflow\_env install** → “embedded PostgreSQL”, isolated, project-only, user-managed.

Think of it like:

* Apt version = renting an apartment in a big building (shared utilities, one master switch).
* Conda version = having a portable cabin just for your project (isolated, no neighbors, you control power).

---





# Step-by-step

## 0) Activate your env (follow this repository https://github.com/Jjrex8988/airflow_env.git)

```bash
conda activate airflow_env
python --version  # should show 3.9.x
```

## 1) Install PostgreSQL + psycopg2 in the env

```bash
conda install -c conda-forge postgresql psycopg2
```


## 2) Initialize Postgres cluster (SCRAM auth + superuser password prompt)

```bash
mkdir -p ~/airflow/pgdata
initdb -D ~/airflow/pgdata -A scram-sha-256 -U postgres -W
chmod 700 ~/airflow/pgdata
```


## 3) Configure Postgres (avoid conflicts)

Edit **`~/airflow/pgdata/postgresql.conf`**:

```conf
listen_addresses = 'localhost'
port = 5433
```

Edit **`~/airflow/pgdata/pg_hba.conf`** (auth rules; bottom lines):

```
local   all             all                                     scram-sha-256
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ::1/128                 scram-sha-256
```

> If a `postgresql.auto.conf` exists and has `port = 5432`, change it to `5433` (it overrides).


## 4) Start Postgres + verify

```bash
pg_ctl -D ~/airflow/pgdata -l ~/airflow/pg.log start
pg_isready -h localhost -p 5433
```


## 5) Create role & database for Airflow

```bash
psql -h localhost -p 5433 -U postgres
```

In `psql`:

```sql
CREATE ROLE jjrex8988 WITH LOGIN PASSWORD 'PASSWORD';
CREATE DATABASE airflow_meta OWNER jjrex8988;
\q
```

Quick test:

```bash
psql -h localhost -p 5433 -U jjrex8988 -d airflow_meta -c '\conninfo'
```


## 6) Persist Airflow env vars (Conda activation hook)

Create **`~/miniconda3/envs/airflow_env/etc/conda/activate.d/env_vars.sh`**:

```bash
mkdir -p ~/miniconda3/envs/airflow_env/etc/conda/activate.d
nano ~/miniconda3/envs/airflow_env/etc/conda/activate.d/env_vars.sh
```

Put **(single lines)**:

```bash
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='postgresql+psycopg2://jjrex8988:PASSWORD@localhost:5433/airflow_meta'
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
```

(Optional) Deactivate hook:

```bash
mkdir -p ~/miniconda3/envs/airflow_env/etc/conda/deactivate.d
printf "unset AIRFLOW__DATABASE__SQL_ALCHEMY_CONN\nunset AIRFLOW__CORE__EXECUTOR\n" \
  > ~/miniconda3/envs/airflow_env/etc/conda/deactivate.d/env_vars.sh
```

Reload:

```bash
conda deactivate && conda activate airflow_env
echo "$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
```


## 7) Install **Airflow 2.10.2** with matching providers (constraints!)

```bash
PYVER=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
AIRFLOW_VER=2.10.2
pip install "apache-airflow[postgres]==${AIRFLOW_VER}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VER}/constraints-${PYVER}.txt"
airflow version   # should print 2.10.2
pip show apache-airflow-providers-postgres | grep Version  # should show 5.12.0 (OK on 2.10.x)
```


## 8) Reset (fresh) and initialize metadata DB

```bash
airflow db reset   # type 'y' when prompted (fresh setup)
airflow db init
```

Verify tables exist:

```bash
psql -h localhost -p 5433 -U jjrex8988 -d airflow_meta -c '\dt'
```

## 9) Create your Airflow UI admin user

```bash
airflow users create \
  --username jjrex8988 \
  --password 'PASSWORD' \
  --firstname jj \
  --lastname rex8988 \
  --role Admin \
  --email jjrex8988@gmail.com
```


## 10) Start Airflow

```bash
airflow scheduler
# open a second terminal (same env):
airflow webserver -p 8080
```

Login at **[http://localhost:8080](http://localhost:8080)** with `jjrex8988 / PASSWORD`.

---


## Sanity checks & tips

* Confirm Airflow is really using Postgres:

  ```bash
  airflow config get-value database sql_alchemy_conn
  ```
* If a password contains special chars, **URL-encode** them (`@` → `%40`, `:` → `%3A`, `/` → `%2F`).
* If `pg_isready -p 5433` says “no response”, recheck `postgresql.conf` (and `postgresql.auto.conf`) and restart.
* Keep exports **on one line**; a newline in the middle causes `command not found`.
* To switch back to SQLite for quick tests:

  ```bash
  unset AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////home/jjrex8988/airflow/airflow.db
  airflow db init
  ```

