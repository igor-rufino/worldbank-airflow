from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import requests
import duckdb

URL = {
    "base_url": "https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/",
    "gdp": "indicator/NY.GDP.MKTP.CD",
}

dag_parameters = {
    "owner": "Data_Team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "default_view": "graph",
}


def extract():
    gdp_data = []
    page = 1
    response = requests.get(
        f"{URL['base_url']}{URL['gdp']}?format=json&page={page}&per_page=50"
    )
    if response.status_code == 200:
        data = response.json()
        gdp_data.extend(data[1])

        pages = data[0]["pages"]
        while page < pages:
            page += 1
            response = requests.get(
                f"{URL['base_url']}{URL['gdp']}?format=json&page={page}&per_page=50"
            )

            if response.status_code == 200:
                data = response.json()
                gdp_data.extend(data[1])
            else:
                print(f"Failed to extract data: {response.status_code}")
    else:
        print(f"Failed to extract data: {response.status_code}")
    print(f"len gdp_data: {len(gdp_data)}")
    return gdp_data


def load(**kwargs):
    gdp_data = kwargs["ti"].xcom_pull(task_ids="extract")

    conn = duckdb.connect(database="gdp.db")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS country (
            id VARCHAR PRIMARY KEY, 
            name VARCHAR, 
            iso3_code VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gdp (
            country_id VARCHAR, 
            year INT, 
            value DOUBLE,
            PRIMARY KEY (country_id, year)
        )
        """
    )

    country_data = []
    gdp_data_bulk = []
    for entry in gdp_data:
        country_id = entry["country"]["id"]
        country_name = entry["country"]["value"]
        iso3_code = entry["countryiso3code"]
        year = int(entry["date"])
        value = float(entry["value"]) if entry["value"] else None

        country_data.append((country_id, country_name, iso3_code))
        gdp_data_bulk.append((country_id, year, value))

    conn.executemany(
        "INSERT OR REPLACE INTO country (id, name, iso3_code) VALUES (?, ?, ?)",
        country_data,
    )

    conn.executemany(
        "INSERT OR REPLACE INTO gdp (country_id, year, value) VALUES (?, ?, ?)",
        gdp_data_bulk,
    )

    query = "SELECT * FROM country c ORDER BY c.id;"
    result = conn.sql(query)
    result.show()
    query = "SELECT * FROM gdp g ORDER BY g.country_id;"
    result = conn.sql(query)
    result.show()

    conn.close()


def query():
    conn = duckdb.connect(database="gdp.db")
    query = """
    SELECT
        c.id,
        c.name,
        c.iso3_code,
        ROUND(MAX(CASE WHEN g.year = 2019 THEN g.value END) / 1e9, 2) AS "2019",
        ROUND(MAX(CASE WHEN g.year = 2020 THEN g.value END) / 1e9, 2) AS "2020",
        ROUND(MAX(CASE WHEN g.year = 2021 THEN g.value END) / 1e9, 2) AS "2021",
        ROUND(MAX(CASE WHEN g.year = 2022 THEN g.value END) / 1e9, 2) AS "2022",
        ROUND(MAX(CASE WHEN g.year = 2023 THEN g.value END) / 1e9, 2) AS "2023"
    FROM
        country c
    LEFT JOIN
        gdp g ON c.id = g.country_id
    GROUP BY
        c.id, c.name, c.iso3_code
    ORDER BY
        c.name;
    """
    result = conn.sql(query)
    result.show()

    conn.close()


with DAG(
    dag_id="worldbank_pipeline",
    default_args=dag_parameters,
    description="Pipeline to collect GDP data from WorldBank API",
    tags=["duckDB", "API"],
    schedule="0 8 * * *",
    catchup=False,
) as dag:
    start_pipeline = EmptyOperator(
        task_id="start_pipeline", priority_weight=1, weight_rule="absolute"
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        dag=dag,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load,
        provide_context=True,
        dag=dag,
    )

    query = PythonOperator(
        task_id="query",
        python_callable=query,
        dag=dag,
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline", priority_weight=1, weight_rule="absolute"
    )

    # Orchestration
    start_pipeline >> extract >> load >> query >> end_pipeline


dag.doc_md = """
# worldbank_pipeline

Pipeline to collect GDP data from WorldBank API

"""
