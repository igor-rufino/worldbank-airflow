# World Bank GDP Data Pipeline

## Introduction

```worldbank-airflow``` is a data engineering project aimed at developing a data ingestion pipeline to extract, load, and query GDP data for South American countries from the World Bank API. The pipeline is implemented using Python and orchestrated with Apache Airflow within a Docker environment. The data is stored in a DuckDB database.

## Prerequisites

Ensure you have the following installed on your machine:

- Docker
- Docker Compose

## Project Structure

The project directory contains the following files:

```
.
├── dags
|   └── dag_worldbank_pipeline.py
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md

```

**Files Description**:

- dag_worldbank_pipeline.py: Contains the Apache Airflow DAG definition for the pipeline.
- docker-compose.yaml: Defines the Docker services for Airflow and DuckDB.
- Dockerfile: Specifies the Docker image configuration for the Airflow worker.
- requirements.txt: Lists the Python dependencies for the project.
- README.md: Provides an overview and setup instructions for the project.


## Setup Instructions

Follow these steps to set up and run the project:

1. Clone the Repository:
    ```
    git clone <repository_url>
    cd <repository_directory>
    ```

2. Build and Start Docker Services:
    ```
    docker-compose up --build
    ```

3. Access the Airflow UI:

   - Open your web browser and go to http://localhost:8080 to access the Airflow UI.


4. Run the Pipeline:

    - Trigger the worldbank_pipeline DAG from the Airflow UI.

## Usage
### Extracting Data
The pipeline extracts GDP data for South American countries from the World Bank API. The extraction is handled by the extract function in the dag_worldbank_pipeline.py script.

### Loading Data
The extracted data is loaded into a DuckDB database. The load function in the dag_worldbank_pipeline.py script handles this process, creating necessary tables and inserting data.

### Querying Data
The query function in dag_worldbank_pipeline.py generates a pivoted report of the GDP data for the last 5 years, presented in billions. The report structure includes columns for country ID, name, ISO3 code, and GDP values for the years 2019 to 2023.

## Design Decisions
- **DuckDB**: Chosen for its simplicity and efficiency in handling SQL operations without requiring a full-fledged database server.
- **Airflow**: Utilized to orchestrate the pipeline, providing scheduling, monitoring, and logging capabilities.
- **Docker**: Employed to containerize the application, ensuring consistent environments and easy deployment.

## Assumptions
- **Data Completeness**: The World Bank API provides complete and accurate data for the specified countries and years.
- **Handling Missing Data**: If GDP values are missing for any year, the value is set to None in the database.