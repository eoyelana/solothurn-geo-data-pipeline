# Solothurn Geospatial Data Pipeline

## Overview

This project is a data pipeline built with Apache Airflow to process geospatial building data from the Solothurn GeoPortal. The pipeline extracts building data, cleans and transforms it, loads it into a PostGIS-enabled PostgreSQL database, and then performs a final geometric query.

The pipeline automates the following key tasks as per the assignment's requirements:

*   **Extract:** Downloads the building dataset (in Shapefile format) from the official Solothurn open data portal and saves it in a local `data/raw` directory.
*   **Clean Database:** Truncates the target PostgreSQL table to ensure a clean slate for each rin, making pipeline idempotent.
*   **Transform & Clean:** Loads the raw data, combines the `bodenbedeckung` and `bodenbedeckung_proj` layers, and then cleans it by:
    *   Filtering for valid building records with a Swiss building identifier (`EGID`).
    *   Selecting only objects classified as buildings (`art_txt = 'Gebaeude'`).
    *   Calculating the area of each building in square meters (using the accurate Swiss LV95 projection).
    *   Transforming the geometry from the Swiss LV95 coordinate system (EPSG:2056) to the global WGS84 standard (EPSG:4326) for broad compatibility.
*   **Load:** Stores the cleaned, transformed data into a PostGIS-enabled PostgreSQL database, complying to a predefined table schema.
*   **Query & Export:** Performs a final query on the database to extract buildings with an area greater than 50m² and saves the result as a `big_building.geojson` file.


The primary goal is to demonstrate a modern data engineering workflow, and automated data pipeline that handles geospatial data from source to final analysis.

## Project Structure

```
.
├── dags
│   └── solothurn_dag.py    # The main Airflow DAG
├── data
│   ├── processed           # Processed data outputs (e.g., GeoJSON)
│   └── raw                 # Raw downloaded data
├── notebooks
│   └── exploration.ipynb   # Jupyter notebook for initial analysis
├── sql
│   └── building_table.sql  # SQL schema for the buildings table
├── .gitignore
├── README.md
└── requirements.txt
```

## Setup and Installation

### Prerequisites

*   Python 3.9+
*   An operational Apache Airflow environment (local or Docker-based).
*   A running PostgreSQL database with the PostGIS extension enabled.

### Installation

1.  **Clone the Repository:**
    ```
    git clone https://github.com/your-username/solothurn-data-pipeline.git
    cd solothurn-data-pipeline
    ```
2.  **Create the Database Table:**
    Before running the pipeline, you must create the target table using the provided schema. Connect to your PostgreSQL database (e.g., using `psql` or a tool like DataGrip) and execute the script:
    ```
    -- Run the contents of sql/building_table.sql
    ```
    This ensures the table exists with the correct columns, data types, and performance-enhancing indices.

3.  **Configure Airflow Connection:**
    In the Airflow UI, go to **Admin -> Connections** and create a new PostgreSQL connection with the following details:
    *   **Conn ID:** `postgres_default`
    *   **Conn Type:** `PostgreSQL`
    *   **Host:** Your PostgreSQL host (e.g., `localhost` or a container name)
    *   **Schema:** The database name (e.g., `airflow` or `solothurn_db`)
    *   **Login:** Your database username
    *   **Password:** Your database password
    *   **Port:** Your database port (e.g., `5432`)

4.  **Install Dependencies:**
    ```
    pip install -r requirements.txt
    ```

## Running the Pipeline

### Part 1: Data Exploration

The Jupyter Notebook in `notebooks/exploration.ipynb` contains the initial data exploration, cleaning, and transformation logic. It produces a `building.geojson` file that can be visualized on [kepler.gl](https://kepler.gl/demo) to verify the data.

### Part 2: Airflow DAG

1.  **Link Your DAG:**
    To make your DAG visible to Airflow, create a symbolic link from your project's `dags` folder to your Airflow `dags` folder. This is the recommended approach as it ensures Airflow always sees the latest version of your code.
    ```
    # Example for a typical local Airflow setup
    ln -s /path/to/solothurn-geo-data-pipeline/dags/solothurn_dag.py ~/airflow/dags/
    ```

2.  **Run in Airflow:**
    *   Open the Airflow UI.
    *   Find the `solothurn_building_pipeline` DAG and un-pause it (click the toggle on the left).
    *   Trigger the DAG manually by clicking the "Play" (▶) button on the right.
    *   Monitor the execution in the Graph or Grid view.

3.  **Locate the Output:**
    Upon successful completion, the final output file will be generated at the following path within your Airflow environment: `~/airflow/data/processed/big_building.geojson`.
    The file `big_building.geojson` can be visualized on [kepler.gl](https://kepler.gl/demo) to verify the data.

## Pipeline Design & Concepts

### Task Orchestration
The pipeline is designed with a clear, logical flow to ensure data integrity:
`[Download] >> [Clean] >> [Load] >> [Query]`
*   The **Clean** task only runs after the **Download** is complete.
*   The **Load** task only runs after the **Clean** task succeeds, guaranteeing it writes to an empty table.
*   The final **Query** task only runs after the **Load** is finished, ensuring it queries the latest data.

### Idempotency (`clean_staging_table` task)
A key feature of this pipeline is its **idempotency**, which means running it multiple times produces the same end result without errors or data duplication. This is achieved by the `clean_staging_table` task, which runs a `TRUNCATE TABLE` command before the load step. This is a best practice in ETL design, making the pipeline reliable for both development and scheduled production runs.

### Adherence to Predefined Schema
This pipeline is built to respect the official table structure defined in `sql/building_table.sql`. 
By using a `TRUNCATE` + `APPEND` strategy (instead of `REPLACE`), we preserve the valuable indices, constraints, and `DEFAULT` values (like `create_timestamp`) that were defined in the SQL schema, ensuring data integrity and query performance.

## Skills Demonstrated

This project demonstrates proficiency in several key areas of data engineering:

*   **Orchestration:** Using Apache Airflow to define, schedule, and monitor a complex data workflow.
*   **Idempotent Pipeline Design:** Implementing modern, repeatable pipelines using a truncate-and-load pattern.
*   **Geospatial Data Handling:** Expertise in using `geopandas` for reading, filtering, and transforming geospatial data (SHP to GeoJSON, CRS transformation, geometric calculations).
*   **Database Management:** Storing and querying geospatial data in a PostGIS database in compliance to a predefined schema to maintain data integrity and performance.
*   **ETL/ELT Best Practices:** Implementing a clear, multi-step ETL process with distinct, dependent tasks for extraction, transformation, and loading.
*   **Database Management:** Storing and querying geospatial data in a PostGIS database, including the use of geometric functions and data types.
*   **Project Management:** Using GitHub for version control, and project documentation to reflect best software engineering practices.