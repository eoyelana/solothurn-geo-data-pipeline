# Solothurn Geospatial Data Pipeline

## Overview

This project is a data pipeline built with Apache Airflow to process geospatial building data from the Solothurn GeoPortal. The pipeline extracts building data, cleans and transforms it, loads it into a PostGIS-enabled PostgreSQL database, and then performs a final geometric query.

The pipeline automates the following key tasks as per the assignment's requirements:

*   **Extract:** Downloads the building dataset (in Shapefile format) from the official Solothurn open data portal and saves it in a local `data/raw` directory.
*   **Transform & Clean:** Loads the raw data, combines the `bodenbedeckung` and `bodenbedeckung_proj` layers, and then cleans it by:
    *   Filtering for records where the Swiss building identifier (`EGID`) is not null.
    *   Selecting only objects classified as buildings (`art_txt = 'Gebaeude'`).
    *   Calculating the area of each building in square meters.
    *   Transforming the geometry from the Swiss LV95 coordinate system (EPSG:2056) to the global WGS84 standard (EPSG:4326).
*   **Load:** Stores the cleaned, transformed data into a PostGIS-enabled PostgreSQL database, adhering to a predefined table schema.
*   **Query:** Performs a final geometric query on the database to extract buildings with an area greater than 50m² and saves the result as a `big_building.geojson` file.


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
*   Docker and Docker Compose (for running Airflow and Postgres)
*   An initialized Airflow environment.

### Installation

1.  **Clone the Repository:**
    ```
    git clone https://github.com/your-username/solothurn-data-pipeline.git
    cd solothurn-data-pipeline
    ```

2.  **Set up Airflow and PostgreSQL:**
    It is recommended to use the official Airflow Docker Compose file. Set up an Airflow connection with the ID `postgres_default` pointing to your PostGIS database.

3.  **Install Dependencies:**
    ```
    pip install -r requirements.txt
    ```

## How to Run

### Part 1: Data Exploration

The Jupyter Notebook in `notebooks/exploration.ipynb` contains the initial data exploration, cleaning, and transformation logic. It produces a `building.geojson` file that can be visualized on [kepler.gl](https://kepler.gl/demo) to verify the data.

### Part 2: Airflow DAG

1.  Place the `dags/solothurn_dag.py` file into your Airflow DAGs folder.
2.  Un-pause the `solothurn_building_pipeline` DAG in the Airflow UI.
3.  Trigger the DAG manually and monitor its execution.
4.  Upon successful completion, the `big_building.geojson` file will be created in the `data/processed` directory inside your Airflow container.

## Skills Demonstrated

This project demonstrates proficiency in several key areas of data engineering:

*   **Orchestration:** Using Apache Airflow to define, schedule, and monitor a data pipeline.
*   **Geospatial Data Handling:** Expertise in using `geopandas` for reading, filtering, and transforming geospatial data (SHP to GeoJSON, CRS transformation).
*   **ETL Process:** Implementing a clear, multi-step ETL process with distinct tasks for extraction, transformation, and loading.
*   **Database Management:** Storing and querying geospatial data in a PostGIS database, including the use of geometric functions and data types.
*   **Project Management:** The use of GitHub for version control, and project documentation to reflect best software engineering practices.