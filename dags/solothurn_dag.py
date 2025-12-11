import pendulum
from airflow.sdk import dag, task
from pathlib import Path
import requests
import zipfile
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook # Professional Way
from airflow.models import variable
import os

data_url = "https://files.geo.so.ch/ch.so.agi.av.mopublic/aktuell/2601.ch.so.agi.av.mopublic.shp.zip"
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
base_dir = os.path.join(AIRFLOW_HOME, "data")
raw_data_path = Path(base_dir) / "raw"
processed_data_path = Path(base_dir) / "processed"
shapefile_zip = raw_data_path / "solothurn.shp.zip"

@dag(
    dag_id='solothurn_building_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['geospatial', 'technical-test'],
)
def solothurn_building_pipeline():
    """
    DAG to process building data from GeoPortal Solothurn.
    """

    @task
    def download_and_extract_data() -> str:
        """Downloads and extracts the shapefile data."""
        raw_data_path.mkdir(parents=True, exist_ok=True)
        response = requests.get(data_url)
        with open(shapefile_zip, 'wb') as f:
            f.write(response.content)
        with zipfile.ZipFile(shapefile_zip, 'r') as zip_ref:
            zip_ref.extractall(raw_data_path)
        
        # Return the folder path so the next task can find both files
        return str(raw_data_path)

    @task
    def clean_staging_table():
        PostgresHook(postgres_conn_id='postgres_default').run("TRUNCATE TABLE building;")


    @task
    def transform_and_load(data_dir: str):
        """Filters, transforms, and loads building data into PostGIS."""
        raw_path = Path(data_dir)
        
        # Correctly load both files as per requirements
        files_to_load = [raw_path / "bodenbedeckung.shp", raw_path / "bodenbedeckung_proj.shp"]
        gdfs = []
        for f in files_to_load:
            if f.exists():
                gdfs.append(gpd.read_file(f))
        
        if not gdfs:
            raise FileNotFoundError("No shapefiles found to process.")
            
        gdf = pd.concat(gdfs, ignore_index=True)

        # Filter data using the clean .notna() method
        filtered_gdf = gdf[(gdf['egid'].notna()) & (gdf['art_txt'] == 'Gebaeude')].copy()

        # 1. Calculate area in original Swiss CRS (EPSG:2056) for accuracy
        filtered_gdf['area'] = filtered_gdf.geometry.area.astype(int) # Integer matches SQL 'integer' type
        filtered_gdf['geo_center'] = filtered_gdf.geometry.centroid

        # 2. Transform the main polygon to WGS84
        gdf_wgs84 = filtered_gdf.to_crs(epsg=4326)
        
        # 3. Transform the 'geo_center' column to WGS84 manually
        # This converts the accurate meter-based points to Lat/Lon without doing bad math.
        gdf_wgs84['geo_center'] = gdf_wgs84['geo_center'].to_crs(epsg=4326)

        # 4. Rename columns to match SQL schema exactly
        gdf_wgs84 = gdf_wgs84.rename(columns={
            'geometry': 'geo_polygon',
            'art_txt': 'building_type'
            # egid matches
            # area matches
        })

        # Select only columns needed for the final query
        # Note SQL file has created timestamp, so no need for recreation or overwrite
        final_cols = ['egid', 'building_type', 'area', 'geo_polygon', 'geo_center']
        gdf_wgs84 = gdf_wgs84[final_cols]

        # Tell geopandas which column is the main geometry column
        gdf_wgs84 = gdf_wgs84.set_geometry("geo_polygon")

        # Load to PostgreSQL using PostgresHook (Professional/Secure)
        # This uses the connection defined in Airflow UI -> Admin -> Connections
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()

        # 'building' is the table name. 
        # if_exists='append' assumes table was created with the SQL script first.
        # if_exists='replace' would drop the custom table with indices.
        gdf_wgs84.to_postgis('building', engine, if_exists='append', index=False)


    @task
    def query_large_buildings():
        """Queries buildings with area > 50m^2 and saves as GeoJSON."""
        processed_data_path.mkdir(parents=True, exist_ok=True)
        
        # Use Hook again
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()

        # Query matches your table columns
        sql = "SELECT egid, building_type, area, geo_polygon FROM building WHERE area > 50"
        
        # geom_col must match the geometry column name
        large_buildings_gdf = gpd.read_postgis(sql, engine, geom_col='geo_polygon')

        output_path = processed_data_path / "big_building.geojson"
        large_buildings_gdf.to_file(output_path, driver='GeoJSON')


    # Define task dependencies
    download = download_and_extract_data()
    clean = clean_staging_table()
    load_task = transform_and_load(download)
    query_task = query_large_buildings()

    download >> clean >> load_task >> query_task

solothurn_building_pipeline()