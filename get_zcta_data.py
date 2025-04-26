import geopandas as gpd
import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime, timezone


def get_zcta_data():
    print("Starting ZCTA data extraction...")
    shapefile_path = os.path.join("manual data", "tl_2024_us_zcta520", "tl_2024_us_zcta520.shp")

    # Verify shapefile exists
    print(f"Checking shapefile path: {shapefile_path}")
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(f"Shapefile not found at: {shapefile_path}")

    # Read shapefile with progress bar
    print("Reading ZCTA shapefile...")
    zcta_gdf = gpd.read_file(shapefile_path)
    print(f"Loaded {len(zcta_gdf)} ZCTA records from shapefile")

    # Extract data with progress bar
    print("Extracting ZCTA information...")
    zcta_data = pd.DataFrame({
        'zcta': zcta_gdf['ZCTA5CE20'],
        'latitude': [p.y for p in tqdm(zcta_gdf.geometry.centroid, desc="Calculating centroids (latitude)")],
        'longitude': [p.x for p in tqdm(zcta_gdf.geometry.centroid, desc="Calculating centroids (longitude)")]
    })

    # Create data folder if it doesn't exist
    data_folder = "data"
    os.makedirs(data_folder, exist_ok=True)

    # Generate CSV filename with UTC datetime
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_filename = f"zcta_data_{timestamp}.csv"
    csv_path = os.path.join(data_folder, csv_filename)

    # Save to CSV
    print(f"Saving data to: {os.path.abspath(csv_path)}")
    zcta_data.to_csv(csv_path, index=False)
    print(f"Successfully extracted and saved data for {len(zcta_data)} ZCTAs to {csv_path}")

    return zcta_data


if __name__ == "__main__":
    get_zcta_data()