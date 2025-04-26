import pandas as pd
import os
import glob
from datetime import datetime, timezone


def get_latest_csv(dataset_name, folder="automated data"):
    """Find the most recent CSV file for a given dataset in the specified folder."""
    pattern = os.path.join(folder, f"{dataset_name}_*.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"No CSV file found for {dataset_name} in {folder}")
        return None

    # Extract timestamp from filename and find the most recent
    def extract_timestamp(filepath):
        filename = os.path.basename(filepath)
        timestamp_str = filename.split('_')[-2] + '_' + filename.split('_')[-1].replace('.csv', '')
        return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")

    latest_file = max(files, key=extract_timestamp)
    print(f"Found latest file for {dataset_name}: {latest_file}")
    return latest_file


def print_merge_info(df1, df2, df1_name, df2_name):
    """Print column names of both DataFrames and their common columns before merging."""
    df1_columns = list(df1.columns)
    df2_columns = list(df2.columns)
    common_columns = [col for col in df1_columns if col in df2_columns]

    print(f"\nPreparing to merge {df1_name} with {df2_name}:")
    print(f"Columns in {df1_name}: {df1_columns}")
    print(f"Columns in {df2_name}: {df2_columns}")
    print(f"Common columns: {common_columns}\n")


def print_dataset_info(df, dataset_name):
    """Print the column names and shape of a loaded dataset."""
    print(f"Dataset {dataset_name} info:")
    print(f"Columns: {list(df.columns)}")
    print(f"Shape: {df.shape}\n")


def join_data():
    print("Starting data merging process...")

    # Load datasets if they exist
    datasets = {}

    # ZCTA data
    zcta_file = get_latest_csv("zcta_data")
    if zcta_file:
        print("Loading ZCTA data...")
        datasets['zcta_data'] = pd.read_csv(zcta_file)
        print_dataset_info(datasets['zcta_data'], "zcta_data")
    else:
        print("ZCTA data is required. Exiting.")
        return

    # ACS income data
    income_file = get_latest_csv("income_data")
    if income_file:
        print("Loading ACS income data...")
        datasets['income_data'] = pd.read_csv(income_file)
        print_dataset_info(datasets['income_data'], "income_data")
    else:
        print("ACS income data not found. Skipping.")

    # Crime data
    crime_file = get_latest_csv("crime_data")
    if crime_file:
        print("Loading crime data...")
        datasets['crime_data'] = pd.read_csv(crime_file)
        print_dataset_info(datasets['crime_data'], "crime_data")
    else:
        print("Crime data not found. Skipping.")

    # Sunlight data
    sunlight_file = get_latest_csv("sunlight_data")
    if sunlight_file:
        print("Loading sunlight data...")
        datasets['sunlight_data'] = pd.read_csv(sunlight_file)
        print_dataset_info(datasets['sunlight_data'], "sunlight_data")
    else:
        print("Sunlight data not found. Skipping.")

    # Load zip_zcta_xref.csv
    xref_file = os.path.join("manual data", "zip_zcta_xref.csv")
    if os.path.exists(xref_file):
        print("Loading zip_zcta_xref data...")
        xref_data = pd.read_csv(xref_file)
        print_dataset_info(xref_data, "zip_zcta_xref")
        # Check if required columns exist
        required_columns = ['zcta', 'zip', 'source']
        missing_columns = [col for col in required_columns if col not in xref_data.columns]
        if missing_columns:
            print(f"Missing required columns in zip_zcta_xref.csv: {missing_columns}. Skipping merge.")
            xref_data = None
        else:
            print("Columns in zip_zcta_xref.csv (after validation):", list(xref_data.columns))
    else:
        print(f"zip_zcta_xref.csv not found in manual data folder. Skipping merge.")
        xref_data = None

    # Load zcta_review.csv
    review_file = os.path.join("manual data", "zcta_review.csv")
    if os.path.exists(review_file):
        print("Loading zcta_review data...")
        review_data = pd.read_csv(review_file)
        print_dataset_info(review_data, "zcta_review")
        # Check if 'zcta' column exists
        if 'zcta' not in review_data.columns:
            print("Missing 'zcta' column in zcta_review.csv. Skipping merge.")
            review_data = None
    else:
        print(f"zcta_review.csv not found in manual data folder. Skipping merge.")
        review_data = None

    # Merge datasets
    print("Merging datasets...")
    merged_data = datasets['zcta_data']
    print(f"Initial shape of merged_data: {merged_data.shape}")

    # Merge with zip_zcta_xref.csv on zcta
    if xref_data is not None:
        xref_subset = xref_data[['zcta', 'zip', 'source']]
        print_merge_info(merged_data, xref_subset, "merged_data", "zip_zcta_xref")
        merged_data = merged_data.merge(xref_subset, on='zcta', how='left')
    else:
        print("Skipping merge with zip_zcta_xref due to missing data or columns.")
    print(f"Shape after zip_zcta_xref merge attempt: {merged_data.shape}")

    # Merge with zcta_review.csv on zcta
    if review_data is not None:
        print_merge_info(merged_data, review_data, "merged_data", "zcta_review")
        merged_data = merged_data.merge(review_data, on='zcta', how='left')
    else:
        print("Skipping merge with zcta_review: review_data is None or 'zcta' column missing in zcta_review.csv.")
    print(f"Shape after zcta_review merge attempt: {merged_data.shape}")

    # Merge with other datasets
    if 'income_data' in datasets:
        print_merge_info(merged_data, datasets['income_data'], "merged_data", "income_data")
        merged_data = merged_data.merge(datasets['income_data'], on='zcta', how='left')
    else:
        print("Skipping merge with income_data: dataset not found.")
    print(f"Shape after income_data merge attempt: {merged_data.shape}")

    if 'crime_data' in datasets:
        print_merge_info(merged_data, datasets['crime_data'], "merged_data", "crime_data")
        merged_data = merged_data.merge(datasets['crime_data'], on='zcta', how='left')
    else:
        print("Skipping merge with crime_data: dataset not found.")
    print(f"Shape after crime_data merge attempt: {merged_data.shape}")

    if 'sunlight_data' in datasets:
        print_merge_info(merged_data, datasets['sunlight_data'], "merged_data", "sunlight_data")
        merged_data = merged_data.merge(datasets['sunlight_data'], on='zcta', how='left')
    else:
        print("Skipping merge with sunlight_data: dataset not found.")
    print(f"Shape after sunlight_data merge attempt: {merged_data.shape}")

    # Create automated data folder if it doesn't exist
    data_folder = "automated data"
    os.makedirs(data_folder, exist_ok=True)

    # Generate CSV filename with UTC datetime
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_filename = f"merged_data_{timestamp}.csv"
    csv_path = os.path.join(data_folder, csv_filename)

    # Save merged data to CSV
    print(f"Saving merged data to: {os.path.abspath(csv_path)}")
    merged_data.to_csv(csv_path, index=False)
    print(f"Merged data saved to {csv_path}")


if __name__ == "__main__":
    join_data()