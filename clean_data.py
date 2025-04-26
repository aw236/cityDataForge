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


def compare_distinct_values(df1, df2, columns, df1_name="merged_data", df2_name="cleaned_data"):
    """Compare the number of distinct values for specified columns between two DataFrames."""
    print("\nComparing distinct values between datasets:")
    for col in columns:
        if col in df1.columns and col in df2.columns:
            df1_distinct = df1[col].nunique()
            df2_distinct = df2[col].nunique()
            print(f"Distinct values for '{col}': {df1_name} = {df1_distinct}, {df2_name} = {df2_distinct}")
        else:
            print(
                f"Column '{col}' not found in one or both datasets: {df1_name} = {col in df1.columns}, {df2_name} = {col in df2.columns}")
    print()


def clean_data():
    print("Starting data cleaning process...")

    # Load the most recent merged data
    merged_file = get_latest_csv("merged_data")
    if not merged_file:
        print("Merged data is required. Exiting.")
        return

    print("Loading merged data...")
    merged_data = pd.read_csv(merged_file)
    print(f"Initial columns in merged_data: {list(merged_data.columns)}")
    print(f"Initial shape of merged_data: {merged_data.shape}")

    # Rename columns
    merged_data = merged_data.rename(columns={
        'latitude_x': 'zcta_latitude',
        'longitude_x': 'zcta_longitude',
        'latitude_y': 'city_latitude',
        'longitude_y': 'city_longitude',
        'notes': 'zcta_review_notes'
    })

    # Drop specified columns
    columns_to_drop = ['point map url', 'zip map url', 'census_reporter_check']
    merged_data = merged_data.drop(columns=columns_to_drop, errors='ignore')

    # Reorder columns: start with zcta, zip, city, stusab, then the rest
    priority_columns = ['zcta', 'zip', 'city', 'stusab']
    # Ensure all priority columns exist in the DataFrame
    priority_columns = [col for col in priority_columns if col in merged_data.columns]
    # Get remaining columns (excluding priority ones)
    remaining_columns = [col for col in merged_data.columns if col not in priority_columns]
    # New column order
    new_column_order = priority_columns + remaining_columns
    merged_data = merged_data[new_column_order]

    # Select relevant columns (ensure these exist after renaming and dropping)
    columns_to_keep = [
        'zcta', 'zip', 'city', 'stusab', 'zcta_latitude', 'zcta_longitude',
        'city_latitude', 'city_longitude', 'median_household_income',
        'crime_grade', 'sunlight_hours_per_year', 'zcta_review_notes'
    ]
    # Filter to columns that exist in the DataFrame
    columns_to_keep = [col for col in columns_to_keep if col in merged_data.columns]
    result = merged_data[columns_to_keep]

    # Compare distinct values between merged_data and result
    columns_to_compare = ['zcta', 'zip', 'city', 'stusab']
    compare_distinct_values(merged_data, result, columns_to_compare)

    # Create automated data folder if it doesn't exist
    data_folder = "automated data"
    os.makedirs(data_folder, exist_ok=True)

    # Generate CSV filename with UTC datetime
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_filename = f"cleaned_data_{timestamp}.csv"
    csv_path = os.path.join(data_folder, csv_filename)

    # Save to CSV
    print(f"Saving cleaned data to: {os.path.abspath(csv_path)}")
    result.to_csv(csv_path, index=False)
    print(f"Final cleaned data saved to {csv_path}")
    print(f"Final columns in result: {list(result.columns)}")
    print(f"Final shape of result: {result.shape}")
    print("\nSample of final dataset:")
    print(result.head())


if __name__ == "__main__":
    clean_data()