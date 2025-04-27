import pandas as pd
import os
import glob
import json
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
    """Print the column names, shape, and sample data of a loaded dataset."""
    print(f"Dataset {dataset_name} info:")
    print(f"Columns: {list(df.columns)}")
    print(f"Shape: {df.shape}")
    # Print sample zcta values (first 5 rows)
    if 'zcta' in df.columns and not df.empty:
        sample_zctas = df['zcta'].head().tolist()
        print(f"Sample zcta values (first 5): {sample_zctas}")
    print()


def join_data():
    print("Starting data merging process...")

    # Collect lineage data for visualization
    lineage_data = []

    # Load datasets if they exist
    datasets = {}

    # ZCTA data
    zcta_file = get_latest_csv("zcta_data")
    if zcta_file:
        print("Loading ZCTA data...")
        datasets['zcta_data'] = pd.read_csv(zcta_file)
        # Ensure zcta is a string
        datasets['zcta_data']['zcta'] = datasets['zcta_data']['zcta'].astype(str).str.strip()
        print_dataset_info(datasets['zcta_data'], "zcta_data")
        lineage_data.append({
            'type': 'dataset',
            'name': 'zcta_data',
            'shape': datasets['zcta_data'].shape,
            'columns': list(datasets['zcta_data'].columns)
        })
    else:
        print("ZCTA data is required. Exiting.")
        return

    # ACS income data
    income_file = get_latest_csv("income_data")
    if income_file:
        print("Loading ACS income data...")
        datasets['income_data'] = pd.read_csv(income_file)
        # Ensure zcta is a string
        datasets['income_data']['zcta'] = datasets['income_data']['zcta'].astype(str).str.strip()
        print_dataset_info(datasets['income_data'], "income_data")
        lineage_data.append({
            'type': 'dataset',
            'name': 'income_data',
            'shape': datasets['income_data'].shape,
            'columns': list(datasets['income_data'].columns)
        })
    else:
        print("ACS income data not found. Skipping.")

    # Crime data
    crime_file = get_latest_csv("crime_data")
    if crime_file:
        print("Loading crime data...")
        datasets['crime_data'] = pd.read_csv(crime_file)
        # Ensure zcta is a string
        datasets['crime_data']['zcta'] = datasets['crime_data']['zcta'].astype(str).str.strip()
        print_dataset_info(datasets['crime_data'], "crime_data")
        lineage_data.append({
            'type': 'dataset',
            'name': 'crime_data',
            'shape': datasets['crime_data'].shape,
            'columns': list(datasets['crime_data'].columns)
        })
    else:
        print("Crime data not found. Skipping.")

    # Sunlight data
    sunlight_file = get_latest_csv("sunlight_data")
    if sunlight_file:
        print("Loading sunlight data...")
        datasets['sunlight_data'] = pd.read_csv(sunlight_file)
        # Ensure zcta is a string
        datasets['sunlight_data']['zcta'] = datasets['sunlight_data']['zcta'].astype(str).str.strip()
        print_dataset_info(datasets['sunlight_data'], "sunlight_data")
        lineage_data.append({
            'type': 'dataset',
            'name': 'sunlight_data',
            'shape': datasets['sunlight_data'].shape,
            'columns': list(datasets['sunlight_data'].columns)
        })
    else:
        print("Sunlight data not found. Skipping.")

    # Load zip_zcta_xref.csv
    xref_file = os.path.join("manual data", "zip_zcta_xref.csv")
    if os.path.exists(xref_file):
        print("Loading zip_zcta_xref data...")
        xref_data = pd.read_csv(xref_file)
        # Ensure zcta is a string and remove .0 suffix
        xref_data['zcta'] = pd.to_numeric(xref_data['zcta'], errors='coerce').fillna(0).astype(int).astype(str)
        xref_data['zcta'] = xref_data['zcta'].replace('0', pd.NA)  # Replace dummy 0 with NA for rows that were NaN
        print_dataset_info(xref_data, "zip_zcta_xref")
        lineage_data.append({
            'type': 'dataset',
            'name': 'zip_zcta_xref',
            'shape': xref_data.shape,
            'columns': list(xref_data.columns)
        })
        # Check if required columns exist (updated to expect 'zip_code')
        required_columns = ['zcta', 'zip_code', 'source']
        missing_columns = [col for col in required_columns if col not in xref_data.columns]
        if missing_columns:
            print(f"Missing required columns in zip_zcta_xref.csv: {missing_columns}. Skipping merge.")
            xref_data = None
        else:
            print("Columns in zip_zcta_xref.csv (after validation):", list(xref_data.columns))
            # Log sample rows to inspect data
            print("Sample rows from zip_zcta_xref (first 5):")
            print(xref_data.head().to_string(index=False))
    else:
        print(f"zip_zcta_xref.csv not found in manual data folder. Skipping merge.")
        xref_data = None

    # Load zcta_review.csv
    review_file = os.path.join("manual data", "zcta_review.csv")
    if os.path.exists(review_file):
        print("Loading zcta_review data...")
        review_data = pd.read_csv(review_file)
        # Ensure zcta is a string
        review_data['zcta'] = review_data['zcta'].astype(str).str.strip()
        # Drop the zip column from zcta_review to avoid duplicates
        if 'zip' in review_data.columns:
            print("Dropping 'zip' column from zcta_review to avoid conflict with zip_zcta_xref.")
            review_data = review_data.drop(columns=['zip'])
        print_dataset_info(review_data, "zcta_review")
        lineage_data.append({
            'type': 'dataset',
            'name': 'zcta_review',
            'shape': review_data.shape,
            'columns': list(review_data.columns)
        })
        # Check if 'zcta' column exists
        if 'zcta' not in review_data.columns:
            print("Missing 'zcta' column in zcta_review.csv. Skipping merge.")
            review_data = None
        else:
            # Ensure zcta formatting is consistent
            review_data['zcta'] = pd.to_numeric(review_data['zcta'], errors='coerce').fillna(0).astype(int).astype(str)
            review_data['zcta'] = review_data['zcta'].replace('0', pd.NA)
    else:
        print(f"zcta_review.csv not found in manual data folder. Skipping merge.")
        review_data = None

    # Merge datasets
    print("Merging datasets...")
    zcta_data = datasets['zcta_data']
    print(f"Initial shape of merged_data: {zcta_data.shape}")
    current_output = 'merged_data_1'
    lineage_data.append({
        'type': 'output',
        'name': current_output,
        'shape': zcta_data.shape,
        'columns': list(zcta_data.columns)
    })

    # Merge with zip_zcta_xref.csv on zcta
    if xref_data is not None:
        # Keep zip_code as-is (do not rename to zip)
        xref_subset = xref_data[['zcta', 'zip_code', 'source']]
        print_merge_info(zcta_data, xref_subset, current_output, "zip_zcta_xref")
        # Check for overlapping zcta values using merge to count matches accurately
        merged_with_zip = zcta_data.merge(xref_subset, on='zcta', how='left')
        merged_with_zip.to_csv('merged_with_zip.csv', index=False)
        matched_zctas = merged_with_zip['zip_code'].notnull().sum()
        total_zctas = len(merged_with_zip)
        print(f"Number of zcta values matched with zip_zcta_xref: {matched_zctas}/{total_zctas}")
        # Log non-matching zcta values
        non_matching = merged_with_zip[merged_with_zip['zip_code'].isnull()]['zcta'].head().tolist()
        if non_matching:
            print(f"Sample zcta values with no match in zip_zcta_xref (first 5): {non_matching}")
        merged_data = merged_with_zip
        # Log sample merged rows
        print("Sample rows after merge with zip_zcta_xref (first 5):")
        print(merged_data.head().to_string(index=False))
        # Check for specific ZCTA
        zcta_47660 = merged_data[merged_data['zcta'] == '47660']
        if not zcta_47660.empty:
            columns_to_log = ['zcta', 'zip_code'] if 'zip_code' in merged_data.columns else ['zcta']
            print(f"ZCTA 47660 after merge with zip_zcta_xref: {zcta_47660[columns_to_log].to_dict('records')}")
        else:
            print("ZCTA 47660 not found in merged data after zip_zcta_xref merge.")
        next_output = 'merged_data_2'
        lineage_data.append({
            'type': 'merge',
            'input1': current_output,
            'input2': 'zip_zcta_xref',
            'join_key': 'zcta',
            'output': next_output
        })
        lineage_data.append({
            'type': 'output',
            'name': next_output,
            'shape': merged_data.shape,
            'columns': list(merged_data.columns)
        })
        current_output = next_output
    else:
        print("Skipping merge with zip_zcta_xref due to missing data or columns.")
    print(f"Shape after zip_zcta_xref merge attempt: {merged_data.shape}")

    # Merge with zcta_review.csv on zcta
    if review_data is not None:
        print_merge_info(merged_data, review_data, current_output, "zcta_review")
        # Check for overlapping zcta values using merge to count matches accurately
        merged_with_review = merged_data.merge(review_data, on='zcta', how='left')
        matched_zctas = merged_with_review['city'].notnull().sum()
        total_zctas = len(merged_with_review)
        print(f"Number of zcta values matched with zcta_review: {matched_zctas}/{total_zctas}")
        # Log non-matching zcta values
        non_matching = merged_with_review[merged_with_review['city'].isnull()]['zcta'].head().tolist()
        if non_matching:
            print(f"Sample zcta values with no match in zcta_review (first 5): {non_matching}")
        merged_data = merged_with_review
        # Log sample merged rows
        print("Sample rows after merge with zcta_review (first 5):")
        print(merged_data.head().to_string(index=False))
        # Check for specific ZCTA
        zcta_47660 = merged_data[merged_data['zcta'] == '47660']
        if not zcta_47660.empty:
            columns_to_log = ['zcta', 'zip_code', 'city'] if 'zip_code' in merged_data.columns else ['zcta', 'city']
            print(f"ZCTA 47660 after merge with zcta_review: {zcta_47660[columns_to_log].to_dict('records')}")
        else:
            print("ZCTA 47660 not found in merged data after zcta_review merge.")
        next_output = 'merged_data_3'
        lineage_data.append({
            'type': 'merge',
            'input1': current_output,
            'input2': 'zcta_review',
            'join_key': 'zcta',
            'output': next_output
        })
        lineage_data.append({
            'type': 'output',
            'name': next_output,
            'shape': merged_data.shape,
            'columns': list(merged_data.columns)
        })
        current_output = next_output
    else:
        print("Skipping merge with zcta_review: review_data is None or 'zcta' column missing in zcta_review.csv.")
    print(f"Shape after zcta_review merge attempt: {merged_data.shape}")

    # Merge with other datasets
    if 'income_data' in datasets:
        print_merge_info(merged_data, datasets['income_data'], current_output, "income_data")
        # Check for overlapping zcta values using merge to count matches accurately
        merged_with_income = merged_data.merge(datasets['income_data'], on='zcta', how='left')
        matched_zctas = merged_with_income['median_household_income'].notnull().sum()
        total_zctas = len(merged_with_income)
        print(f"Number of zcta values matched with income_data: {matched_zctas}/{total_zctas}")
        merged_data = merged_with_income
        next_output = 'merged_data_4'
        lineage_data.append({
            'type': 'merge',
            'input1': current_output,
            'input2': 'income_data',
            'join_key': 'zcta',
            'output': next_output
        })
        lineage_data.append({
            'type': 'output',
            'name': next_output,
            'shape': merged_data.shape,
            'columns': list(merged_data.columns)
        })
        current_output = next_output
    else:
        print("Skipping merge with income_data: dataset not found.")
    print(f"Shape after income_data merge attempt: {merged_data.shape}")

    if 'crime_data' in datasets:
        print_merge_info(merged_data, datasets['crime_data'], current_output, "crime_data")
        # Check for overlapping zcta values using merge to count matches accurately
        merged_with_crime = merged_data.merge(datasets['crime_data'], on='zcta', how='left')
        matched_zctas = merged_with_crime['crime_grade'].notnull().sum()
        total_zctas = len(merged_with_crime)
        print(f"Number of zcta values matched with crime_data: {matched_zctas}/{total_zctas}")
        merged_data = merged_with_crime
        next_output = 'merged_data_5'
        lineage_data.append({
            'type': 'merge',
            'input1': current_output,
            'input2': 'crime_data',
            'join_key': 'zcta',
            'output': next_output
        })
        lineage_data.append({
            'type': 'output',
            'name': next_output,
            'shape': merged_data.shape,
            'columns': list(merged_data.columns)
        })
        current_output = next_output
    else:
        print("Skipping merge with crime_data: dataset not found.")
    print(f"Shape after crime_data merge attempt: {merged_data.shape}")

    if 'sunlight_data' in datasets:
        print_merge_info(merged_data, datasets['sunlight_data'], current_output, "sunlight_data")
        # Check for overlapping zcta values using merge to count matches accurately
        merged_with_sunlight = merged_data.merge(datasets['sunlight_data'], on='zcta', how='left')
        matched_zctas = merged_with_sunlight['sunlight_hours_per_year'].notnull().sum()
        total_zctas = len(merged_with_sunlight)
        print(f"Number of zcta values matched with sunlight_data: {matched_zctas}/{total_zctas}")
        merged_data = merged_with_sunlight
        next_output = 'merged_data_final'
        lineage_data.append({
            'type': 'merge',
            'input1': current_output,
            'input2': 'sunlight_data',
            'join_key': 'zcta',
            'output': next_output
        })
        lineage_data.append({
            'type': 'output',
            'name': next_output,
            'shape': merged_data.shape,
            'columns': list(merged_data.columns)
        })
    else:
        print("Skipping merge with sunlight_data: dataset not found.")
        # If sunlight_data is the last merge, set the final output name
        next_output = 'merged_data_final'
        lineage_data.append({
            'type': 'output',
            'name': next_output,
            'shape': merged_data.shape,
            'columns': list(merged_data.columns)
        })
    print(f"Shape after sunlight_data merge attempt: {merged_data.shape}")

    # Create automated data folder if it doesn't exist
    data_folder = "automated data"
    os.makedirs(data_folder, exist_ok=True)

    # Create automated data lineage folder if it doesn't exist
    lineage_folder = "automated data lineage"
    os.makedirs(lineage_folder, exist_ok=True)
    print(f"Permissions for {lineage_folder}:")
    os.system(f"ls -ld '{lineage_folder}'")

    # Generate CSV filename with UTC datetime
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_filename = f"merged_data_{timestamp}.csv"
    csv_path = os.path.join(data_folder, csv_filename)

    # Save lineage data to JSON in automated data lineage folder
    lineage_filename = f"join_lineage_{timestamp}.json"
    lineage_file = os.path.join(lineage_folder, lineage_filename)
    with open(lineage_file, 'w') as f:
        json.dump(lineage_data, f, indent=4)
    print(f"Lineage data saved to: {lineage_file}")

    # Save merged data to CSV
    print(f"Saving merged data to: {os.path.abspath(csv_path)}")
    merged_data.to_csv(csv_path, index=False)
    print(f"Merged data saved to {csv_path}")


if __name__ == "__main__":
    join_data()