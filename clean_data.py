import pandas as pd
import os
import glob
import json
from datetime import datetime, timezone
from graphviz import Digraph


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


def get_latest_lineage(dataset_name, folder="automated data"):
    """Find the most recent lineage JSON file for a given dataset in the specified folder."""
    pattern = os.path.join(folder, f"{dataset_name}_*.json")
    files = glob.glob(pattern)
    if not files:
        print(f"No lineage file found for {dataset_name} in {folder}")
        return None

    # Extract timestamp from filename and find the most recent
    def extract_timestamp(filepath):
        filename = os.path.basename(filepath)
        timestamp_str = filename.split('_')[-2] + '_' + filename.split('_')[-1].replace('.json', '')
        return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")

    latest_file = max(files, key=extract_timestamp)
    print(f"Found latest lineage file for {dataset_name}: {latest_file}")
    with open(latest_file, 'r') as f:
        return json.load(f)


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


def generate_data_lineage_graph(lineage_data, output_path):
    """Generate a data lineage graph using Graphviz."""
    print("Generating data lineage graph...")
    print(f"Output path: {output_path}")
    print(f"Lineage data entries: {len(lineage_data)}")
    for entry in lineage_data:
        print(f"Lineage entry: {entry}")

    dot = Digraph(comment='Data Lineage Graph', format='png')
    dot.attr(rankdir='LR')  # Left to right layout

    # Add nodes for datasets and operations
    print("Building graph nodes and edges...")
    for entry in lineage_data:
        if entry['type'] == 'dataset':
            node_id = entry['name']
            label = f"{entry['name']}\\nShape: {entry['shape']}\\nColumns: {', '.join(entry['columns'])}"
            dot.node(node_id, label=label, shape='box', style='filled', fillcolor='lightblue')
        elif entry['type'] == 'merge':
            node_id = f"merge_{entry['output']}"
            label = f"Merge\\nJoin on: {entry['join_key']}"
            dot.node(node_id, label=label, shape='ellipse', style='filled', fillcolor='lightgreen')
            dot.edge(entry['input1'], node_id)
            dot.edge(entry['input2'], node_id)
            dot.edge(node_id, entry['output'])
        elif entry['type'] == 'operation':
            node_id = f"op_{entry['name']}"
            label = f"{entry['name']}\\n{entry['details']}"
            dot.node(node_id, label=label, shape='ellipse', style='filled', fillcolor='lightgreen')
            dot.edge(entry['input'], node_id)
            dot.edge(node_id, entry['output'])
        elif entry['type'] == 'output':
            node_id = entry['name']
            label = f"{entry['name']}\\nShape: {entry['shape']}\\nColumns: {', '.join(entry['columns'])}"
            dot.node(node_id, label=label, shape='box', style='filled', fillcolor='lightyellow')

    # Save the graph with detailed error handling
    try:
        print(f"Attempting to render graph to: {output_path}")
        dot.save(output_path + '.dot')  # Explicitly save the .dot file
        print(f"Saved .dot file to: {output_path}.dot")
        dot.render(output_path, view=False, cleanup=False)  # Keep the .dot file for debugging
        print(f"Unified data lineage graph saved to: {output_path}.png")
        # Verify the files exist
        dot_file = f"{output_path}.dot"
        png_file = f"{output_path}.png"
        if os.path.exists(dot_file):
            print(f"Found .dot file: {dot_file}")
        else:
            print(f".dot file not found: {dot_file}")
        if os.path.exists(png_file):
            print(f"Found .png file: {png_file}")
        else:
            print(f".png file not found: {png_file}")
    except Exception as e:
        print(f"Error rendering Graphviz graph: {e}")
        print("Ensure Graphviz is installed and the 'dot' executable is in your PATH.")
        print(f"You can manually render the .dot file using: dot -Tpng {output_path}.dot -o {output_path}.png")


def clean_data():
    print("Starting data cleaning process...")
    print(f"Current working directory: {os.getcwd()}")
    print(f"PATH environment variable: {os.environ['PATH']}")

    # Load lineage data from join_data.py
    join_lineage = get_latest_lineage("join_lineage")
    lineage_data = join_lineage if join_lineage is not None else []

    # Load the most recent merged data
    merged_file = get_latest_csv("merged_data")
    if not merged_file:
        print("Merged data is required. Exiting.")
        return

    print("Loading merged data...")
    merged_data = pd.read_csv(merged_file)
    print(f"Initial columns in merged_data: {list(merged_data.columns)}")
    print(f"Initial shape of merged_data: {merged_data.shape}")
    lineage_data.append({
        'type': 'dataset',
        'name': 'merged_data',
        'shape': merged_data.shape,
        'columns': list(merged_data.columns)
    })

    # Rename columns
    merged_data = merged_data.rename(columns={
        'latitude_x': 'zcta_latitude',
        'longitude_x': 'zcta_longitude',
        'latitude_y': 'city_latitude',
        'longitude_y': 'city_longitude',
        'notes': 'zcta_review_notes'
    })
    lineage_data.append({
        'type': 'operation',
        'name': 'rename_columns',
        'details': 'Renamed: latitude_x->zcta_latitude, longitude_x->zcta_longitude, latitude_y->city_latitude, longitude_y->city_longitude, notes->zcta_review_notes',
        'input': 'merged_data',
        'output': 'renamed_data'
    })
    lineage_data.append({
        'type': 'output',
        'name': 'renamed_data',
        'shape': merged_data.shape,
        'columns': list(merged_data.columns)
    })

    # Drop specified columns
    columns_to_drop = ['point map url', 'zip map url', 'census_reporter_check']
    merged_data = merged_data.drop(columns=columns_to_drop, errors='ignore')
    lineage_data.append({
        'type': 'operation',
        'name': 'drop_columns',
        'details': f"Dropped: {', '.join(columns_to_drop)}",
        'input': 'renamed_data',
        'output': 'dropped_data'
    })
    lineage_data.append({
        'type': 'output',
        'name': 'dropped_data',
        'shape': merged_data.shape,
        'columns': list(merged_data.columns)
    })

    # Reorder columns: start with zcta, zip, city, stusab, then the rest
    priority_columns = ['zcta', 'zip', 'city', 'stusab']
    priority_columns = [col for col in priority_columns if col in merged_data.columns]
    remaining_columns = [col for col in merged_data.columns if col not in priority_columns]
    new_column_order = priority_columns + remaining_columns
    merged_data = merged_data[new_column_order]
    lineage_data.append({
        'type': 'operation',
        'name': 'reorder_columns',
        'details': f"Priority columns: {', '.join(priority_columns)}",
        'input': 'dropped_data',
        'output': 'reordered_data'
    })
    lineage_data.append({
        'type': 'output',
        'name': 'reordered_data',
        'shape': merged_data.shape,
        'columns': list(merged_data.columns)
    })

    # Select relevant columns
    columns_to_keep = [
        'zcta', 'zip', 'city', 'stusab', 'zcta_latitude', 'zcta_longitude',
        'city_latitude', 'city_longitude', 'median_household_income',
        'crime_grade', 'sunlight_hours_per_year', 'zcta_review_notes'
    ]
    columns_to_keep = [col for col in columns_to_keep if col in merged_data.columns]
    result = merged_data[columns_to_keep]
    lineage_data.append({
        'type': 'operation',
        'name': 'select_columns',
        'details': f"Selected: {', '.join(columns_to_keep)}",
        'input': 'reordered_data',
        'output': 'cleaned_data'
    })
    lineage_data.append({
        'type': 'output',
        'name': 'cleaned_data',
        'shape': result.shape,
        'columns': list(result.columns)
    })

    # Compare distinct values between merged_data and result
    columns_to_compare = ['zcta', 'zip', 'city', 'stusab']
    compare_distinct_values(merged_data, result, columns_to_compare)

    # Create automated data folder if it doesn't exist
    data_folder = "automated data"
    os.makedirs(data_folder, exist_ok=True)
    print(f"Permissions for {data_folder}:")
    os.system(f"ls -ld '{data_folder}'")

    # Create automated data lineage folder if it doesn't exist
    lineage_folder = "automated data lineage"
    os.makedirs(lineage_folder, exist_ok=True)
    print(f"Permissions for {lineage_folder}:")
    os.system(f"ls -ld '{lineage_folder}'")

    # Generate unified data lineage graph
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    # Sanitize the filename to avoid spaces and special characters
    graph_filename = f"data_lineage_unified_{timestamp}".replace(" ", "_").replace("(", "").replace(")", "")
    graph_path = os.path.join(lineage_folder, graph_filename)
    generate_data_lineage_graph(lineage_data, graph_path)

    # Generate CSV filename with UTC datetime
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