import pandas as pd
import uuid

# Sample data sources (replace with actual data files or API calls)
# 1. ZCTA database (e.g., from Census TIGER/Line or SimpleMaps)
zcta_data = pd.DataFrame({
    'zcta': ['10001', '60601', '94109', '33101'],
    'city': ['New York', 'Chicago', 'San Francisco', 'Miami'],
    'state': ['NY', 'IL', 'CA', 'FL'],
    'latitude': [40.7508, 41.8850, 37.7920, 25.7743],
    'longitude': [-73.9961, -87.6220, -122.4220, -80.1937]
})

# 2. Income data (e.g., from Census ACS 2023, Table B19013)
income_data = pd.DataFrame({
    'zcta': ['10001', '60601', '94109', '33101'],
    'median_household_income': [85000, 95000, 82000, 65000]  # Placeholder values
})

# 3. Crime data (e.g., from NeighborhoodScout or CrimeGrade)
crime_data = pd.DataFrame({
    'zcta': ['10001', '60601', '94109', '33101'],
    'crime_grade': ['C', 'B', 'D', 'C']  # Placeholder: A (low) to F (high)
})

# 4. Sunlight hours (approximated using NREL NSRDB or Unbound Solar data)
sunlight_data = pd.DataFrame({
    'zcta': ['10001', '60601', '94109', '33101'],
    'peak_sun_hours_per_day': [4.0, 4.2, 5.0, 5.5],  # Approximated
    'sunlight_hours_per_year': [4.0 * 365, 4.2 * 365, 5.0 * 365, 5.5 * 365]
})

# Merge datasets
merged_data = zcta_data.merge(income_data, on='zcta', how='left')
merged_data = merged_data.merge(crime_data, on='zcta', how='left')
merged_data = merged_data.merge(sunlight_data, on='zcta', how='left')

# Select relevant columns
result = merged_data[['zcta', 'city', 'state', 'median_household_income',
                      'crime_grade', 'sunlight_hours_per_year']]

result.to_csv('zcta_data_table.csv', index=False) # Save to CSV
print(result) # Print sample table

# Note: To scale to all ZCTAs, replace sample data with:
# - Census TIGER/Line ZCTA shapefiles or SimpleMaps ZCTA database (CSV)
# - Census ACS 2023 ZCTA income data (via API or CSV from data.census.gov)
# - NeighborhoodScout/CrimeGrade API for crime data (if available)
# - NREL NSRDB solar insolation data mapped to ZCTA centroids