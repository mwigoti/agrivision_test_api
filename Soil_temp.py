import cdsapi
import xarray as xr
import json
import os
from datetime import datetime
import numpy as np

# Initialize CDS client
client = cdsapi.Client()

# Create a temporary file for the NetCDF data
temp_file = 'temp_era5_data.nc'

# Dataset and request parameters
dataset = "reanalysis-era5-land"
# Use the correct short variable names in the request
request = {
    "variable": [
        "stl1",  # soil temperature level 1
        "swvl1", # volumetric soil water layer 1
        "e"      # total evaporation
    ],
    "year": "2017",
    "month": "11",
    "day": ["11"],
    "time": ["00:00"],
    "format": "netcdf",
    "area": [0, 36, -1.23, 36.32]
}

try:
    # Download data to temporary file
    client.retrieve(dataset, request, temp_file)
    
    # Read the NetCDF file using xarray
    ds = xr.open_dataset(temp_file, engine='netcdf4')
    
    # Create JSON structure
    data = {
        "metadata": {
            "timestamp": "2017-11-11T00:00:00",
            "area": {
                "north": 0,
                "east": 36.32,
                "south": -1.23,
                "west": 36
            },
            "source": "ERA5-Land"
        },
        "data": {}
    }
    
    # Variable mapping for human-readable names
    variable_mapping = {
        'stl1': 'soil_temperature_level_1',
        'swvl1': 'volumetric_soil_water_layer_1',
        'e': 'total_evaporation'
    }
    
    # Process each variable using the correct short names
    for var_short_name in request["variable"]:
        if var_short_name in ds:
            values = ds[var_short_name].values
            readable_name = variable_mapping[var_short_name]
            data["data"][readable_name] = {
                "mean": float(np.nanmean(values)),
                "min": float(np.nanmin(values)),
                "max": float(np.nanmax(values)),
                "unit": str(ds[var_short_name].units) if hasattr(ds[var_short_name], 'units') else "unknown"
            }
    
    # Save to JSON file
    output_file = 'era5_data_2017_11_11.json'
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Data successfully converted and saved to {output_file}")

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    # Clean up temporary file
    if os.path.exists(temp_file):
        os.remove(temp_file)
        
    # Close the dataset if it was opened
    if 'ds' in locals():
        ds.close()