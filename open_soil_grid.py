import requests
import pandas as pd

def get_soilgrid_data(latitude, longitude):
    # SoilGrids v2.0 REST API endpoint
    base_url = "https://rest.isric.org"
    endpoint = f"soilgrids/v2.0/properties/query?lat={latitude}&lon={longitude}"
    
    try:
        response = requests.get(f"{base_url}/{endpoint}")
        if response.status_code == 200:
            data = response.json()
            
            # Extract soil properties
            properties = data['properties']['layers']
            
            # Convert to pandas DataFrame
            soil_data = {}
            for prop in properties:
                soil_data[prop['name']] = prop['depths'][0]['values']['mean']
                
            return pd.DataFrame([soil_data])
    except Exception as e:
        print(f"Error: {e}")
        return None

# Example usage
latitude = 20.5937
longitude = 78.9629
soil_data = get_soilgrid_data(latitude, longitude)
print(soil_data)