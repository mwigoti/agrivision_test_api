import cdsapi
import xarray as xr
import requests
import pandas as pd
import json
import os
from datetime import datetime
import time
import numpy as np
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor

class IntegratedSoilAnalyzer:
    """Class for integrated soil and climate data analysis using ERA5 and SoilGrids"""
    
    def __init__(self, huggingface_token: str = None):
        """Initialize with required configurations"""
        self.huggingface_token = huggingface_token or os.getenv("HUGGINGFACE_TOKEN")
        if not self.huggingface_token:
            raise ValueError("Hugging Face token is required")
            
        # Falcon-7B configuration
        self.model_url = "https://api-inference.huggingface.co/models/tiiuae/falcon-7b-instruct"
        self.headers = {"Authorization": f"Bearer {self.huggingface_token}"}
        
        # CDS client initialization
        self.cds_client = cdsapi.Client()
        
        # Request configuration
        self.max_retries = 3
        self.retry_delay = 5
        self.last_request_time = None
        self.min_request_interval = 2
    
    def get_era5_data(self, latitude: float, longitude: float, date: str) -> Dict[str, Any]:
        """Fetch ERA5 climate data"""
        temp_file = 'temp_era5_data.nc'
        year, month, day = date.split('-')
        
        # Calculate area bounds (0.5 degree buffer)
        area = [
            latitude + 0.5, longitude - 0.5,
            latitude - 0.5, longitude + 0.5
        ]
        
        request = {
            "variable": ["stl1", "swvl1", "e"],
            "year": year,
            "month": month,
            "day": [day],
            "time": ["00:00"],
            "format": "netcdf",
            "area": area
        }
        
        try:
            self.cds_client.retrieve("reanalysis-era5-land", request, temp_file)
            ds = xr.open_dataset(temp_file, engine='netcdf4')
            
            data = {
                "metadata": {
                    "timestamp": f"{date}T00:00:00",
                    "area": {
                        "north": area[0],
                        "east": area[3],
                        "south": area[2],
                        "west": area[1]
                    },
                    "source": "ERA5-Land"
                },
                "data": {}
            }
            
            variable_mapping = {
                'stl1': 'soil_temperature_level_1',
                'swvl1': 'volumetric_soil_water_layer_1',
                'e': 'total_evaporation'
            }
            
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
            
            return data
            
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
            if 'ds' in locals():
                ds.close()

    def get_soilgrid_data(self, latitude: float, longitude: float) -> Optional[pd.DataFrame]:
        """Fetch soil data from SoilGrids"""
        base_url = "https://rest.isric.org"
        endpoint = f"soilgrids/v2.0/properties/query?lat={latitude}&lon={longitude}"
        
        try:
            response = requests.get(f"{base_url}/{endpoint}")
            response.raise_for_status()
            
            data = response.json()
            properties = data['properties']['layers']
            
            soil_data = {}
            for prop in properties:
                soil_data[prop['name']] = prop['depths'][0]['values']['mean']
                
            return pd.DataFrame([soil_data])
            
        except Exception as e:
            print(f"SoilGrids API Error: {e}")
            return None

   
    def analyze_combined_data(self, soil_data: pd.DataFrame, climate_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze combined soil and climate data using Falcon-7B"""
        combined_data = {
            "soil_properties": soil_data.to_dict(orient='records')[0],
            "climate_conditions": climate_data['data']
        }
        
        prompt = f"""You are an expert agricultural scientist and soil specialist. Analyze the following integrated soil and climate data to provide a detailed agricultural assessment and recommendations. Focus on practical, actionable insights.

Context:
The data provided combines soil properties from SoilGrids and climate conditions from ERA5-Land satellite data. All measurements are from ground level to 30cm depth.

Data for Analysis:
{json.dumps(combined_data, indent=2)}

PProvide a comprehensive analysis covering:

    Soil Health Assessment: Analyze texture, structure, nutrients, organic matter, deficiencies; rate health (1-10).
    Climate Impact: Assess temperature, moisture, evaporation; suggest climate-smart practices.
    Crop Recommendations: Suggest top 3 crops with yields, planting times, varieties, management.
    Soil Plan: Propose amendments, fertilizers, conservation, irrigation.
    Risk & Mitigation: Identify degradation risks, imbalances, pests; preventive measures.
    Economics: Cost analysis, ROI, low-cost options."""

        retries = 0
        current_delay = self.retry_delay
        
        while retries < self.max_retries:
            try:
                if self.last_request_time:
                    elapsed = (datetime.now() - self.last_request_time).total_seconds()
                    if elapsed < self.min_request_interval:
                        time.sleep(self.min_request_interval - elapsed)
                
                self.last_request_time = datetime.now()
                
                response = requests.post(
                    self.model_url,
                    headers=self.headers,
                    json={
                        "inputs": prompt,
                        "parameters": {
                            "max_new_tokens": 1000,  # Increased token limit for more detailed response
                            "temperature": 0.7,
                            "top_p": 0.95,
                            "return_full_text": False
                        }
                    },
                    timeout=45  # Increased timeout for longer response
                )
                response.raise_for_status()
                
                result = response.json()
                analysis = result[0]['generated_text'] if isinstance(result, list) else result.get('generated_text', str(result))
                
                return {
                    "success": True,
                    "model": "falcon-7b-instruct",
                    "analysis": analysis,
                    "timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                print(f"Analysis attempt {retries + 1} failed: {e}")
                retries += 1
                if retries < self.max_retries:
                    time.sleep(current_delay)
                    current_delay *= 2
        
        return {
            "success": False,
            "error": "Max retries exceeded",
            "model": "falcon-7b-instruct",
            "timestamp": datetime.now().isoformat()
        }
def main():
    """Main execution function"""
    try:
        analyzer = IntegratedSoilAnalyzer()
        
        # Example coordinates and date
        latitude = 20.5937
        longitude = 78.9629
        date = "2023-11-11"  # Adjust as needed
        
        print("Fetching data...")
        
        # Use ThreadPoolExecutor for parallel data fetching
        with ThreadPoolExecutor(max_workers=2) as executor:
            soil_future = executor.submit(analyzer.get_soilgrid_data, latitude, longitude)
            climate_future = executor.submit(analyzer.get_era5_data, latitude, longitude, date)
            
            soil_data = soil_future.result()
            climate_data = climate_future.result()
        
        if soil_data is not None and climate_data is not None:
            print("Analyzing combined data...")
            result = analyzer.analyze_combined_data(soil_data, climate_data)
            
            if result["success"]:
                print("\nAnalysis Results:")
                print(result["analysis"])
                
                # Save results
                output_file = f'soil_analysis_{date.replace("-", "_")}.json'
                with open(output_file, 'w') as f:
                    json.dump(result, f, indent=2)
                print(f"\nResults saved to {output_file}")
            else:
                print(f"\nAnalysis failed: {result['error']}")
        else:
            print("Unable to proceed with analysis due to missing data.")
            
    except Exception as e:
        print(f"Program execution failed: {e}")

if __name__ == "__main__":
    main()