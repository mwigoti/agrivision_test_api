import cdsapi
import xarray as xr
import requests
import pandas as pd
import json
import os
import sys
from datetime import datetime
import time
import numpy as np
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

class IntegratedSoilAnalyzer:
    def __init__(self, huggingface_token: Optional[str] = None):
        """Initialize with required configurations"""
        self.huggingface_token = huggingface_token or os.getenv("HUGGINGFACE_TOKEN")
        if not self.huggingface_token:
            raise ValueError("Hugging Face token is required. Set HUGGINGFACE_TOKEN environment variable.")
            
        # Falcon-7B configuration
        self.model_url = "https://api-inference.huggingface.co/models/tiiuae/falcon-7b-instruct"
        self.headers = {
            "Authorization": f"Bearer {self.huggingface_token}",
            "Content-Type": "application/json"
        }
        
        # CDS client initialization
        self.cds_client = cdsapi.Client()
        
        # Request configuration
        self.max_retries = 3
        self.retry_delay = 5
        self.last_request_time = None
        self.min_request_interval = 2

    def get_era5_data(self, latitude: float, longitude: float, date: str) -> Dict[str, Any]:
        """Fetch ERA5 climate data"""
        temp_file = f'temp_era5_data_{int(time.time())}.nc'
        try:
            year, month, day = date.split('-')
            
            # Calculate area bounds (0.5 degree buffer)
            area = [
                min(90, latitude + 0.5), 
                max(-180, longitude - 0.5),
                max(-90, latitude - 0.5), 
                min(180, longitude + 0.5)
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
            
            self.cds_client.retrieve("reanalysis-era5-land", request, temp_file)
            
            # Open dataset with error handling
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
            
        except Exception as e:
            print(f"Error fetching ERA5 data: {e}")
            raise
        finally:
            # Ensure file is closed and deleted
            if 'ds' in locals():
                ds.close()
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def get_soilgrid_data(self, latitude: float, longitude: float) -> Optional[pd.DataFrame]:
        """Fetch soil data from SoilGrids"""
        base_url = "https://rest.isric.org"
        endpoint = f"soilgrids/v2.0/properties/query?lat={latitude}&lon={longitude}"
        
        try:
            response = requests.get(f"{base_url}/{endpoint}", timeout=30)
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
        """Analyze combined soil and climate data using Falcon-7B with enhanced error handling"""
        combined_data = {
            "soil_properties": soil_data.to_dict(orient='records')[0],
            "climate_conditions": climate_data['data']
        }
        
        # Consistent prompt (previously omitted for brevity)
        prompt = f"""Analyze the following soil and climate data to provide insights about agricultural potential and environmental conditions:

Soil Properties:
{json.dumps(combined_data['soil_properties'], indent=2)}

Climate Conditions:
{json.dumps(combined_data['climate_conditions'], indent=2)}

Provide a comprehensive analysis including:
1. Soil fertility assessment
2. Water retention capabilities
3. Potential crop suitability
4. Climate stress factors
5. Recommendations for land management"""

        retries = 0
        while retries < self.max_retries:
            try:
                # Rate limiting
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
                            "max_new_tokens": 1000,
                            "temperature": 0.7,
                            "top_p": 0.95,
                            "return_full_text": False
                        }
                    },
                    timeout=45
                )
                
                response.raise_for_status()
                result = response.json()
                
                # Extract generated text
                if isinstance(result, list) and result:
                    analysis = result[0].get('generated_text', '')
                elif isinstance(result, dict):
                    analysis = result.get('generated_text', '')
                else:
                    analysis = str(result)
                
                if not analysis.strip():
                    raise ValueError("Empty analysis received from model")
                
                return {
                    "success": True,
                    "model": "falcon-7b-instruct",
                    "analysis": analysis,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": {
                        "retries": retries,
                        "response_length": len(analysis)
                    }
                }
                
            except Exception as e:
                print(f"Analysis attempt {retries + 1} failed: {e}")
                retries += 1
                
                if retries < self.max_retries:
                    time.sleep(self.retry_delay * (2 ** retries))
        
        return {
            "success": False,
            "error": "Max retries exceeded",
            "model": "falcon-7b-instruct",
            "timestamp": datetime.now().isoformat(),
            "metadata": {
                "retries": self.max_retries,
                "last_error": str(e) if 'e' in locals() else "Unknown error"
            }
        }

def main():
    """Main execution function with robust error handling"""
    try:
        # Validate Hugging Face token
        huggingface_token = os.getenv("HUGGINGFACE_TOKEN")
        if not huggingface_token:
            print("Error: HUGGINGFACE_TOKEN environment variable not set.")
            sys.exit(1)
        
        # Initialize analyzer
        analyzer = IntegratedSoilAnalyzer(huggingface_token)
        
        # Coordinates for data retrieval (India's approximate center)
        latitude = 20.5937
        longitude = 78.9629
        date = "2023-11-11"
        
        print("\nInitiating data retrieval...")
        
        # Concurrent data fetching
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit tasks
            soil_future = executor.submit(analyzer.get_soilgrid_data, latitude, longitude)
            climate_future = executor.submit(analyzer.get_era5_data, latitude, longitude, date)
            
            # Wait for results
            soil_data = soil_future.result()
            climate_data = climate_future.result()
        
        # Validate retrieved data
        if soil_data is None or climate_data is None:
            print("Error: Failed to retrieve complete data.")
            sys.exit(1)
        
        # Perform analysis
        print("\nAnalyzing combined data...")
        result = analyzer.analyze_combined_data(soil_data, climate_data)
        
        # Process and save results
        if result["success"]:
            print("\nAnalysis Successful:")
            print(result["analysis"])
            
            # Generate output filename
            output_file = f'soil_analysis_{date.replace("-", "_")}.json'
            
            # Save results
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"\nResults saved to {output_file}")
        else:
            print(f"\nAnalysis failed: {result.get('error', 'Unknown error')}")
            print(f"Metadata: {result.get('metadata', {})}")
    
    except Exception as e:
        print(f"\nCritical error during execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()