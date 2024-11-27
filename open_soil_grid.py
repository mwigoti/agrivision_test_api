import cdsapi
import xarray as xr
import requests
import pandas as pd
import json
import os
import sys
import time
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any


class SoilClimateAnalyzer:
    """Class to integrate and analyze soil and climate data."""

    def __init__(self, huggingface_token: Optional[str] = None):
        self.huggingface_token = huggingface_token or os.getenv("HUGGINGFACE_TOKEN")
        if not self.huggingface_token:
            raise ValueError("Hugging Face token is required. Set the HUGGINGFACE_TOKEN environment variable.")

        self.model_url = "https://api-inference.huggingface.co/models/tiiuae/falcon-7b-instruct"
        self.headers = {
            "Authorization": f"Bearer {self.huggingface_token}",
            "Content-Type": "application/json"
        }

        self.cds_client = cdsapi.Client()
        self.max_retries = 3
        self.retry_delay = 5
        self.last_request_time = None
        self.min_request_interval = 2

    def fetch_era5_data(self, latitude: float, longitude: float, date: str) -> Dict[str, Any]:
        """Fetch ERA5 climate data."""
        temp_file = f"temp_era5_data_{int(time.time())}.nc"
        try:
            year, month, day = date.split('-')
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
            ds = xr.open_dataset(temp_file, engine="netcdf4")

            variable_mapping = {
                'stl1': 'soil_temperature_level_1',
                'swvl1': 'volumetric_soil_water_layer_1',
                'e': 'total_evaporation'
            }

            data = {
                "metadata": {"timestamp": f"{date}T00:00:00", "area": area, "source": "ERA5-Land"},
                "data": {
                    var: {
                        "mean": float(np.nanmean(ds[var].values)),
                        "min": float(np.nanmin(ds[var].values)),
                        "max": float(np.nanmax(ds[var].values)),
                        "unit": str(ds[var].units) if hasattr(ds[var], 'units') else "unknown"
                    }
                    for var, readable_name in variable_mapping.items() if var in ds
                }
            }

            return data
        except Exception as e:
            raise RuntimeError(f"Error fetching ERA5 data: {e}")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def fetch_soilgrid_data(self, latitude: float, longitude: float) -> Optional[pd.DataFrame]:
        """Fetch soil data from SoilGrids."""
        try:
            url = f"https://rest.isric.org/soilgrids/v2.0/properties/query?lat={latitude}&lon={longitude}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            properties = response.json()['properties']['layers']
            soil_data = {prop['name']: prop['depths'][0]['values']['mean'] for prop in properties}

            return pd.DataFrame([soil_data])
        except Exception as e:
            raise RuntimeError(f"Error fetching SoilGrids data: {e}")

    def analyze_data(self, soil_data: pd.DataFrame, climate_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze soil and climate data using the Falcon-7B model."""
        combined_data = {
            "soil_properties": soil_data.to_dict(orient="records")[0],
            "climate_conditions": climate_data["data"]
        }

        prompt = f"""
        Analyze the following soil and climate data for agricultural potential and environmental conditions:

        Soil Properties:
        {json.dumps(combined_data['soil_properties'], indent=2)}

        Climate Conditions:
        {json.dumps(combined_data['climate_conditions'], indent=2)}

        Provide:
        - Soil fertility assessment
        - Water retention capabilities
        - Crop suitability
        - Climate stress factors
        - Land management recommendations
        """

        for attempt in range(self.max_retries):
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
                        "parameters": {"max_new_tokens": 1000, "temperature": 0.7, "top_p": 0.95}
                    },
                    timeout=45
                )
                response.raise_for_status()
                result = response.json()

                analysis = result[0].get('generated_text', '') if isinstance(result, list) else result.get('generated_text', '')
                if analysis.strip():
                    return {"success": True, "analysis": analysis}

            except Exception as e:
                if attempt == self.max_retries - 1:
                    return {"success": False, "error": str(e)}

    @staticmethod
    def save_analysis(result: Dict[str, Any], output_file: str):
        """Save analysis results to a JSON file."""
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"Results saved to {output_file}")


def main():
    try:
        token = os.getenv("HUGGINGFACE_TOKEN")
        if not token:
            raise EnvironmentError("HUGGINGFACE_TOKEN not set.")

        analyzer = SoilClimateAnalyzer(token)

        latitude, longitude, date = 20.5937, 78.9629, "2023-11-11"

        with ThreadPoolExecutor() as executor:
            soil_future = executor.submit(analyzer.fetch_soilgrid_data, latitude, longitude)
            climate_future = executor.submit(analyzer.fetch_era5_data, latitude, longitude, date)

            soil_data = soil_future.result()
            climate_data = climate_future.result()

        result = analyzer.analyze_data(soil_data, climate_data)

        if result.get("success"):
            output_file = f"soil_analysis_{date.replace('-', '_')}.json"
            analyzer.save_analysis(result, output_file)
        else:
            print(f"Analysis failed: {result.get('error')}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
