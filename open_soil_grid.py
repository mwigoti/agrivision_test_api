import requests
import pandas as pd
import json
import os
from datetime import datetime
import time
from typing import Optional, Dict, Any

class SoilAnalyzer:
    """Class for analyzing soil data using Hugging Face's Falcon-7B model"""
    
    def __init__(self, huggingface_token: str = None):
        """
        Initialize SoilAnalyzer with Falcon-7B configuration.
        
        Args:
            huggingface_token (str): Authentication token for Hugging Face API
        """
        self.huggingface_token = huggingface_token or os.getenv("HUGGINGFACE_TOKEN")
        if not self.huggingface_token:
            raise ValueError("Hugging Face token is required. Set it either through constructor or HUGGINGFACE_TOKEN environment variable.")
        
        # Falcon-7B model configuration
        self.model_url = "https://api-inference.huggingface.co/models/tiiuae/falcon-7b-instruct"
        self.headers = {"Authorization": f"Bearer {self.huggingface_token}"}
        
        # Request configuration
        self.max_retries = 3
        self.retry_delay = 5
        self.last_request_time = None
        self.min_request_interval = 2
        
    def _wait_for_rate_limit(self):
        """Implement rate limiting"""
        if self.last_request_time:
            elapsed = (datetime.now() - self.last_request_time).total_seconds()
            if elapsed < self.min_request_interval:
                time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = datetime.now()

    def analyze_soil(self, soil_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze soil data using Falcon-7B model.
        
        Args:
            soil_data (pd.DataFrame): Soil data to analyze
            
        Returns:
            Dict[str, Any]: Analysis results and metadata
        """
        prompt = self._create_analysis_prompt(soil_data)
        retries = 0
        current_delay = self.retry_delay
        last_error = None

        while retries < self.max_retries:
            try:
                self._wait_for_rate_limit()
                
                response = requests.post(
                    self.model_url,
                    headers=self.headers,
                    json={
                        "inputs": prompt,
                        "parameters": {
                            "max_new_tokens": 500,
                            "temperature": 0.7,
                            "top_p": 0.95,
                            "return_full_text": False
                        }
                    },
                    timeout=30
                )
                response.raise_for_status()
                
                result = response.json()
                if isinstance(result, list):
                    analysis = result[0]['generated_text']
                else:
                    analysis = result.get('generated_text', str(result))
                
                return {
                    "success": True,
                    "model": "falcon-7b-instruct",
                    "analysis": analysis,
                    "timestamp": datetime.now().isoformat()
                }
                
            except requests.exceptions.HTTPError as e:
                print(f"HTTP Error: {e}")
                retries += 1
                last_error = e
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed (attempt {retries + 1}/{self.max_retries}): {e}")
                retries += 1
                last_error = e
                
            if retries < self.max_retries:
                print(f"Retrying in {current_delay} seconds...")
                time.sleep(current_delay)
                current_delay *= 2
        
        return {
            "success": False,
            "error": str(last_error),
            "model": "falcon-7b-instruct",
            "timestamp": datetime.now().isoformat()
        }

    def _create_analysis_prompt(self, soil_data: pd.DataFrame) -> str:
        """Create analysis prompt optimized for Falcon-7B"""
        soil_data_str = json.dumps(soil_data.to_dict(orient='records')[0], indent=2)
        
        return f"""Instruction: Analyze the following soil data and provide:
1. Soil quality assessment
2. Crop recommendations
3. Required amendments
4. Potential issues
5. Management recommendations

Soil Data:
{soil_data_str}

Response: Let me analyze this soil data and provide detailed recommendations."""

def get_soilgrid_data(latitude: float, longitude: float) -> Optional[pd.DataFrame]:
    """Fetch soil data from SoilGrids v2.0 REST API"""
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
        
    except requests.exceptions.RequestException as e:
        print(f"API Request Error: {e}")
        return None
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Data Processing Error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected Error: {e}")
        return None

def main():
    """Main function to demonstrate usage"""
    try:
        # Initialize analyzer with Falcon-7B
        analyzer = SoilAnalyzer()
        
        # Example coordinates (India)
        latitude = 20.5937
        longitude = 78.9629
        
        # Get soil data
        print("Fetching soil data...")
        soil_data = get_soilgrid_data(latitude, longitude)
        
        if soil_data is not None:
            # Analyze soil data with Falcon-7B
            print("Analyzing soil data using Falcon-7B...")
            result = analyzer.analyze_soil(soil_data)
            
            if result["success"]:
                print(f"\nAnalysis Results (using {result['model']}):")
                print(result["analysis"])
            else:
                print(f"\nAnalysis failed: {result['error']}")
        else:
            print("Unable to proceed with analysis due to missing soil data.")
            
    except Exception as e:
        print(f"Program execution failed: {e}")

if __name__ == "__main__":
    main()