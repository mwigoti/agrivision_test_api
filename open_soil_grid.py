import requests
import pandas as pd
import json
import os
from datetime import datetime
import time
from typing import Optional, Dict, Any

def get_soilgrid_data(latitude: float, longitude: float) -> Optional[pd.DataFrame]:
    """
    Fetch soil data from SoilGrids v2.0 REST API.
    
    Args:
        latitude (float): Latitude coordinate
        longitude (float): Longitude coordinate
        
    Returns:
        Optional[pd.DataFrame]: DataFrame containing soil properties or None if error occurs
    """
    base_url = "https://rest.isric.org"
    endpoint = f"soilgrids/v2.0/properties/query?lat={latitude}&lon={longitude}"
    
    try:
        response = requests.get(f"{base_url}/{endpoint}")
        response.raise_for_status()  # Raise exception for bad status codes
        
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

class SoilAnalyzer:
    """Class for analyzing soil data using Hugging Face's API"""
    
    def __init__(self, huggingface_token: str = None):
        """
        Initialize SoilAnalyzer with configuration parameters.
        
        Args:
            huggingface_token (str): Authentication token for Hugging Face API
        """
        self.huggingface_token = huggingface_token or os.getenv("HUGGINGFACE_TOKEN")
        if not self.huggingface_token:
            raise ValueError("Hugging Face token is required. Set it either through the constructor or HUGGINGFACE_TOKEN environment variable.")
            
        self.huggingface_model = "mistralai/Mistral-7B-Instruct-v0.1"
        self.max_retries = 5
        self.retry_delay = 5  # Initial delay in seconds
        
    def analyze_with_huggingface(self, soil_data: pd.DataFrame) -> str:
        """
        Analyze soil data using Hugging Face's API with retry mechanism.
        
        Args:
            soil_data (pd.DataFrame): Soil data to analyze
            
        Returns:
            str: Analysis result or error message
        """
        api_url = f"https://api-inference.huggingface.co/models/{self.huggingface_model}"
        headers = {"Authorization": f"Bearer {self.huggingface_token}"}
        prompt = self._create_analysis_prompt(soil_data)
        
        retries = 0
        current_delay = self.retry_delay
        
        while retries < self.max_retries:
            try:
                response = requests.post(api_url, headers=headers, json={"inputs": prompt})
                response.raise_for_status()
                
                return response.json()[0]['generated_text']
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed (attempt {retries + 1}/{self.max_retries}): {e}")
                retries += 1
                
                if retries < self.max_retries:
                    print(f"Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= 2  # Exponential backoff
            
        return "Maximum number of retries reached. Unable to analyze soil data."
    
    def _create_analysis_prompt(self, soil_data: pd.DataFrame) -> str:
        """
        Create a structured prompt for the Hugging Face API.
        
        Args:
            soil_data (pd.DataFrame): Soil data to include in prompt
            
        Returns:
            str: Formatted prompt string
        """
        return f"""
        Analyze this soil data and provide:
        1. Soil quality assessment
        2. Crop recommendations
        3. Required amendments
        4. Potential issues
        5. Management recommendations
        
        Soil Data:
        {json.dumps(soil_data.to_dict(orient='records')[0], indent=2)}
        
        Please provide a detailed analysis based on these parameters.
        """

def main():
    """Main function to demonstrate usage"""
    try:
        # Initialize analyzer with token from environment variable
        analyzer = SoilAnalyzer()
        
        # Example coordinates (India)
        latitude = 20.5937
        longitude = 78.9629
        
        # Get soil data
        print("Fetching soil data...")
        soil_data = get_soilgrid_data(latitude, longitude)
        
        if soil_data is not None:
            # Analyze with Hugging Face
            print("Analyzing with Hugging Face...")
            hf_analysis = analyzer.analyze_with_huggingface(soil_data)
            print("\nAnalysis Results:")
            print(hf_analysis)
        else:
            print("Unable to proceed with analysis due to missing soil data.")
            
    except Exception as e:
        print(f"Program execution failed: {e}")

if __name__ == "__main__":
    main()