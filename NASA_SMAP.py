import requests
import json
from datetime import datetime, timedelta
import os
from typing import Dict, Any, Tuple
import numpy as np
from dataclasses import dataclass

@dataclass
class APIConfig:
    """Configuration for various APIs"""
    OPENWEATHER_API_KEY: str = os.getenv('OPENWEATHER_API_KEY', '')
    NASA_API_KEY: str = os.getenv('NASA_API_KEY', '')  # Get from https://api.nasa.gov/
    
class FreeSoilAnalyzer:
    def __init__(self, config: APIConfig):
        """
        Initialize the FreeSoilAnalyzer with API keys.
        
        Args:
            config (APIConfig): Configuration containing API keys
        """
        self.config = config
        self.openweather_base_url = "http://api.openweathermap.org/data/2.5"
        self.nasa_power_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
        self.soilgrids_url = "https://rest.isric.org/soilgrids/v2.0/properties/query"

    def get_soil_data(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """
        Get comprehensive soil data using multiple free APIs.
        
        Args:
            latitude (float): Latitude of the location
            longitude (float): Longitude of the location
            
        Returns:
            Dict containing soil parameters
        """
        try:
            # Collect data from multiple sources
            weather_data = self._get_weather_data(latitude, longitude)
            nasa_data = self._get_nasa_power_data(latitude, longitude)
            soil_properties = self._get_soilgrids_data(latitude, longitude)
            
            # Combine and process all data
            soil_analysis = self._process_soil_data(
                weather_data,
                nasa_data,
                soil_properties
            )
            
            return soil_analysis
            
        except Exception as e:
            raise Exception(f"Error analyzing soil data: {str(e)}")

    def _get_weather_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Get weather data from OpenWeatherMap API.
        """
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.config.OPENWEATHER_API_KEY,
            'units': 'metric'
        }
        
        response = requests.get(
            f"{self.openweather_base_url}/weather",
            params=params
        )
        response.raise_for_status()
        return response.json()

    def _get_nasa_power_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Get soil and climate data from NASA POWER API.
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        params = {
            'start': start_date.strftime("%Y%m%d"),
            'end': end_date.strftime("%Y%m%d"),
            'latitude': lat,
            'longitude': lon,
            'community': 'AG',
            'parameters': 'PRECTOT,RH2M,T2M,ALLSKY_SFC_SW_DWN,ALLSKY_SFC_PAR_TOT',
            'format': 'JSON',
            'api_key': self.config.NASA_API_KEY
        }
        
        response = requests.get(self.nasa_power_url, params=params)
        response.raise_for_status()
        return response.json()

    def _get_soilgrids_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Get soil properties from SoilGrids API.
        """
        params = {
            'lat': lat,
            'lon': lon
        }
        
        headers = {
            'Accept': 'application/json'
        }
        
        response = requests.get(
            self.soilgrids_url,
            params=params,
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    def _process_soil_data(
        self,
        weather_data: Dict[str, Any],
        nasa_data: Dict[str, Any],
        soil_properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process and combine data from all sources to create comprehensive soil analysis.
        """
        # Extract relevant data from NASA POWER
        nasa_properties = nasa_data.get('properties', {})
        parameters = nasa_properties.get('parameter', {})
        
        # Calculate average values from NASA data
        avg_temp = np.mean([float(v) for v in parameters.get('T2M', {}).values()])
        avg_precip = np.mean([float(v) for v in parameters.get('PRECTOT', {}).values()])
        avg_humidity = np.mean([float(v) for v in parameters.get('RH2M', {}).values()])
        
        # Extract soil properties from SoilGrids
        soil_layers = soil_properties.get('properties', {}).get('layers', [])
        
        # Calculate soil moisture content
        moisture_content = self._estimate_moisture_content(
            avg_precip,
            avg_temp,
            avg_humidity,
            soil_layers
        )
        
        # Determine soil type and properties
        soil_type, soil_properties = self._analyze_soil_properties(soil_layers)
        
        # Create comprehensive analysis
        analysis = {
            "moisture_content": moisture_content,
            "soil_type": soil_type,
            "organic_matter": soil_properties['organic_matter'],
            "ph_level": soil_properties['ph'],
            "nitrogen_content": soil_properties['nitrogen'],
            "additional_properties": {
                "temperature_celsius": avg_temp,
                "precipitation_mm": avg_precip,
                "humidity_percent": avg_humidity,
                "clay_content_percent": soil_properties['clay'],
                "sand_content_percent": soil_properties['sand'],
                "silt_content_percent": soil_properties['silt']
            },
            "timestamp": datetime.now().isoformat(),
            "data_sources": [
                "OpenWeatherMap API",
                "NASA POWER API",
                "ISRIC SoilGrids API"
            ]
        }
        
        return analysis

    def _estimate_moisture_content(
        self,
        precipitation: float,
        temperature: float,
        humidity: float,
        soil_layers: list
    ) -> float:
        """
        Estimate soil moisture content using weather and soil data.
        """
        # Get soil texture from first layer
        clay_content = self._get_layer_property(soil_layers, 'clay')
        sand_content = self._get_layer_property(soil_layers, 'sand')
        
        # Calculate field capacity based on soil texture
        field_capacity = (0.3 * clay_content + 0.2 * (100 - sand_content - clay_content)) / 100
        
        # Adjust for weather conditions
        moisture_factor = (precipitation * 0.4 + humidity * 0.4 - temperature * 0.2) / 100
        estimated_moisture = field_capacity * (1 + moisture_factor)
        
        return round(max(0, min(100, estimated_moisture * 100)), 2)

    def _analyze_soil_properties(
        self,
        soil_layers: list
    ) -> Tuple[str, Dict[str, float]]:
        """
        Analyze soil properties from SoilGrids data.
        """
        # Get average properties from top layers
        clay = self._get_layer_property(soil_layers, 'clay')
        sand = self._get_layer_property(soil_layers, 'sand')
        silt = 100 - clay - sand
        
        # Determine soil type based on composition
        soil_type = self._classify_soil_type(clay, sand, silt)
        
        # Estimate other properties
        organic_matter = self._get_layer_property(soil_layers, 'soc') * 0.058  # Convert to percentage
        ph = self._get_layer_property(soil_layers, 'phh2o')
        nitrogen = organic_matter * 0.05  # Rough estimation based on organic matter
        
        properties = {
            "clay": round(clay, 2),
            "sand": round(sand, 2),
            "silt": round(silt, 2),
            "organic_matter": round(organic_matter, 2),
            "ph": round(ph, 2),
            "nitrogen": round(nitrogen, 3)
        }
        
        return soil_type, properties

    @staticmethod
    def _get_layer_property(layers: list, property_name: str, depth_idx: int = 0) -> float:
        """
        Extract property value from soil layers.
        """
        try:
            return float(layers[depth_idx][property_name]['value'])
        except (IndexError, KeyError, ValueError):
            return 0.0

    @staticmethod
    def _classify_soil_type(clay: float, sand: float, silt: float) -> str:
        """
        Classify soil type based on texture triangle.
        """
        if sand >= 85:
            return "Sandy"
        elif clay >= 40:
            return "Clay"
        elif silt >= 80:
            return "Silty"
        elif sand >= 70:
            return "Sandy Loam"
        elif clay >= 27 and silt >= 28 and sand <= 45:
            return "Clay Loam"
        else:
            return "Loam"

def main():
    # Example usage
    config = APIConfig()
    
    # Check for required API keys
    if not config.OPENWEATHER_API_KEY:
        raise ValueError("Please set OPENWEATHER_API_KEY environment variable")
    if not config.NASA_API_KEY:
        raise ValueError("Please set NASA_API_KEY environment variable")

    analyzer = FreeSoilAnalyzer(config)
    
    # Get coordinates from user
    latitude = float(input("Enter latitude: "))
    longitude = float(input("Enter longitude: "))
    
    try:
        soil_data = analyzer.get_soil_data(latitude, longitude)
        print("\nSoil Analysis Results:")
        print(json.dumps(soil_data, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()