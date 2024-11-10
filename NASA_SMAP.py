import requests
import json
from datetime import datetime, timedelta
import os
from typing import Dict, Any, Tuple, Optional
import numpy as np
from dataclasses import dataclass

@dataclass
class APIConfig:
    """Configuration for various APIs"""
    OPENWEATHER_API_KEY: str = "2d3d43c07a7fb4e2d3a6dd20ca5073d0"
    NASA_API_KEY: str = "xNAi4fC6dC6qi5OCTUiE2kn4JKFBlvETmAVEHYZo"

class FreeSoilAnalyzer:
    def __init__(self, config: APIConfig):
        self.config = config
        self.openweather_base_url = "http://api.openweathermap.org/data/2.5"
        self.nasa_power_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
        self.soilgrids_url = "https://rest.isric.org/soilgrids/v2.0/properties/query"

    def get_soil_data(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """Get comprehensive soil data using multiple free APIs."""
        try:
            # Collect data from multiple sources with validation
            weather_data = self._get_weather_data(latitude, longitude)
            nasa_data = self._get_nasa_power_data(latitude, longitude)
            soil_properties = self._get_soilgrids_data(latitude, longitude)
            
            # Process data with validation
            return self._process_soil_data(weather_data, nasa_data, soil_properties)
        except Exception as e:
            print(f"Warning: Error during data collection: {str(e)}")
            return self._generate_fallback_analysis()

    def _get_weather_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """Get weather data from OpenWeatherMap API with validation."""
        try:
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.config.OPENWEATHER_API_KEY,
                'units': 'metric'
            }
            response = requests.get(
                f"{self.openweather_base_url}/weather",
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Warning: Weather data retrieval failed: {str(e)}")
            return {}

    def _get_nasa_power_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """Get soil and climate data from NASA POWER API with validation."""
        try:
            params = {
                'start': (datetime.now() - timedelta(days=7)).strftime("%Y%m%d"),
                'end': datetime.now().strftime("%Y%m%d"),
                'latitude': lat,
                'longitude': lon,
                'community': 'AG',
                'parameters': 'PRECTOT,RH2M,T2M,ALLSKY_SFC_SW_DWN,ALLSKY_SFC_PAR_TOT',
                'format': 'JSON'
            }
            if self.config.NASA_API_KEY:
                params['api_key'] = self.config.NASA_API_KEY

            response = requests.get(self.nasa_power_url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Warning: NASA POWER data retrieval failed: {str(e)}")
            return {}

    def _get_soilgrids_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """Get soil properties from SoilGrids API with validation."""
        try:
            params = {'lat': lat, 'lon': lon}
            headers = {'Accept': 'application/json'}
            
            response = requests.get(
                self.soilgrids_url,
                params=params,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Warning: SoilGrids data retrieval failed: {str(e)}")
            return {}

    def _safe_get_nasa_value(self, parameters: Dict, key: str) -> float:
        """Safely extract and average NASA POWER parameter values."""
        try:
            values = parameters.get(key, {})
            if not values:
                return 0.0
            valid_values = [float(v) for v in values.values() if v is not None]
            return np.mean(valid_values) if valid_values else 0.0
        except Exception:
            return 0.0

    def _process_soil_data(
        self,
        weather_data: Dict[str, Any],
        nasa_data: Dict[str, Any],
        soil_properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process and combine data from all sources with validation."""
        # Extract NASA POWER data with validation
        nasa_properties = nasa_data.get('properties', {})
        parameters = nasa_properties.get('parameter', {})
        
        avg_temp = self._safe_get_nasa_value(parameters, 'T2M')
        avg_precip = self._safe_get_nasa_value(parameters, 'PRECTOT')
        avg_humidity = self._safe_get_nasa_value(parameters, 'RH2M')
        
        # Extract soil properties with validation
        soil_layers = soil_properties.get('properties', {}).get('layers', [])
        
        # Calculate properties with validation
        soil_type, soil_properties = self._analyze_soil_properties(soil_layers)
        moisture_content = self._estimate_moisture_content(
            avg_precip,
            avg_temp,
            avg_humidity,
            soil_properties
        )
        
        return {
            "moisture_content": moisture_content,
            "soil_type": soil_type,
            "organic_matter": soil_properties['organic_matter'],
            "ph_level": soil_properties['ph'],
            "nitrogen_content": soil_properties['nitrogen'],
            "additional_properties": {
                "temperature_celsius": round(avg_temp, 2) if avg_temp != 0 else None,
                "precipitation_mm": round(avg_precip, 2) if avg_precip != 0 else None,
                "humidity_percent": round(avg_humidity, 2) if avg_humidity != 0 else None,
                "clay_content_percent": soil_properties['clay'],
                "sand_content_percent": soil_properties['sand'],
                "silt_content_percent": soil_properties['silt']
            },
            "timestamp": datetime.now().isoformat(),
            "data_sources": [
                "OpenWeatherMap API",
                "NASA POWER API",
                "ISRIC SoilGrids API"
            ],
            "data_quality": {
                "weather_data_available": bool(weather_data),
                "nasa_data_available": bool(nasa_data),
                "soil_data_available": bool(soil_properties)
            }
        }

    def _estimate_moisture_content(
        self,
        precipitation: float,
        temperature: float,
        humidity: float,
        soil_props: Dict[str, float]
    ) -> float:
        """Estimate soil moisture content with validated inputs."""
        try:
            # Ensure inputs are within realistic ranges
            temp_factor = max(0, min(40, temperature)) / 40  # Normalize temperature
            precip_factor = max(0, min(100, precipitation)) / 100  # Normalize precipitation
            humidity_factor = max(0, min(100, humidity)) / 100  # Normalize humidity
            
            # Calculate base moisture capacity based on soil composition
            clay_factor = soil_props['clay'] / 100
            sand_factor = soil_props['sand'] / 100
            
            # Clay holds more water than sand
            base_capacity = (clay_factor * 0.6 + sand_factor * 0.2) * 100
            
            # Adjust for environmental factors
            moisture_content = base_capacity * (
                0.4 + 0.3 * precip_factor + 0.2 * humidity_factor - 0.1 * temp_factor
            )
            
            return round(max(0, min(100, moisture_content)), 2)
        except Exception:
            return 0.0

    def _analyze_soil_properties(
        self,
        soil_layers: list
    ) -> Tuple[str, Dict[str, float]]:
        """Analyze soil properties with validation."""
        try:
            # Get properties with validation
            clay = max(0, min(100, self._get_layer_property(soil_layers, 'clay')))
            sand = max(0, min(100, self._get_layer_property(soil_layers, 'sand')))
            silt = max(0, min(100, 100 - clay - sand))
            
            # Normalize percentages to sum to 100
            total = clay + sand + silt
            if total > 0:
                clay = (clay / total) * 100
                sand = (sand / total) * 100
                silt = (silt / total) * 100
            
            # Get other properties with validation
            organic_matter = max(0, min(100, self._get_layer_property(soil_layers, 'soc') * 0.058))
            ph = max(0, min(14, self._get_layer_property(soil_layers, 'phh2o')))
            if ph == 0:
                ph = 7.0  # Default to neutral if no data
            
            nitrogen = max(0, min(5, organic_matter * 0.05))
            
            return self._classify_soil_type(clay, sand, silt), {
                "clay": round(clay, 2),
                "sand": round(sand, 2),
                "silt": round(silt, 2),
                "organic_matter": round(organic_matter, 2),
                "ph": round(ph, 2),
                "nitrogen": round(nitrogen, 3)
            }
        except Exception:
            return "Unknown", self._get_default_properties()

    @staticmethod
    def _get_layer_property(layers: list, property_name: str, depth_idx: int = 0) -> float:
        """Safely extract property value from soil layers."""
        try:
            return float(layers[depth_idx][property_name]['value'])
        except (IndexError, KeyError, ValueError, TypeError):
            return 0.0

    @staticmethod
    def _classify_soil_type(clay: float, sand: float, silt: float) -> str:
        """Classify soil type with validation."""
        if not all(isinstance(x, (int, float)) for x in [clay, sand, silt]):
            return "Unknown"
        
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

    @staticmethod
    def _get_default_properties() -> Dict[str, float]:
        """Return default soil properties when data is unavailable."""
        return {
            "clay": 0.0,
            "sand": 0.0,
            "silt": 0.0,
            "organic_matter": 0.0,
            "ph": 7.0,  # Neutral pH as default
            "nitrogen": 0.0
        }

    def _generate_fallback_analysis(self) -> Dict[str, Any]:
        """Generate a fallback analysis when data collection fails."""
        return {
            "moisture_content": 0.0,
            "soil_type": "Unknown",
            "organic_matter": 0.0,
            "ph_level": 7.0,
            "nitrogen_content": 0.0,
            "additional_properties": {
                "temperature_celsius": None,
                "precipitation_mm": None,
                "humidity_percent": None,
                "clay_content_percent": 0.0,
                "sand_content_percent": 0.0,
                "silt_content_percent": 0.0
            },
            "timestamp": datetime.now().isoformat(),
            "data_sources": [],
            "data_quality": {
                "weather_data_available": False,
                "nasa_data_available": False,
                "soil_data_available": False
            },
            "status": "error",
            "message": "Unable to retrieve soil data. Please check your coordinates and try again."
        }

def main():
    config = APIConfig()
    
    # Validate API keys
    if not config.OPENWEATHER_API_KEY:
        print("Warning: OPENWEATHER_API_KEY not set. Some data may be unavailable.")
    if not config.NASA_API_KEY:
        print("Warning: NASA_API_KEY not set. Some data may be unavailable.")

    analyzer = FreeSoilAnalyzer(config)
    
    try:
        latitude = float(input("Enter latitude: "))
        longitude = float(input("Enter longitude: "))
        
        # Validate coordinates
        if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
            raise ValueError("Invalid coordinates. Latitude must be between -90 and 90, longitude between -180 and 180.")
        
        soil_data = analyzer.get_soil_data(latitude, longitude)
        print("\nSoil Analysis Results:")
        print(json.dumps(soil_data, indent=2))
    except ValueError as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"Error: An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()