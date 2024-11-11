import requests
import json
import time
import logging
from datetime import datetime, timedelta
import os
from typing import Dict, Any, Tuple, Optional, List, Union
import numpy as np
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataQualityLevel(Enum):
    """Enumeration for data quality levels"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INSUFFICIENT = "insufficient"

@dataclass
class APIConfig:
    """Configuration for API access and request handling"""
    OPENWEATHER_API_KEY: str = "#"
    NASA_API_KEY: str = #
    OPENWEATHER_URL: str = "http://api.openweathermap.org/data/2.5/weather"
    NASA_POWER_URL: str = "https://power.larc.nasa.gov/api/temporal/daily/point"
    SOILGRIDS_URL: str = "https://rest.isric.org/soilgrids/v2.0/properties/query"
    DEFAULT_TIMEOUT: int = 30
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 2

@dataclass
class SoilParameters:
    """Range configurations for various soil and environmental parameters"""
    TEMPERATURE_RANGE: Tuple[float, float] = (-50.0, 60.0)
    HUMIDITY_RANGE: Tuple[float, float] = (0.0, 100.0)
    PRECIPITATION_RANGE: Tuple[float, float] = (0.0, 500.0)
    CLAY_RANGE: Tuple[float, float] = (0.0, 100.0)
    SAND_RANGE: Tuple[float, float] = (0.0, 100.0)
    SILT_RANGE: Tuple[float, float] = (0.0, 100.0)
    PH_RANGE: Tuple[float, float] = (3.0, 10.0)
    ORGANIC_MATTER_RANGE: Tuple[float, float] = (0.0, 30.0)
    NITROGEN_RANGE: Tuple[float, float] = (0.0, 5.0)
    MOISTURE_RANGE: Tuple[float, float] = (0.0, 100.0)

class DataValidator:
    """Validates soil and environmental data against defined ranges"""
    
    def __init__(self, parameters: SoilParameters):
        self.params = parameters

    def validate_range(self, value: Union[float, None], range_tuple: Tuple[float, float], parameter_name: str) -> Tuple[float, bool]:
        """Validate a value against an acceptable range"""
        if value is None or np.isnan(value):
            logger.warning(f"Invalid {parameter_name} value: None or NaN")
            return range_tuple[0], False
        
        try:
            value = float(value)
            if range_tuple[0] <= value <= range_tuple[1]:
                return value, True
            else:
                logger.warning(f"{parameter_name} value {value} outside valid range {range_tuple[0]}-{range_tuple[1]}")
                return max(range_tuple[0], min(range_tuple[1], value)), False
        except (ValueError, TypeError):
            logger.warning(f"Could not convert {parameter_name} value to float: {value}")
            return range_tuple[0], False

    def validate_soil_composition(self, clay: float, sand: float, silt: float) -> Tuple[Dict[str, float], bool]:
        """Validate and normalize soil composition percentages"""
        clay_val, clay_valid = self.validate_range(clay, self.params.CLAY_RANGE, "clay")
        sand_val, sand_valid = self.validate_range(sand, self.params.SAND_RANGE, "sand")
        silt_val, silt_valid = self.validate_range(silt, self.params.SILT_RANGE, "silt")
        
        total = clay_val + sand_val + silt_val
        if total == 0:
            logger.warning("All soil composition values are zero")
            return {"clay": 0.0, "sand": 0.0, "silt": 0.0}, False
            
        normalized = {
            "clay": round((clay_val / total) * 100, 2),
            "sand": round((sand_val / total) * 100, 2),
            "silt": round((silt_val / total) * 100, 2)
        }
        
        is_valid = all([clay_valid, sand_valid, silt_valid])
        return normalized, is_valid

class APIHandler:
    """Handles API requests with retry logic and error handling"""
    
    def __init__(self, config: APIConfig):
        self.config = config
        self.session = requests.Session()
    
    def make_request(self, url: str, params: Dict[str, Any], headers: Optional[Dict[str, str]] = None, timeout: Optional[int] = None) -> Tuple[Dict[str, Any], bool]:
        """Make HTTP request with retry logic and error handling"""
        timeout = timeout or self.config.DEFAULT_TIMEOUT
        
        for attempt in range(self.config.MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, headers=headers, timeout=timeout)
                response.raise_for_status()
                return response.json(), True
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.config.MAX_RETRIES}): {str(e)}")
                if attempt < self.config.MAX_RETRIES - 1:
                    time.sleep(self.config.RETRY_DELAY * (attempt + 1))
                    continue
                return {}, False
        
        return {}, False

class SoilAnalyzer:
    """Analyzes soil and environmental conditions based on input data"""
    
    def __init__(self):
        self.config = APIConfig()
        self.params = SoilParameters()
        self.validator = DataValidator(self.params)
        self.api_handler = APIHandler(self.config)
        
    def analyze_location(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """Performs comprehensive soil and environmental analysis for a given location"""
        if not self._validate_coordinates(latitude, longitude):
            return self._generate_error_response("Invalid coordinates. Latitude must be between -90 and 90, longitude between -180 and 180.")
        
        try:
            weather_data = self._get_weather_data(latitude, longitude)
            nasa_data = self._get_nasa_data(latitude, longitude)
            soil_data = self._get_soil_data(latitude, longitude)
            
            analysis = self._process_data(weather_data, nasa_data, soil_data)
            analysis["timestamp"] = datetime.now().isoformat()
            analysis["coordinates"] = {"latitude": latitude, "longitude": longitude}
            
            return analysis
        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}")
            return self._generate_error_response(str(e))

    def _get_weather_data(self, lat: float, lon: float) -> Tuple[Dict[str, Any], bool]:
        """Fetch weather data from OpenWeatherMap API"""
        params = {'lat': lat, 'lon': lon, 'appid': self.config.OPENWEATHER_API_KEY, 'units': 'metric'}
        return self.api_handler.make_request(self.config.OPENWEATHER_URL, params)

    def _get_nasa_data(self, lat: float, lon: float) -> Tuple[Dict[str, Any], bool]:
        """Fetch data from NASA POWER API"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        params = {
            'start': start_date.strftime("%Y%m%d"),
            'end': end_date.strftime("%Y%m%d"),
            'latitude': lat,
            'longitude': lon,
            'community': 'AG',
            'parameters': 'PRECTOT,RH2M,T2M,ALLSKY_SFC_SW_DWN',
            'format': 'JSON'
        }
        
        if self.config.NASA_API_KEY:
            params['api_key'] = self.config.NASA_API_KEY
            
        return self.api_handler.make_request(self.config.NASA_POWER_URL, params, timeout=60)

    def _get_soil_data(self, lat: float, lon: float) -> Tuple[Dict[str, Any], bool]:
        """Fetch soil data from SoilGrids API"""
        params = {'lat': lat, 'lon': lon}
        headers = {'Accept': 'application/json'}
        return self.api_handler.make_request(self.config.SOILGRIDS_URL, params, headers)

    def _process_data(self, weather_data: Tuple[Dict[str, Any], bool], nasa_data: Tuple[Dict[str, Any], bool], soil_data: Tuple[Dict[str, Any], bool]) -> Dict[str, Any]:
        """Combine and process weather, NASA, and soil data"""
        weather_info, weather_success = weather_data
        nasa_info, nasa_success = nasa_data
        soil_info, soil_success = soil_data
        
        env_data = self._process_environmental_data(weather_info, nasa_info)
        soil_properties = self._process_soil_properties(soil_info)
        
        quality_level = self._assess_data_quality(weather_success, nasa_success, soil_success, env_data, soil_properties)
        
        return {
            "soil_properties": soil_properties,
            "environmental_conditions": env_data,
            "soil_type": self._classify_soil_type(soil_properties["composition"]),
            "data_quality": {
                "level": quality_level.value,
                "sources_available": {
                    "weather_data": weather_success,
                    "nasa_data": nasa_success,
                    "soil_data": soil_success
                }
            }
        }

    def _process_environmental_data(self, weather_data: Dict[str, Any], nasa_data: Dict[str, Any]) -> Dict[str, float]:
        """Process and validate environmental data from weather and NASA"""
        temp = weather_data.get('main', {}).get('temp', nasa_data.get('properties', {}).get('parameter', {}).get('T2M', {}).get('value'))
        temp_val, _ = self.validator.validate_range(temp, self.params.TEMPERATURE_RANGE, "temperature")
        
        humidity = weather_data.get('main', {}).get('humidity', nasa_data.get('properties', {}).get('parameter', {}).get('RH2M', {}).get('value'))
        humidity_val, _ = self.validator.validate_range(humidity, self.params.HUMIDITY_RANGE, "humidity")
        
        precip = nasa_data.get('properties', {}).get('parameter', {}).get('PRECTOT', {}).get('value', 0)
        precip_val, _ = self.validator.validate_range(precip, self.params.PRECIPITATION_RANGE, "precipitation")
        
        return {
            "temperature_celsius": round(temp_val, 2),
            "humidity_percent": round(humidity_val, 2),
            "precipitation_mm": round(precip_val, 2)
        }

    def _process_soil_properties(self, soil_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and validate soil properties"""
        layers = soil_data.get('properties', {}).get('layers', [])
        
        clay = self._extract_soil_property(layers, 'clay')
        sand = self._extract_soil_property(layers, 'sand')
        silt = self._extract_soil_property(layers, 'silt')
        
        composition, _ = self.validator.validate_soil_composition(clay, sand, silt)
        
        ph = self._extract_soil_property(layers, 'phh2o')
        ph_val, _ = self.validator.validate_range(ph, self.params.PH_RANGE, "pH")
        
        organic = self._extract_soil_property(layers, 'soc')
        organic_val, _ = self.validator.validate_range(organic, self.params.ORGANIC_MATTER_RANGE, "organic matter")
        
        return {
            "composition": composition,
            "ph": round(ph_val, 2),
            "organic_matter": round(organic_val, 2)
        }

    def _extract_soil_property(self, layers: List[Dict], property_name: str) -> float:
        """Extract and average values for a given soil property"""
        values = [layer[property_name].get('value', 0) for layer in layers if property_name in layer]
        return np.mean(values) if values else 0

    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        """Validate geographic coordinates"""
        try:
            lat_float = float(lat)
            lon_float = float(lon)
            return -90 <= lat_float <= 90 and -180 <= lon_float <= 180
        except ValueError:
            return False

    def _generate_error_response(self, error_message: str) -> Dict[str, Any]:
        """Standard error response format"""
        return {
            "status": "error",
            "message": error_message,
            "timestamp": datetime.now().isoformat(),
            "data_quality": {
                "level": DataQualityLevel.INSUFFICIENT.value,
                "sources_available": {
                    "weather_data": False,
                    "nasa_data": False,
                    "soil_data": False
                }
            }
        }

def main():
    """Main function to demonstrate usage"""
    latitude = 40.7128
    longitude = -74.0060
    analyzer = SoilAnalyzer()
    results = analyzer.analyze_location(latitude, longitude)
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
