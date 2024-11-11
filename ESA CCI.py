import json
from typing import Tuple, Dict, List
from datetime import datetime
import random  # For simulation purposes

class AgriDataSimulator:
    """
    Simulates agricultural data retrieval and returns JSON structured output
    instead of downloading actual data files.
    """
    
    def __init__(self):
        self.soil_layers = {
            1: {'depth': '0-7cm'},
            2: {'depth': '7-28cm'},
            3: {'depth': '28-100cm'},
            4: {'depth': '100-289cm'}
        }
    
    def get_era5_land_data(self, 
                          coordinates: Tuple[float, float],
                          year: int,
                          month: int) -> Dict:
        """
        Simulate ERA5-Land data retrieval and return structured JSON.
        
        Args:
            coordinates: (latitude, longitude)
            year: Year of interest
            month: Month of interest
        """
        lat, lon = coordinates
        
        # Create data structure
        era5_data = {
            "metadata": {
                "coordinates": {"latitude": lat, "longitude": lon},
                "time_period": {
                    "year": year,
                    "month": month,
                },
                "data_source": "ERA5-Land",
                "spatial_resolution": "0.1 degrees",
                "temporal_resolution": "6-hourly"
            },
            "soil_temperature": {
                f"layer_{i}": {
                    "depth": self.soil_layers[i]['depth'],
                    "daily_mean": round(random.uniform(5, 25), 2),
                    "daily_min": round(random.uniform(0, 15), 2),
                    "daily_max": round(random.uniform(20, 35), 2),
                    "unit": "°C"
                } for i in range(1, 5)
            },
            "soil_moisture": {
                f"layer_{i}": {
                    "depth": self.soil_layers[i]['depth'],
                    "daily_mean": round(random.uniform(0.2, 0.4), 3),
                    "daily_min": round(random.uniform(0.1, 0.3), 3),
                    "daily_max": round(random.uniform(0.3, 0.5), 3),
                    "unit": "m³/m³"
                } for i in range(1, 5)
            },
            "surface_parameters": {
                "surface_pressure": {
                    "daily_mean": round(random.uniform(980, 1020), 1),
                    "daily_min": round(random.uniform(970, 990), 1),
                    "daily_max": round(random.uniform(1010, 1030), 1),
                    "unit": "hPa"
                },
                "total_precipitation": {
                    "monthly_total": round(random.uniform(0, 200), 1),
                    "daily_max": round(random.uniform(0, 50), 1),
                    "unit": "mm"
                },
                "temperature_2m": {
                    "daily_mean": round(random.uniform(10, 25), 1),
                    "daily_min": round(random.uniform(5, 15), 1),
                    "daily_max": round(random.uniform(20, 35), 1),
                    "unit": "°C"
                }
            }
        }
        
        return era5_data

    def get_satellite_soil_moisture(self,
                                  coordinates: Tuple[float, float],
                                  year: int,
                                  month: int) -> Dict:
        """
        Simulate satellite soil moisture data retrieval.
        """
        lat, lon = coordinates
        
        satellite_data = {
            "metadata": {
                "coordinates": {"latitude": lat, "longitude": lon},
                "time_period": {
                    "year": year,
                    "month": month,
                },
                "data_source": "Satellite Soil Moisture",
                "spatial_resolution": "0.25 degrees",
                "temporal_resolution": "daily"
            },
            "surface_soil_moisture": {
                "daily_mean": round(random.uniform(0.2, 0.4), 3),
                "daily_min": round(random.uniform(0.1, 0.3), 3),
                "daily_max": round(random.uniform(0.3, 0.5), 3),
                "uncertainty": round(random.uniform(0.02, 0.08), 3),
                "unit": "m³/m³",
                "depth": "0-5cm"
            }
        }
        
        return satellite_data

def main():
    """
    Example usage demonstrating JSON output format.
    """
    try:
        # Get user input
        latitude = float(input("Latitude (-90 to 90): "))
        longitude = float(input("Longitude (-180 to 180): "))
        year = int(input("Year (e.g., 2023): "))
        month = int(input("Month (1-12): "))
        
        # Validate inputs
        if not (-90 <= latitude <= 90):
            raise ValueError("Latitude must be between -90 and 90")
        if not (-180 <= longitude <= 180):
            raise ValueError("Longitude must be between -180 and 180")
        if not (1 <= month <= 12):
            raise ValueError("Month must be between 1 and 12")
        
        # Initialize simulator
        simulator = AgriDataSimulator()
        
        # Get simulated data
        era5_data = simulator.get_era5_land_data(
            coordinates=(latitude, longitude),
            year=year,
            month=month
        )
        
        satellite_data = simulator.get_satellite_soil_moisture(
            coordinates=(latitude, longitude),
            year=year,
            month=month
        )
        
        # Combine data sources
        combined_data = {
            "era5_land": era5_data,
            "satellite": satellite_data,
            "retrieved_at": datetime.now().isoformat()
        }
        
        # Print formatted JSON
        print(json.dumps(combined_data, indent=2))
        
    except ValueError as e:
        print(f"Invalid input: {str(e)}")

if __name__ == "__main__":
    main()