import cdsapi
import xarray as xr
import pandas as pd
from typing import Tuple, Dict, List
import os
from datetime import datetime

class AgriDataRetriever:
    """
    Retrieves agricultural relevant data (soil and climate) for specific coordinates
    using the CDS API.
    """
    
    def __init__(self):
        self.client = cdsapi.Client()
        
    def get_era5_land_data(self, 
                          coordinates: Tuple[float, float],
                          year: int,
                          month: int,
                          output_dir: str = "./data") -> str:
        """
        Retrieve ERA5-Land data (soil and surface parameters) for specific coordinates.
        
        Args:
            coordinates: (latitude, longitude)
            year: Year of interest
            month: Month of interest
            output_dir: Directory to save the data
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Convert coordinates to area format [North, West, South, East]
        lat, lon = coordinates
        area = [lat + 0.1, lon - 0.1, lat - 0.1, lon + 0.1]
        
        target = os.path.join(output_dir, f'era5_land_{year}_{month:02d}.nc')
        
        request = {
            'format': 'netcdf',
            'variable': [
                'soil_temperature_level_1',
                'soil_temperature_level_2',
                'soil_temperature_level_3',
                'soil_temperature_level_4',
                'volumetric_soil_water_layer_1',
                'volumetric_soil_water_layer_2',
                'volumetric_soil_water_layer_3',
                'volumetric_soil_water_layer_4',
                'surface_pressure',
                'total_precipitation',
                '2m_temperature',
                'soil_type',
            ],
            'year': str(year),
            'month': str(month).zfill(2),
            'day': [str(day).zfill(2) for day in range(1, 32)],
            'time': [
                '00:00', '06:00', '12:00', '18:00'
            ],
            'area': area,
        }
        
        try:
            self.client.retrieve(
                'reanalysis-era5-land',
                request,
                target
            )
            return target
        except Exception as e:
            print(f"Error retrieving ERA5-Land data: {str(e)}")
            return None

    def get_soil_moisture_data(self,
                             coordinates: Tuple[float, float],
                             year: int,
                             month: int,
                             output_dir: str = "./data") -> str:
        """
        Retrieve satellite soil moisture data for specific coordinates.
        """
        os.makedirs(output_dir, exist_ok=True)
        
        lat, lon = coordinates
        area = [lat + 0.1, lon - 0.1, lat - 0.1, lon + 0.1]
        
        target = os.path.join(output_dir, f'soil_moisture_{year}_{month:02d}.nc')
        
        request = {
            'format': 'netcdf',
            'variable': [
                'surface_soil_moisture',
                'soil_moisture_uncertainty',
            ],
            'year': str(year),
            'month': str(month).zfill(2),
            'day': [str(day).zfill(2) for day in range(1, 32)],
            'time': [
                '00:00', '06:00', '12:00', '18:00'
            ],
            'type': 'v202212',
        }
        
        try:
            self.client.retrieve(
                'satellite-soil-moisture',
                request,
                target
            )
            return target
        except Exception as e:
            print(f"Error retrieving soil moisture data: {str(e)}")
            return None

    def analyze_data(self, 
                    filepath: str, 
                    coordinates: Tuple[float, float]) -> Dict:
        """
        Analyze the retrieved data for the specific location.
        """
        try:
            ds = xr.open_dataset(filepath)
            lat, lon = coordinates
            
            # Select the nearest point to our coordinates
            ds_point = ds.sel(latitude=lat, longitude=lon, method='nearest')
            
            # Calculate basic statistics for each variable
            stats = {}
            for var in ds.data_vars:
                if var in ds_point:
                    stats[var] = {
                        'mean': float(ds_point[var].mean()),
                        'min': float(ds_point[var].min()),
                        'max': float(ds_point[var].max()),
                        'std': float(ds_point[var].std())
                    }
            
            return stats
            
        except Exception as e:
            print(f"Error analyzing data: {str(e)}")
            return None

def main():
    """
    Example usage of the AgriDataRetriever class.
    """
    # Get user input
    print("Enter coordinates for data retrieval:")
    try:
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
            
    except ValueError as e:
        print(f"Invalid input: {str(e)}")
        return
    
    # Initialize retriever
    retriever = AgriDataRetriever()
    
    print("\nRetrieving data...")
    
    # Get ERA5-Land data
    era5_file = retriever.get_era5_land_data(
        coordinates=(latitude, longitude),
        year=year,
        month=month
    )
    
    # Get soil moisture data
    soil_file = retriever.get_soil_moisture_data(
        coordinates=(latitude, longitude),
        year=year,
        month=month
    )
    
    # Analyze the data
    if era5_file:
        print("\nERA5-Land data statistics:")
        stats = retriever.analyze_data(era5_file, (latitude, longitude))
        if stats:
            for var, var_stats in stats.items():
                print(f"\n{var}:")
                for stat_name, value in var_stats.items():
                    print(f"  {stat_name}: {value}")
    
    if soil_file:
        print("\nSoil moisture data statistics:")
        stats = retriever.analyze_data(soil_file, (latitude, longitude))
        if stats:
            for var, var_stats in stats.items():
                print(f"\n{var}:")
                for stat_name, value in var_stats.items():
                    print(f"  {stat_name}: {value}")

if __name__ == "__main__":
    main()