import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from typing import List, Tuple, Dict, Optional
import matplotlib.pyplot as plt

class ESACCISoilData:
    """
    A class to handle retrieval and processing of ESA CCI soil moisture data through the updated CDS API.
    """
    
    def __init__(self, output_dir: str = "./esa_cci_data"):
        """
        Initialize the ESA CCI Soil Data handler.
        """
        self.output_dir = output_dir
        self.c = cdsapi.Client()
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def get_soil_moisture(self, 
                         year: int,
                         month: int,
                         output_file: str) -> str:
        """
        Retrieve soil moisture data using the updated CDS API format.
        
        Args:
            year (int): Year to retrieve data for
            month (int): Month to retrieve data for
            output_file (str): Path to save the output file
            
        Returns:
            str: Path to the downloaded file
        """
        try:
            # Request data from CDS using updated API format
            self.c.retrieve(
                'satellite-soil-moisture',
                {
                    'format': 'netcdf',
                    'processing_level': 'level_2',
                    'sensor': 'active_passive_combined',
                    'temporal_resolution': 'daily',
                    'time': [
                        f'{year}-{month:02d}-01/to/{year}-{month:02d}-31'
                    ],
                    'variable': [
                        'soil_moisture',
                        'soil_moisture_uncertainty',
                    ]
                },
                output_file
            )
            
            return output_file
            
        except Exception as e:
            raise Exception(f"Error retrieving data from CDS API: {str(e)}")

    def process_soil_data(self, file_path: str) -> xr.Dataset:
        """
        Process downloaded NetCDF file into an xarray Dataset.
        """
        try:
            dataset = xr.open_dataset(file_path)
            return dataset
        except Exception as e:
            raise Exception(f"Error processing NetCDF file: {str(e)}")

    def analyze_soil_data(self, 
                         dataset: xr.Dataset,
                         location: Tuple[float, float] = None) -> Dict:
        """
        Analyze soil moisture data for a specific location or the entire dataset.
        """
        results = {}
        
        if location:
            lat, lon = location
            data = dataset.sel(latitude=lat, longitude=lon, method='nearest')
        else:
            data = dataset
            
        # Calculate basic statistics for soil moisture
        if 'soil_moisture' in data.variables:
            moisture_stats = {
                'mean': float(data.soil_moisture.mean()),
                'std': float(data.soil_moisture.std()),
                'min': float(data.soil_moisture.min()),
                'max': float(data.soil_moisture.max())
            }
            results['soil_moisture_statistics'] = moisture_stats
            
        return results

    def plot_time_series(self,
                        dataset: xr.Dataset,
                        variable: str,
                        location: Tuple[float, float],
                        save_path: Optional[str] = None):
        """
        Plot time series data for a specific variable at a given location.
        """
        lat, lon = location
        data = dataset.sel(latitude=lat, longitude=lon, method='nearest')
        
        plt.figure(figsize=(12, 6))
        data[variable].plot()
        plt.title(f'{variable.replace("_", " ").title()} Time Series at ({lat}, {lon})')
        plt.grid(True)
        
        if save_path:
            plt.savefig(save_path)
        plt.close()

def main():
    """
    Example usage of the updated ESACCISoilData class.
    """
    # Initialize the handler
    handler = ESACCISoilData()
    
    # Define parameters
    year = 2023
    month = 1
    output_file = "soil_data.nc"
    location = (48.8566, 2.3522)  # Paris coordinates
    
    try:
        # Retrieve data
        file_path = handler.get_soil_moisture(
            year=year,
            month=month,
            output_file=output_file
        )
        
        # Process the data
        dataset = handler.process_soil_data(file_path)
        
        # Analyze data
        analysis = handler.analyze_soil_data(dataset, location)
        print("Analysis results:", analysis)
        
        # Create plots
        handler.plot_time_series(
            dataset=dataset,
            variable='soil_moisture',
            location=location,
            save_path='soil_moisture_time_series.png'
        )
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()