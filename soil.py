import ee
import folium
from IPython.display import display

# Initialize Earth Engine
ee.Initialize(project="ee-henrymwoha02")

def get_soil_moisture_soil_data(lon_min, lat_min, lon_max, lat_max, start_date, end_date):
    # Define Area of Interest (AOI)
    aoi = ee.Geometry.Rectangle([lon_min, lat_min, lon_max, lat_max])

    # Sentinel-1 soil moisture proxy using VV band
    soil_moisture = ee.ImageCollection("COPERNICUS/S1_GRD") \
                    .filterBounds(aoi) \
                    .filterDate(start_date, end_date) \
                    .filter(ee.Filter.eq('instrumentMode', 'IW')) \
                    .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
                    .select('VV').mean().clip(aoi)

    # Sentinel-2 soil reflectance using NIR band (B8)
    soil_reflectance = ee.ImageCollection("COPERNICUS/S2") \
                        .filterBounds(aoi) \
                        .filterDate(start_date, end_date) \
                        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10)) \
                        .select('B8').mean().clip(aoi)

    # Create a map with the bounding box
    map_center = [(lat_min + lat_max) / 2, (lon_min + lon_max) / 2]
    folium_map = folium.Map(location=map_center, zoom_start=12)
    folium.Rectangle(
        bounds=[(lat_min, lon_min), (lat_max, lon_max)],
        color="blue", fill=True, fill_opacity=0.1
    ).add_to(folium_map)

    # Add soil moisture layer to the map
    soil_moisture_url = soil_moisture.getMapId({'min': -25, 'max': 0, 'palette': ['blue', 'yellow', 'green']})['tile_fetcher'].url_format
    folium.TileLayer(
        tiles=soil_moisture_url, attr='Soil Moisture', overlay=True
    ).add_to(folium_map)

    # Add soil reflectance layer to the map
    soil_reflectance_url = soil_reflectance.getMapId({'min': 100, 'max': 3000, 'palette': ['brown', 'white']})['tile_fetcher'].url_format
    folium.TileLayer(
        tiles=soil_reflectance_url, attr='Soil Reflectance', overlay=True
    ).add_to(folium_map)

    # Add layer control to toggle between layers
    folium.LayerControl().add_to(folium_map)
    
    # Display the map
    display(folium_map)

    # Option to export processed images to Google Drive
    export_to_drive = input("Do you want to export the soil moisture and reflectance data to Google Drive? (yes/no): ").strip().lower()
    if export_to_drive == "yes":
        # Export soil moisture to Google Drive
        task_moisture = ee.batch.Export.image.toDrive(
            image=soil_moisture,
            description='Soil_Moisture_Export',
            scale=30,
            region=aoi,
            fileFormat='GeoTIFF'
        )
        task_moisture.start()

        # Export soil reflectance to Google Drive
        task_reflectance = ee.batch.Export.image.toDrive(
            image=soil_reflectance,
            description='Soil_Reflectance_Export',
            scale=30,
            region=aoi,
            fileFormat='GeoTIFF'
        )
        task_reflectance.start()

        print("Export tasks started. Check your Google Drive for the output files.")

# User input for bounding box coordinates and date range
lon_min = float(input("Enter minimum longitude: "))
lat_min = float(input("Enter minimum latitude: "))
lon_max = float(input("Enter maximum longitude: "))

lat_max = float(input("Enter maximum latitude: "))
start_date = input("Enter start date (YYYY-MM-DD): ")
end_date = input("Enter end date (YYYY-MM-DD): ")

# Call function to get soil moisture and soil data
get_soil_moisture_soil_data(lon_min, lat_min, lon_max, lat_max, start_date, end_date)
