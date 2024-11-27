from pyproj import Proj, transform

# Define the projected coordinate system (replace with the correct EPSG code)
# Example: UTM Zone 33N (EPSG:32633)
proj_in = Proj(init='epsg:3857')  # Example: UTM Zone 33N

# Define WGS84 (latitude/longitude)
proj_out = Proj(init='epsg:4326')  # WGS84

# Example coordinates (replace these with your coordinates)
x = 4099485.256665835  # easting (X)
y = -55182.457381204076  # northing (Y)

# Convert to latitude and longitude
longitude, latitude = transform(proj_in, proj_out, x, y)

print(f"Latitude: {latitude}, Longitude: {longitude}")
