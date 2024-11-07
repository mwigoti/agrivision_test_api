import folium

# Create a map object centered on the specified location
mapobj = folium.Map(location=[-1.2930392499491719, 36.817867215366086], zoom_start=13)

# Add a terrain layer from Stamen
folium.TileLayer('Stamen Terrain', attr='Stamen Terrain').add_to(mapobj)

# Add other supported layers
folium.TileLayer('Stamen Toner', attr='Stamen Toner').add_to(mapobj)
folium.TileLayer('Stamen Watercolor', attr='Stamen Watercolor').add_to(mapobj)
folium.TileLayer('OpenStreetMap').add_to(mapobj)

# Add a layer control panel
folium.LayerControl().add_to(mapobj)

# Save map to an HTML file
mapobj.save('map.html')
