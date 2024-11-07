
import folium

mapobj = folium.Map(location=[-1.2930392499491719, 36.817867215366086], zoom_start='stamenterrain')



folium.TileLayer('stamenterrain').add_to(mapobj)

	
folium.TileLayer('mapquestopen').add_to(mapobj)
	
folium.TileLayer('MapQuest Open Aerial').add_to(mapobj)
	
folium.TileLayer('Mapbox Bright').add_to(mapobj)

folium.LayerControl().add_to(mapobj)

mapobj.save('map.html')

