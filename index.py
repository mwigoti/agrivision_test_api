
import folium

mapobj = folium.Map(location=[-1.2930392499491719, 36.817867215366086])



folium.TileLayer('stamenterrain', attr='stamenterrain').add_to(mapobj)

	
folium.TileLayer('mapquestopen', attr='mapquestopen').add_to(mapobj)
	
folium.TileLayer('MapQuest Open Aerial', attr='mapquestopenaerial').add_to(mapobj)
	
folium.TileLayer('Mapbox Bright',attr='mapboxbright').add_to(mapobj)

folium.LayerControl().add_to(mapobj)

mapobj.save('map.html')

