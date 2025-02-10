import geopandas as gpd

# Load the shapefile
shapefile_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/polygon/site_full_ext.shp"
gdf = gpd.read_file(shapefile_path)

# Convert to GeoJSON format
geojson_data = gdf.to_json()

# Save the GeoJSON data to a file
# Saves to same directory as this python file
with open('/static/polygons.geojson', 'w') as f:
    f.write(geojson_data)