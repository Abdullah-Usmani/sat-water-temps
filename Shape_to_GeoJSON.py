import os
import geopandas as gpd

# Load the shapefile
# Saves to same directory as this python file
output_dir = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\sat-water-temps\static"
shapefile_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/polygon/new_polygons.shp"

output_path = os.path.join(output_dir, 'polygons_new.geojson')
gdf = gpd.read_file(shapefile_path)

# Convert to GeoJSON format
geojson_data = gdf.to_json()

# Save the GeoJSON data to a file

# Create the directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

with open(output_path, 'w') as f:
    f.write(geojson_data)