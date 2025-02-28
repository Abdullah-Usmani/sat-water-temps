<<<<<<< HEAD
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
=======
import geopandas as gpd

# Load the shapefile
shapefile_path = r"C:\Users\Abdullah Usmani\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/polygon/site_full_ext.shp"
gdf = gpd.read_file(shapefile_path)

# Convert to GeoJSON format
geojson_data = gdf.to_json()

# Save the GeoJSON data to a file
# Saves to same directory as this python file
with open('polygons.geojson', 'w') as f:
>>>>>>> 25db19d0c430b659f5118d10e74f4d8d6560e49f
    f.write(geojson_data)