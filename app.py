from flask import Flask, json, request, jsonify, send_file, render_template, abort
import os
import io
import re
from matplotlib import pyplot as plt
import numpy as np
import rasterio
from rasterio.plot import reshape_as_image
from PIL import Image

app = Flask(__name__)

# Define the external data directory
root_folder = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\\"

BASE_PATH = "./Water Temp Sensors/ECOraw"  # Adjust path as needed

@app.route('/')
def index():
    return render_template('index.html')

def extract_layer(filename):
    match = re.search(r'ECO_L2T_LSTE\.002_([A-Za-z]+(?:_err)?)_', filename)
    return match.group(1) if match else "unknown"

# Register it as a Jinja filter
app.jinja_env.filters['extract_layer'] = extract_layer


@app.route('/feature/<feature_id>')
def feature_page(feature_id):
    geojson_path = os.path.join(root_folder, 'sat-water-temps', 'static', 'polygons_new.geojson')  # Adjust path as needed

    # Load GeoJSON and find the lake feature
    with open(geojson_path, 'r') as f:
        geojson_data = json.load(f)

    polygon_coords = None
    for feature in geojson_data['features']:
        if feature['properties']['name'] == feature_id and feature['properties']['location'] == 'lake':  
            polygon_coords = feature['geometry']['coordinates']
            break

    if polygon_coords is None:
        abort(404)  # Feature not found

    return render_template('feature_page.html', feature_id=feature_id, coords=json.dumps(polygon_coords))

@app.route('/feature/<feature_id>/archive')
def feature_archive(feature_id):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECOraw', feature_id, 'lake')
    # Add data_folder check for RIVER folder

    if not os.path.isdir(data_folder):
        abort(404)

    tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif')]
    
    return render_template('feature_archive.html', feature_id=feature_id, tif_files=tif_files)

# Function to convert .tif to .png for display
def convert_tif_to_png(tif_path):
    """
    Converts a .tif file to a .png image using different processing methods
    based on the layer type.
    """
    # layer_type = extract_layer(tif_path)  # Assumes function exists
    with rasterio.open(tif_path) as dataset:
        # if layer_type in ['QC', 'cloud', 'height', 'water', 'EmisWB']:
        #    add logic for different color palettes for each layer type
        bands = [dataset.read(1)]  # Read the first band (assuming grayscale)

        norm_bands = []
        for band in bands:
            min_val = np.nanmin(band)  # Use np.nanmin to ignore NaN values
            max_val = np.nanmax(band)

            # print(f"Min: {min_val}, Max: {max_val}")  # Debugging output
            norm_band = ((band - min_val) / (max_val - min_val) * 255).astype(np.uint8)

            norm_bands.append(norm_band)

        # Stack grayscale as RGB if necessary
        img_array = np.stack([norm_bands[0]] * 3, axis=-1)  # Convert grayscale to RGB

        # Convert to image format and return response
        img = Image.fromarray(img_array)
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)

    return img_bytes

# Normalize each band to 0-255 and create an alpha mask for missing data
def normalize(data):
    """Normalizes data to 0-255 and handles NaN values."""
    data = np.where(np.isfinite(data), data, np.nan)  # Convert Inf to NaN
    min_val, max_val = np.nanmin(data), np.nanmax(data)

    if np.isnan(min_val) or np.isnan(max_val) or max_val == min_val:
        return np.zeros_like(data, dtype=np.uint8), np.zeros_like(data, dtype=np.uint8)  # Black + Transparent

    norm_data = ((data - min_val) / (max_val - min_val) * 255).astype(np.uint8)

    # Create an alpha mask: Transparent for NaN/missing values, opaque for valid data
    alpha_mask = np.where(np.isnan(data) | (data < -1000), 0, 255).astype(np.uint8)

    return norm_data, alpha_mask

# This thing creates a .png for the .tif file
def multi_tif_to_png(tif_path):
    """Converts a multi-band .tif to a .png with transparency for missing data."""

    with rasterio.open(tif_path) as dataset:
        num_bands = dataset.count
        # print(f"Number of bands: {num_bands}")

        if num_bands < 5:
            # Return a placeholder image indicating the image is missing
            img = Image.new('RGBA', (256, 256), (255, 0, 0, 0))  # Red transparent image
            img_bytes = io.BytesIO()
            img.save(img_bytes, format="PNG")
            img_bytes.seek(0)
            return img_bytes

        bands = [dataset.read(band) for band in range(1, num_bands + 1)]
        norm_bands, alpha_mask = zip(*[normalize(band) for band in bands])

        # Color coded output?    
        # # Use the first band for color mapping
        # norm_band = norm_bands[0]

        # # Apply a colormap (e.g., 'jet') using matplotlib
        # cmap = plt.get_cmap('jet')
        # rgba_img = cmap(norm_band / 255.0)  # Normalize to 0-1 for colormap

        # # Convert to 8-bit per channel
        # rgba_img = (rgba_img * 255).astype(np.uint8)

        # # Add transparency channel (alpha)
        # rgba_img[..., 3] = alpha_mask[0]

        # Grayscale custom output
        # Stack bands as RGB, using only the first band as grayscale
        # Can add selector for different color combinations and what not
        img_array = np.stack([norm_bands[0], norm_bands[0], norm_bands[0]], axis=-1)  # Use bands 0, 3, and 4 as RGB
        
        # Add transparency channel (alpha)
        img_array = np.dstack((img_array, alpha_mask[0]))

        # Save as PNG
        img = Image.fromarray(img_array, mode="RGBA")
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)

    return img_bytes
    
@app.route('/serve_tif_as_png/<feature_id>/<filename>')
def serve_tif_as_png(feature_id, filename):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECOraw', feature_id, 'lake')
    tif_path = os.path.join(data_folder, filename)

    if not os.path.exists(tif_path):
        abort(404)

    img_bytes = convert_tif_to_png(tif_path)
    return send_file(img_bytes, mimetype='image/png')

@app.route('/latest_lst_tif/<feature_id>/')  # Add route for serving .png files
def get_latest_lst_tif(feature_id):
    """Finds and returns the latest .tif file in the specified folder."""

    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')

    filtered_files = [os.path.join(data_folder, file) for file in os.listdir(data_folder) if file.endswith('.tif')]

    # Sort by modification time (newest first)
    if filtered_files:
        filtered_files.sort(key=os.path.getmtime, reverse=True)
        img_bytes = multi_tif_to_png(filtered_files[0])
        return send_file(img_bytes, mimetype='image/png')
    
    return None  # Return None if no .tif file is found

@app.route('/download_tif/<feature_id>/<filename>')
def download_tif(feature_id, filename):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECOraw', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)
    
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        abort(404)

if __name__ == "__main__":
    app.run(debug=True)
