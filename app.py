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

BASE_PATH = "./Water Temp Sensors/ECO"  # Adjust path as needed

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
    geojson_path = os.path.join(root_folder, 'sat-water-temps', 'static', 'polygons.geojson')  # Adjust path as needed

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
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    
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
    layer_type = extract_layer(tif_path)  # Assumes function exists
    png_path = tif_path.replace(".tif", ".png")
    print(png_path)
    with rasterio.open(tif_path) as dataset:
        if layer_type in ['QC', 'cloud']:
            # print("Layer Type = QC/cloud")
            data = dataset.read(1)  # Read first band
            min_val, max_val = np.min(data), np.max(data)
            if max_val - min_val == 0:
                norm_data = np.zeros_like(data, dtype=np.uint8)
            else:
                norm_data = (data - min_val) / (max_val - min_val + 1e-6)  # Normalize

            # Save using appropriate colormap
            plt.imsave(png_path, norm_data, cmap='jet')

        else:  # Handle LST, LST_err
            # print("Layer Type = LST/err")
            num_bands = dataset.count
            print(f"Number of bands: {num_bands}")  # Debugging output

            bands = [dataset.read(1)]  # Read the first band (assuming grayscale)

            norm_bands = []
            for band in bands:
                min_val = np.nanmin(band)  # Use np.nanmin to ignore NaN values
                max_val = np.nanmax(band)

                print(f"Min: {min_val}, Max: {max_val}")  # Debugging output
                norm_band = ((band - min_val) / (max_val - min_val) * 255).astype(np.uint8)

                norm_bands.append(norm_band)

            # Stack grayscale as RGB if necessary
            img_array = np.stack([norm_bands[0]] * 3, axis=-1)  # Convert grayscale to RGB

            # Convert to image format and return response
            img = Image.fromarray(img_array)
            img_bytes = io.BytesIO()
            img.save(img_bytes, format="PNG")
            img_bytes.seek(0)
            png_path = img_bytes

    return png_path
     
@app.route('/serve_tif_as_png/<feature_id>/<filename>')
def serve_tif_as_png(feature_id, filename):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    tif_path = os.path.join(data_folder, filename)

    if not os.path.exists(tif_path):
        abort(404)

    png_path = convert_tif_to_png(tif_path)
    return send_file(png_path, mimetype='image/png')

@app.route('/download_tif/<feature_id>/<filename>')
def download_tif(feature_id, filename):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)
    
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        abort(404)


if __name__ == "__main__":
    app.run(debug=True)
