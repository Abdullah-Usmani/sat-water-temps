from flask import Flask, json, jsonify, send_file, render_template, abort
from flask import Flask, json, jsonify, send_file, render_template, abort
import os
import io
import re
import pandas as pd
import pandas as pd
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from PIL import Image
# from ECO_Converted import extract_metadata 

app = Flask(__name__)

# Define the external data directory
root_folder = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\\"

BASE_PATH = "./Water Temp Sensors/ECOraw"  # Adjust path as needed


GLOBAL_MIN = 273  # Kelvin
GLOBAL_MAX = 308  # Kelvin

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
    # Add data_folder check for RIVER folder

    if not os.path.isdir(data_folder):
        abort(404)

    tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif')]
    
    return render_template('feature_archive.html', feature_id=feature_id, tif_files=tif_files)

@app.route('/serve_tif_as_png/<feature_id>/<filename>')
def serve_tif_as_png(feature_id, filename):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
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

    
@app.route('/feature/<feature_id>/temperature')
def get_latest_temperature(feature_id):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    csv_files = [os.path.join(data_folder, file) for file in os.listdir(data_folder) if file.endswith('.csv')]

    if not csv_files:
        return jsonify({"error": "No CSV files found"}), 404

    csv_files.sort(key=os.path.getmtime, reverse=True)
    csv_path = csv_files[0]
    
    df = pd.read_csv(csv_path)
    if not {'x', 'y', 'LST_filter'}.issubset(df.columns):
        return jsonify({"error": "CSV file missing required columns"}), 400

    temp_data = df[['x', 'y', 'LST_filter']].dropna()
    
    if temp_data.empty:
        return jsonify({"error": "No data found"}), 404
    
    return temp_data.to_json(orient='records')

@app.route('/feature/<feature_id>/get_dates')
def get_doys(feature_id):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    if not os.path.isdir(data_folder):
        abort(404)

    tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif')]
    doys = get_updated_dates(tif_files)  # Assuming extract_metadata returns a dictionary with 'DOY'
    return jsonify(list(reversed(doys)))

@app.route('/feature/<feature_id>/tif/<doy>')
def get_tif_by_doy(feature_id, doy):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif')]

    for tif_file in tif_files:
        metadata = extract_metadata(tif_file)
        if metadata[1] == doy:
            tif_path = os.path.join(data_folder, tif_file)
            img_bytes = convert_tif_to_png(tif_path)
            return send_file(img_bytes, mimetype='image/png')

    abort(404)  # No matching DOY found

@app.route('/feature/<feature_id>/temperature/<doy>')
def get_temperature_by_doy(feature_id, doy):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    csv_files = [os.path.join(data_folder, file) for file in os.listdir(data_folder) if file.endswith('.csv')]

    for csv_file in csv_files:
        metadata = extract_metadata(csv_file)
        if metadata[1] == doy:
            csv_path = os.path.join(data_folder, csv_file)
            df = pd.read_csv(csv_path)
            if not {'x', 'y', 'LST_filter'}.issubset(df.columns):
                return jsonify({"error": "CSV file missing required columns"}), 400

            temp_data = df[['x', 'y', 'LST_filter']].dropna()
            
            if temp_data.empty:
                return jsonify({"error": "No data found"}), 404
            
            return temp_data.to_json(orient='records')
            
@app.route('/feature/<feature_id>/check_wtoff/<date>')
def check_wtoff(feature_id, date):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    
    if not os.path.isdir(data_folder):
        abort(404)

    try:
        tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif') and "_wtoff" in f and date in f]
    except Exception as e:
        print("Error fetching .tif files:", e)
        return jsonify({"error": "Failed to fetch files"}), 500

    if tif_files:
        return jsonify({"wtoff": True, "files": tif_files})
    else:
        return jsonify({"wtoff": False})

@app.route('/download_tif/<feature_id>/<filename>')
def download_tif(feature_id, filename):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)
    
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        abort(404)

@app.route('/download_csv/<feature_id>/<filename>')
def download_csv(feature_id, filename):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)
    
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        abort(404)

# get all DOYs from the folder
# show DOYs in selector
# when DOY is selected, show the image for that DOY

# Function to extract aid number and date from filename
def extract_metadata(filename):
    aid_match = re.search(r'aid(\d{4})', filename)
    date_match = re.search(r'lake_(\d{13})', filename)

    aid_number = int(aid_match.group(1)) if aid_match else None
    date = date_match.group(1) if date_match else None

    return aid_number, date

def get_updated_folders(new_files):
    return [extract_metadata(f)[0] for f in new_files if extract_metadata(f)[0]]

# Function to filter only new files and return unique dates
def get_updated_dates(new_files):
    return [extract_metadata(f)[1] for f in new_files if extract_metadata(f)[1]]

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

# Function to convert .tif to .png for display
def convert_tif_to_png(tif_path):
    """
    Converts a .tif file to a .png image using different processing methods
    based on the layer type.
    """
    # layer_type = extract_layer(tif_path)  # Assumes function exists
    with rasterio.open(tif_path) as dataset:
        # # if layer_type in ['QC', 'cloud', 'height', 'water', 'EmisWB']:
        # #    add logic for different color palettes for each layer type
        # bands = [dataset.read(1)]  # Read the first band (assuming grayscale)

        # norm_bands = []
        # for band in bands:
        #     min_val = np.nanmin(band)  # Use np.nanmin to ignore NaN values
        #     max_val = np.nanmax(band)

        #     # print(f"Min: {min_val}, Max: {max_val}")  # Debugging output
        #     norm_band = ((band - min_val) / (max_val - min_val) * 255).astype(np.uint8)

        #     norm_bands.append(norm_band)


        # # Stack grayscale as RGB if necessary
        # img_array = np.stack([norm_bands[0]] * 3, axis=-1)  # Convert grayscale to RGB

        # # Convert to image format and return response
        # img = Image.fromarray(img_array)
        # img_bytes = io.BytesIO()
        # img.save(img_bytes, format="PNG")
        # img_bytes.seek(0)

        num_bands = dataset.count
        # print(f"Number of bands: {num_bands}")

        if num_bands < 5:
            # Return a placeholder image indicating the image is missing
            img = Image.new('RGBA', (256, 256), (255, 0, 0, 0))  # Red transparent image
            img_bytes = io.BytesIO()
            img.save(img_bytes, format="PNG")
            img_bytes.seek(0)
            return img_bytes

        # Single color-scale output for LST
        # Read the first band
        band = dataset.read(1).astype(np.float32)  # Convert to float for normalization

        # Handle no-data values (replace NaNs with 0)
        band[np.isnan(band)] = 0  # Ensure NaNs don't affect normalization

        # Normalize band using a fixed global min/max
        band = np.clip(band, GLOBAL_MIN, GLOBAL_MAX)  # Clip values to valid range
        norm_band = ((band - GLOBAL_MIN) / (GLOBAL_MAX - GLOBAL_MIN) * 255).astype(np.uint8)

        # Generate an alpha mask: Transparent where band = 0 (or NaN originally)
        alpha_mask = np.where(band <= GLOBAL_MIN, 0, 255).astype(np.uint8)

        # Apply colormap (e.g., 'jet')
        cmap = plt.get_cmap('jet')
        rgba_img = cmap(norm_band / 255.0)  # Normalize to 0-1 for colormap

        # Convert to 8-bit per channel
        rgba_img = (rgba_img * 255).astype(np.uint8)

        # **Ensure the alpha channel is set correctly**
        rgba_img[..., 3] = alpha_mask  # Apply the transparency mask

        # bands = [dataset.read(band) for band in range(1, num_bands + 1)]
        # norm_bands, alpha_mask = zip(*[normalize(band) for band in bands])

        # # Color coded output?    
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
        # img_array = np.stack([norm_bands[0], norm_bands[0], norm_bands[0]], axis=-1)  # Use bands 0, 3, and 4 as RGB
        
        # Add transparency channel (alpha)
        # img_array = np.dstack((img_array, alpha_mask[0]))

        # Save as PNG
        img = Image.fromarray(rgba_img, mode="RGBA")
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)

    return img_bytes

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


        # Read the first band
        band = dataset.read(1).astype(np.float32)  # Convert to float for normalization

        # Handle no-data values (replace NaNs with 0)
        band[np.isnan(band)] = 0  # Ensure NaNs don't affect normalization

        # Normalize band using a fixed global min/max
        band = np.clip(band, GLOBAL_MIN, GLOBAL_MAX)  # Clip values to valid range
        norm_band = ((band - GLOBAL_MIN) / (GLOBAL_MAX - GLOBAL_MIN) * 255).astype(np.uint8)

        # Generate an alpha mask: Transparent where band = 0 (or NaN originally)
        alpha_mask = np.where(band <= GLOBAL_MIN, 0, 255).astype(np.uint8)

        # Apply colormap (e.g., 'jet')
        cmap = plt.get_cmap('jet')
        rgba_img = cmap(norm_band / 255.0)  # Normalize to 0-1 for colormap

        # Convert to 8-bit per channel
        rgba_img = (rgba_img * 255).astype(np.uint8)

        # **Ensure the alpha channel is set correctly**
        rgba_img[..., 3] = alpha_mask  # Apply the transparency mask

        
        # bands = [dataset.read(band) for band in range(1, num_bands + 1)] # Read all bands
        # norm_bands, alpha_mask = zip(*[normalize(band) for band in bands]) # Normalize each band

        # # Color coded output?    
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
        # img_array = np.stack([norm_bands[0], norm_bands[0], norm_bands[0]], axis=-1)  # Use bands 0, 3, and 4 as RGB
        
        # Add transparency channel (alpha)
        # img_array = np.dstack((img_array, alpha_mask[0]))

        # Save as PNG
        img = Image.fromarray(rgba_img, mode="RGBA")
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)

    return img_bytes

@app.route('/feature/full-view')
def full_view():
    return render_template('full_view.html')

if __name__ == "__main__":
    app.run(debug=True)