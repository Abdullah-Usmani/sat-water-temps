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

app = Flask(__name__)

# Define the external data directory
root_folder = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\\"

BASE_PATH = "./Water Temp Sensors/ECOraw"  # Adjust path as needed


GLOBAL_MIN = 273.15  # Kelvin
GLOBAL_MAX = 308.15  # Kelvin

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

    img_bytes = tif_to_png(tif_path)
    return send_file(img_bytes, mimetype='image/png')

@app.route('/latest_lst_tif/<feature_id>/')  # Add route for serving .png files
def get_latest_lst_tif(feature_id, scale='relative'):
    """Finds and returns the latest .tif file in the specified folder."""

    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')

    filtered_files = [os.path.join(data_folder, file) for file in os.listdir(data_folder) if file.endswith('.tif')]

    # Sort by modification time (newest first)
    if filtered_files:
        filtered_files.sort(key=os.path.getmtime, reverse=True)
        img_bytes = tif_to_png(filtered_files[0])
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
    
    min_max_values = [temp_data['LST_filter'].min(), temp_data['LST_filter'].max()]
    
    return jsonify({
        "data": temp_data.to_dict(orient='records'),
        "min_max": min_max_values
    })

@app.route('/feature/<feature_id>/get_dates')
def get_doys(feature_id):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    if not os.path.isdir(data_folder):
        abort(404)

    tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif')]
    doys = get_updated_dates(tif_files)  # Assuming extract_metadata returns a dictionary with 'DOY'
    return jsonify(list(reversed(doys)))

@app.route('/feature/<feature_id>/tif/<doy>/<scale>')
def get_tif_by_doy(feature_id, doy, scale):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif')]

    for tif_file in tif_files:
        metadata = extract_metadata(tif_file)
        if metadata[1] == doy:
            tif_path = os.path.join(data_folder, tif_file)
            img_bytes = tif_to_png(tif_path, scale)
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
            
            min_max_values = [temp_data['LST_filter'].min(), temp_data['LST_filter'].max()]
            
            return jsonify({
                "data": temp_data.to_dict(orient='records'),
                "min_max": min_max_values
            })

            
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
        return jsonify({"wtoff": False, "files": tif_files})
    else:
        return jsonify({"wtoff": True})

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
    filename = filename.replace(".tif", ".csv")  # Change the file extension to .csv
    
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)
    

    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        abort(404)

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

# Consolidated function to convert .tif to .png with selectable color scale
def tif_to_png(tif_path, color_scale="relative"):
    """
    Converts a .tif file to a .png image using different processing methods
    based on the selected color scale.

    Parameters:
        tif_path (str): Path to the .tif file.
        color_scale (str): Color scale to use ("relative", "fixed", "grayscale").
    """
    with rasterio.open(tif_path) as dataset:
        num_bands = dataset.count

        if num_bands < 5:
            # Return a placeholder image indicating the image is missing
            img = Image.new('RGBA', (256, 256), (255, 0, 0, 0))  # Red transparent image
            img_bytes = io.BytesIO()
            img.save(img_bytes, format="PNG")
            img_bytes.seek(0)
            return img_bytes

        if color_scale == "fixed":
            # fixedscale output
            band = dataset.read(1).astype(np.float32)  # Convert to float for normalization
            band[np.isnan(band)] = 0
            band = np.clip(band, GLOBAL_MIN, GLOBAL_MAX)  # Clip values to valid range
            norm_band = ((band - GLOBAL_MIN) / (GLOBAL_MAX - GLOBAL_MIN) * 255).astype(np.uint8)
            alpha_mask = np.where(band <= GLOBAL_MIN, 0, 255).astype(np.uint8)
            cmap = plt.get_cmap('jet')
            rgba_img = cmap(norm_band / 255.0)  # Normalize to 0-1 for colormap
            rgba_img = (rgba_img * 255).astype(np.uint8)
            rgba_img[..., 3] = alpha_mask  # Apply transparency mask

        elif color_scale == "relative":
            # Relative color-coded output
            bands = [dataset.read(band) for band in range(1, num_bands + 1)]  # Read all bands
            norm_bands, alpha_mask = zip(*[normalize(band) for band in bands])  # Normalize each band
            norm_band = norm_bands[0]  # Use the first band for color mapping
            cmap = plt.get_cmap('jet')
            rgba_img = cmap(norm_band / 255.0)  # Normalize to 0-1 for colormap
            rgba_img = (rgba_img * 255).astype(np.uint8)
            rgba_img[..., 3] = alpha_mask[0]  # Apply transparency mask

        elif color_scale == "gray":
            # Grayscale output
            bands = [dataset.read(band) for band in range(1, num_bands + 1)]  # Read all bands
            norm_bands, alpha_mask = zip(*[normalize(band) for band in bands])  # Normalize each band
            img_array = np.stack([norm_bands[0], norm_bands[0], norm_bands[0]], axis=-1)  # Grayscale RGB
            img_array = np.dstack((img_array, alpha_mask[0]))  # Add transparency channel
            rgba_img = img_array

        else:
            raise ValueError(f"Invalid color_scale: {color_scale}. Choose 'relative', 'fixed', or 'grayscale'.")

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