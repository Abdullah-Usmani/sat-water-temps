from flask import Flask, json, jsonify, send_file, render_template, abort
from dotenv import load_dotenv
import os
import io
import re
import pandas as pd
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from PIL import Image
from supabase import create_client

app = Flask(__name__)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
bucket_name = "multitifs"

supabase_folder = f"{SUPABASE_URL}/storage/v1/object/public/{bucket_name}"

GLOBAL_MIN = 273.15  # Kelvin
GLOBAL_MAX = 308.15  # Kelvin

@app.route('/')
def index():
    try:
        response = supabase.storage.from_(bucket_name).download("static/polygons_new.geojson")
        geojson_data = json.loads(response.decode("utf-8"))
    except Exception as e:
        print("Error downloading geojson:", e)
        geojson_data = None  # Handle the error gracefully

    return render_template('index.html', geojson=json.dumps(geojson_data))

def extract_layer(filename):
    match = re.search(r'ECO_L2T_LSTE\.002_([A-Za-z]+(?:_err)?)_', filename)
    return match.group(1) if match else "unknown"

# Register it as a Jinja filter
app.jinja_env.filters['extract_layer'] = extract_layer

@app.route('/feature/<feature_id>')
def feature_page(feature_id):
    # # Load GeoJSON and find the lake feature
    try:
        response = supabase.storage.from_(bucket_name).download("static/polygons_new.geojson")
        geojson_data = json.loads(response.decode("utf-8"))
    except Exception as e:
        print("Error downloading geojson:", e)
        abort(500)  # Internal server error

    polygon_coords = None
    for feature in geojson_data['features']:
        if feature['properties']['name'] == feature_id and feature['properties']['location'] == 'lake':  
            polygon_coords = feature['geometry']['coordinates']
            break

    if polygon_coords is None:
        print("huh huh")
        abort(404)  # Feature not found

    return render_template('feature_page.html', feature_id=feature_id, coords=json.dumps(polygon_coords))

@app.route('/feature/<feature_id>/archive')
def feature_archive(feature_id):
    data_folder = f"ECO/{feature_id}/lake"
    # Add data_folder check for RIVER folder
    try:
        files = supabase.storage.from_(bucket_name).list(data_folder)
        tif_files = [file['name'] for file in files if file['name'].endswith('.tif')]
    except Exception as e:
        tif_files = []
        print("Error fetching .tif files:", e)
    
    return render_template('feature_archive.html', feature_id=feature_id, tif_files=tif_files)

  
@app.route('/serve_tif_as_png/<feature_id>/<filename>')
def serve_tif_as_png(feature_id, filename):
    data_folder = f"ECO/{feature_id}/lake"
    tif_path = f"{data_folder}/{filename}"
    try:
        response = supabase.storage.from_(bucket_name).download(tif_path)
        tif_data = io.BytesIO(response)
        img_bytes = tif_to_png(tif_data)
    except Exception as e:
        print("Error downloading or processing .tif file:", e)
        abort(500)
    return send_file(img_bytes, mimetype='image/png')

@app.route('/latest_lst_tif/<feature_id>/')  # Add route for serving .png files
def get_latest_lst_tif(feature_id):
    """Finds and returns the latest .tif file in the specified folder."""
    data_folder = f"ECO/{feature_id}/lake"
    try:
        files = supabase.storage.from_(bucket_name).list(data_folder)
        filtered_files = [file['name'] for file in files if file['name'].endswith('.tif')]
    except Exception as e:
        filtered_files = []
        print("Error fetching .tif files:", e)

    # Sort files alphabetically or by naming convention (Supabase does not provide modification time)
    if filtered_files:
        filtered_files.sort()  # Adjust sorting logic if needed
        latest_file_path = f"{data_folder}/{filtered_files[-1]}"  # Get the latest file (last in sorted list)
        try:
            response = supabase.storage.from_(bucket_name).download(latest_file_path)
            tif_data = io.BytesIO(response)
            img_bytes = tif_to_png(tif_data)
            return send_file(img_bytes, mimetype='image/png')
        except Exception as e:
            print("Error processing .tif file:", e)
            abort(500)

    abort(404)  # No .tif files found

@app.route('/feature/<feature_id>/temperature')
def get_latest_temperature(feature_id):
    data_folder = f"ECO/{feature_id}/lake"
    try:
        files = supabase.storage.from_(bucket_name).list(data_folder)
        csv_files = [file['name'] for file in files if file['name'].endswith('.csv')]
    except Exception as e:
        csv_files = []
        print("Error fetching .csv files:", e)

    if not csv_files:
        return jsonify({"error": "No CSV files found"}), 404

    # Assuming the CSV files are sorted by some naming convention, not modification time
    csv_files.sort()  # Sort alphabetically or by naming convention
    csv_path = f"{data_folder}/{csv_files[0]}"  # Use the first CSV file

    try:
        response = supabase.storage.from_(bucket_name).download(csv_path)
        csv_data = response.decode("utf-8")
        df = pd.read_csv(io.StringIO(csv_data))
    except Exception as e:
        print("Error reading CSV file:", e)
        return jsonify({"error": "Failed to read CSV file"}), 500

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
    data_folder = f"ECO/{feature_id}/lake"
    try:
        files = supabase.storage.from_(bucket_name).list(data_folder)
        tif_files = [file['name'] for file in files if file['name'].endswith('.tif')]
    except Exception as e:
        tif_files = []
        print("Error fetching .tif files:", e)

    doys = get_updated_dates(tif_files)  # Assuming extract_metadata returns a dictionary with 'DOY'
    return jsonify(list(reversed(doys)))

@app.route('/feature/<feature_id>/tif/<doy>/<scale>')
def get_tif_by_doy(feature_id, doy, scale):
    data_folder = f"ECO/{feature_id}/lake"
    try:    
        files = supabase.storage.from_(bucket_name).list(data_folder)
        tif_files = [file['name'] for file in files if file['name'].endswith('.tif')]
    except Exception as e:
        tif_files = []
        print("Error fetching .tif files:", e)

    for tif_file in tif_files:
        metadata = extract_metadata(tif_file)
        if metadata[1] == doy:
            # Construct the virtual path for the .tif file
            tif_path = f"{data_folder}/{tif_file}"
            response = supabase.storage.from_(bucket_name).download(tif_path)
            tif_data = io.BytesIO(response)
            img_bytes = tif_to_png(tif_data, scale)
            return send_file(img_bytes, mimetype='image/png')
        
    abort(404)  # No matching DOY found

@app.route('/feature/<feature_id>/temperature/<doy>')
def get_temperature_by_doy(feature_id, doy):
    data_folder = f"ECO/{feature_id}/lake"
    try:    
        files = supabase.storage.from_(bucket_name).list(data_folder)
        csv_files = [file['name'] for file in files if file['name'].endswith('.csv')]
    except Exception as e:
        csv_files = []
        print("Error fetching .tif files:", e)

    for csv_file in csv_files:
        metadata = extract_metadata(csv_file)
        if metadata[1] == doy:
            # Construct the virtual path for the .csv file
            csv_path = f"{data_folder}/{csv_file}"
            try:
                response = supabase.storage.from_(bucket_name).download(csv_path)
                csv_data = response.decode("utf-8")
                df = pd.read_csv(io.StringIO(csv_data))
            except Exception as e:
                print("Error reading CSV file:", e)
                return jsonify({"error": "Failed to read CSV file"}), 500

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
    data_folder = f"ECO/{feature_id}/lake"
    try:
        files = supabase.storage.from_(bucket_name).list(data_folder)
        matching_files = [file['name'] for file in files if file['name'].endswith('.tif') and "_wtoff" in file['name'] and date in file['name']]
    except Exception as e:
        print("Error fetching .tif files:", e)
        return jsonify({"error": "Failed to fetch files"}), 500

    if matching_files:
        return jsonify({"wtoff": False, "files": matching_files})
    else:
        return jsonify({"wtoff": True})

@app.route('/download_tif/<feature_id>/<filename>')
def download_tif(feature_id, filename):
    data_folder = f"ECO/{feature_id}/lake"
    file_path = f"{data_folder}/{filename}"
    
    try:
        # Download the file from Supabase storage
        response = supabase.storage.from_(bucket_name).download(file_path)
        tif_data = io.BytesIO(response)
        tif_data.seek(0)  # Ensure the file pointer is at the beginning
        return send_file(tif_data, as_attachment=True, download_name=filename, mimetype='application/octet-stream')
    except Exception as e:
        print("Error downloading .tif file:", e)
        abort(404)

@app.route('/download_csv/<feature_id>/<filename>')
def download_csv(feature_id, filename):
    filename = filename.replace(".tif", ".csv")  # Change the file extension to .csv
    data_folder = f"ECO/{feature_id}/lake"
    file_path = f"{data_folder}/{filename}"

    try:
        # Download the file from Supabase storage
        response = supabase.storage.from_(bucket_name).download(file_path)
        csv_data = io.BytesIO(response)
        csv_data.seek(0)  # Ensure the file pointer is at the beginning
        return send_file(csv_data, as_attachment=True, download_name=filename, mimetype='application/octet-stream')
    except Exception as e:
        print("Error downloading .tif file:", e)
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

# Consolidated function to convert .tif to .png with selectable color scale
def tif_to_png(tif_path, color_scale="relative"):
    """
    Converts a .tif file to a .png image using different processing methods
    based on the selected color scale.

    Parameters:
        tif_path (str): Path to the .tif file.
        color_scale (str): Color scale to use ("relative", "hard", "grayscale").
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

        if color_scale == "hard":
            # Hardscale output
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

        elif color_scale == "grayscale":
            # Grayscale output
            bands = [dataset.read(band) for band in range(1, num_bands + 1)]  # Read all bands
            norm_bands, alpha_mask = zip(*[normalize(band) for band in bands])  # Normalize each band
            img_array = np.stack([norm_bands[0], norm_bands[0], norm_bands[0]], axis=-1)  # Grayscale RGB
            img_array = np.dstack((img_array, alpha_mask[0]))  # Add transparency channel
            rgba_img = img_array

        else:
            raise ValueError(f"Invalid color_scale: {color_scale}. Choose 'relative', 'hard', or 'grayscale'.")

        # Save as PNG
        img = Image.fromarray(rgba_img, mode="RGBA")
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)

    return img_bytes

if __name__ == "__main__":
    app.run(debug=True)