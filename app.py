from flask import Flask, render_template, send_file, abort, make_response
import os
import io
import numpy as np
import rasterio
from rasterio.plot import reshape_as_image
from PIL import Image

app = Flask(__name__)

# Define the external data directory
root_folder = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\\"


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/feature/<feature_id>')
def feature_page(feature_id):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    
    if not os.path.isdir(data_folder):
        abort(404)

    tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif')]
    
    return render_template('feature_map.html', feature_id=feature_id, tif_files=tif_files)


@app.route('/tif_image/<feature_id>/<filename>')
def serve_tif_as_png(feature_id, filename):
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECO', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)

    if not os.path.exists(file_path):
        print("File not found.")  # Debugging output
        abort(404)

    with rasterio.open(file_path) as dataset:
        num_bands = dataset.count
        print(f"Number of bands: {num_bands}")  # Debugging output

        bands = [dataset.read(1)]  # Read the first band (assuming grayscale)

        norm_bands = []
        for band in bands:
            min_val = np.nanmin(band)  # Use np.nanmin to ignore NaN values
            max_val = np.nanmax(band)

            print(f"Min: {min_val}, Max: {max_val}")  # Debugging output

            if np.isnan(min_val) or np.isnan(max_val):
                print("NaN values detected!")  # Debugging output
                norm_band = np.zeros_like(band, dtype=np.uint8)  # Set to black
            elif max_val - min_val == 0:
                print("Constant image detected!")  # Debugging output
                norm_band = np.zeros_like(band, dtype=np.uint8)  # Set to black
            else:
                norm_band = ((band - min_val) / (max_val - min_val) * 255).astype(np.uint8)

            norm_bands.append(norm_band)

        # Stack grayscale as RGB if necessary
        img_array = np.stack([norm_bands[0]] * 3, axis=-1)  # Convert grayscale to RGB

        # Convert to image format and return response
        img = Image.fromarray(img_array)
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)

    return send_file(img_bytes, mimetype="image/png")

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
