from flask import Flask, render_template, send_file, abort
import os
import re
import glob

app = Flask(__name__)

# Define an absolute path to the external data directory
root_folder = r"C:\Users\Abdullah Usmani\Documents\Uni\y2\2019 (SEGP)\\"

@app.route('/')
def index():
    # Main map showing all polygons
    return render_template('index.html')

@app.route('/feature/<feature_id>')
def feature_page(feature_id):
    # Directory containing .tif files for the feature
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECOraw', feature_id, 'lake')
    
    if not os.path.isdir(data_folder):
        abort(404)
    
    # Fetch .tif file names in the directory
    tif_files = [f for f in os.listdir(data_folder) if f.endswith('.tif')]
    
    return render_template('feature_map.html', feature_id=feature_id, tif_files=tif_files)


@app.route('/tif/<feature_id>/<filename>')
def serve_tif(feature_id, filename):
    # Serve the specified .tif file for the given feature_id
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECOraw', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)
    
    print(f"Serving file from path: {file_path}")  # Debugging output

    if os.path.exists(file_path):
        return send_file(file_path, mimetype='image/tiff')
    else:
        print("File not found.")  # Debugging output
        abort(404)

if __name__ == "__main__":
    app.run(debug=True)
