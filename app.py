from flask import Flask, render_template, send_file, abort, make_response, url_for
import os
from PIL import Image
import io

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
    
    # Pass feature_id and tif file names to the template
    return render_template('feature_map.html', feature_id=feature_id, tif_files=tif_files)

@app.route('/tif_image/<feature_id>/<filename>')
def serve_tif_as_png(feature_id, filename):
    # Serve the .tif file as a PNG image
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECOraw', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)
    
    if not os.path.exists(file_path):
        print("File not found.")  # Debugging output
        abort(404)
    
    # Convert .tif to .png
    with Image.open(file_path) as img:
        img = img.convert('RGB')  # Convert if necessary
        img_bytes = io.BytesIO()
        img.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        
    response = make_response(img_bytes.read())
    response.headers.set('Content-Type', 'image/png')
    return response

@app.route('/download_tif/<feature_id>/<filename>')
def download_tif(feature_id, filename):
    # Serve the original .tif file as a download
    data_folder = os.path.join(root_folder, 'Water Temp Sensors', 'ECOraw', feature_id, 'lake')
    file_path = os.path.join(data_folder, filename)
    
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        abort(404)

if __name__ == "__main__":
    app.run(debug=True)
