from flask import Flask, render_template
import os

app = Flask(__name__)

@app.route('/')
def index():
    # Main map showing all polygons
    return render_template('index.html')

@app.route('/feature/<feature_id>')
def feature_map(feature_id):
    # Render a separate map for each feature using its ID
    # Check if the temperature data file exists for the given feature_id
    data_path = f'static/data/temperature_{feature_id}.geojson'
    if not os.path.exists(data_path):
        return f"No temperature data found for feature ID: {feature_id}", 404
    
    return render_template('feature_map.html', feature_id=feature_id)
    
if __name__ == "__main__":
    app.run(debug=True)
