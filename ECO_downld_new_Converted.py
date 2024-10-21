import os
import time
import requests
import json
import geopandas as gpd
import rasterio
import numpy as np
from datetime import datetime, timedelta

# Directory paths
print("Setting Directory Paths")
pt = r"C:\Users\Abdullah Usmani\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/ECOraw/"
output_path = r"C:\Users\Abdullah Usmani\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/ECO/"
roi_path = r"C:\Users\Abdullah Usmani\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/polygon/test/site_full_ext_Test.shp"

if not os.path.exists(roi_path):
    raise FileNotFoundError(f"The ROI shapefile does not exist at {roi_path}")

try:
    roi = gpd.read_file(roi_path)
except Exception as e:
    raise ValueError(f"Could not read the shapefile: {e}")

# Define Earthdata login credentials (Replace with your actual credentials)
user = 'abdullahusmani1'
password = 'haziqLOVERS123!'

# Get token (API login via requests)
def get_token(user, password):
    try:
        response = requests.post('https://appeears.earthdatacloud.nasa.gov/api/login', auth=(user, password))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(f"Authentication failed: {err}")

token = get_token(user, password)

# Get today and yesterday's date for start/end dates
today_date = datetime.now()
today_date_str = today_date.strftime("%m-%d-%Y")
yesterday_date_str = (today_date - timedelta(days=1)).strftime("%m-%d-%Y")
sd, ed = yesterday_date_str, today_date_str

# Products, headers, and layers
product = "ECO_L2T_LSTE.002"
headers = {
    "Authorization": f"Bearer {token['token']}",
    "Content-Type": "application/json"
}
layers = ["LST", "LST_err"]

# Load the area of interest (ROI)
roi_json = roi.__geo_interface__  # Convert ROI to GeoJSON

# Function to build task request
def build_task_request(product, layers, roi, sd, ed):
    return {
        "task_type": "area",
        "task_name": "ECOStress_Request",
        "params": {
            "dates": [{"startDate": sd, "endDate": ed}],
            "layers": [{"product": product, "layer": layer} for layer in layers],
            "geo": roi,
            "output": {"format": {"type": "geotiff"}, "projection": "geographic"}
        }
    }

# Submit the task request
def submit_task(headers, task_request):
    response = requests.post('https://appeears.earthdatacloud.nasa.gov/api/task', json=task_request, headers=headers)
    if response.status_code == 202:
        return response.json()["task_id"]
    else:
        raise Exception(f"Task submission failed: {response.status_code}")

# Check the status of a task
def check_task_status(task_id, headers):
    url = f"https://appeears.earthdatacloud.nasa.gov/api/task/{task_id}"
    while True:
        response = requests.get(url, headers=headers)
        status = response.json().get("status", "")
        if status == "done":
            return "done"
        elif status in ["processing", "queued"]:
            time.sleep(30)
        else:
            raise Exception(f"Task {task_id} failed with status: {status}")

# Download results from AppEEARS
def download_results(task_id, output_path, headers):
    url = f"https://appeears.earthdatacloud.nasa.gov/api/bundle/{task_id}"
    response = requests.get(url, headers=headers)
    files = response.json()['files']
    for file in files:
        file_id = file['file_id']
        local_filename = os.path.join(output_path, file['file_name'])
        download_url = f"{url}/{file_id}"
        with requests.get(download_url, stream=True) as r:
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded {local_filename}")

# Process raster files
def process_rasters(output_folder):
    raster_files = [os.path.join(output_folder, f) for f in os.listdir(output_folder) if f.endswith('.tif')]
    for tif_file in raster_files:
        with rasterio.open(tif_file) as src:
            lst = src.read(1)
            lst_filtered = np.where(lst == -9999, np.nan, lst)
            filtered_file = tif_file.replace(".tif", "_filtered.tif")
            with rasterio.open(
                filtered_file, 'w', driver='GTiff', height=lst_filtered.shape[0], width=lst_filtered.shape[1],
                count=1, dtype='float32', crs=src.crs, transform=src.transform
            ) as dst:
                dst.write(lst_filtered, 1)
            print(f"Filtered raster saved: {filtered_file}")

# Phase 1: Submit all tasks
task_ids = []
for idx, row in roi.iterrows():
    roi_geometry = gpd.GeoSeries([row.geometry], crs=roi.crs).__geo_interface__
    task_request = build_task_request(product, layers, roi_geometry, sd, ed)
    task_id = submit_task(headers, task_request)
    output_folder = os.path.join(pt, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    task_ids.append((task_id, output_folder))

print(f"All {len(task_ids)} tasks submitted!")

# Phase 2: Check statuses and download results
completed_tasks = []  # Track completed tasks
total_tasks = len(task_ids)  # Total number of tasks to complete

while len(completed_tasks) < total_tasks:
    print(f"Checking task statuses... {len(completed_tasks)}/{total_tasks} tasks completed.")
    
    for task_id, output_folder in task_ids:
        if task_id not in completed_tasks:  # Check only tasks that aren't yet completed
            try:
                status = check_task_status(task_id, headers)
                if status == "done":
                    download_results(task_id, output_folder, headers)
                    process_rasters(output_folder)
                    completed_tasks.append(task_id)  # Mark the task as completed
                    print(f"Task {task_id} completed and processed.")
            except Exception as e:
                print(f"Error processing task {task_id}: {e}")
    
    # Print progress
    print(f"{len(completed_tasks)}/{total_tasks} tasks completed.")
    
    if len(completed_tasks) < total_tasks:
        print(f"Waiting for 30 seconds before the next check...")
        time.sleep(30)

print("All tasks completed and processed.")

