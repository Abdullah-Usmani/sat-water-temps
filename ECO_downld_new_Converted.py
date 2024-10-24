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

# Get token (API login via r)
def get_token(user, password):

    #Authenticates with Earthdata and retrieves an authentication token.
    try:
        response = requests.post('https://appeears.earthdatacloud.nasa.gov/api/login', auth=(user, password))
        response.raise_for_status()
        token_response = response.json()
        return token_response['token']
    except requests.exceptions.HTTPError as err:
        raise SystemExit(f"Authentication failed: {err}")

token = get_token(user, password)
print(token)

# Get Today Date As End Date
print("Setting Dates")
today_date = datetime.now()
today_date_str = today_date.strftime("%m-%d-%Y")
ed = today_date_str

# Get Yesterday Date as Start Date
yesterday_date = today_date - timedelta(days=1)
yesterday_date_str = yesterday_date.strftime("%m-%d-%Y")
sd = yesterday_date_str

# Products, Headers and layers
product = "ECO_L2T_LSTE.002"
headers = {
    'Authorization': f'Bearer {token}'
}
layers = ["LST", "LST_err"]

# Load the area of interest (ROI)
print("Loading Regions of Interest")
roi = gpd.read_file(roi_path)
roi_json = roi.__geo_interface__  # Convert ROI to GeoJSON


def build_task_request(product, layers, roi_json, sd, ed):
    # Prepare the request payload
    task = {
        "task_type": "area",
        "task_name": "ECOStress_Request",
        "params": {
            "dates": [{"startDate": sd, "endDate": ed}],
            "layers": [{"product": product, "layer": layer} for layer in layers],
            "geo": roi_json,  # Use the properly formatted roi
            "output": {
                "format": {"type": "geotiff"},
                "projection": "geographic"
            }
        }
    }
    return task

# Function to submit the task request to AppEEARS
def submit_task(headers, task_request):
    url = 'https://appeears.earthdatacloud.nasa.gov/api/task'
    response = requests.post(url, json=task_request, headers=headers)
    if response.status_code == 202:  # Task accepted
        print("Task submitted successfully!")
        return response.json()["task_id"]
    else:
        print(f"Task submission failed: {response.status_code}")
        print(response.text)  # Print detailed error message
        raise Exception(f"Task submission failed: {response.status_code}")

# Function to check the task status
def check_task_status(task_id, headers):
    url = f"https://appeears.earthdatacloud.nasa.gov/api/task/{task_id}"
    while True:
        response = requests.get(url, headers=headers)
        status = response.json()["status"]
        doneFlag = False
        if status == "done":
            print(f"Task {task_id} is complete!")
            doneFlag = True
            break
        elif status == "processing":
            print(f"Task {task_id} is still processing. Checking again in 30 seconds...")
            time.sleep(30)
        elif status == "queued":
            print(f"Task {task_id} is still queued. Checking again in 30 seconds...")
            time.sleep(30)
        else:
            raise Exception(f"Task failed with status: {status}")
    return doneFlag

# Function to download results from AppEEARS
def download_results(task_id, output_path, headers):
    url = f"https://appeears.earthdatacloud.nasa.gov/api/bundle/{task_id}"
    response = requests.get(url, headers=headers)
    files = response.json()['files']
    for file in files:
        file_id = file['file_id']
        local_filename = os.path.join(output_path, file['file_name'])
        download_url = f"{url}/{file_id}"
        download_response = requests.get(download_url, headers=headers, stream=True, allow_redirects="True")
        with open(local_filename, 'wb') as f:
            for chunk in download_response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded {local_filename}")

# Function to process the raster files
def process_rasters(output_folder):
    raster_files = [os.path.join(output_folder, f) for f in os.listdir(output_folder) if f.endswith('.tif')]

    for tif_file in raster_files:
        with rasterio.open(tif_file) as src:
            lst = src.read(1)  # Load LST layer
            lst_filtered = np.where(lst == -9999, np.nan, lst)  # Replace nodata with NaN

            # Save the filtered raster back
            filtered_file = tif_file.replace(".tif", "_filtered.tif")
            with rasterio.open(
                filtered_file,
                'w',
                driver='GTiff',
                height=lst_filtered.shape[0],
                width=lst_filtered.shape[1],
                count=1,
                dtype='float32',
                crs=src.crs,
                transform=src.transform
            ) as dst:
                dst.write(lst_filtered, 1)
                print(f"Filtered raster saved: {filtered_file}")

# Phase 1: Submit all tasks first
task_ids = []  # List to hold task_ids and corresponding output folders

for idx, row in roi.iterrows():
    # Create bounding box for ROI
    roi_bbox = row.geometry.bounds
    roi2 = gpd.GeoSeries([row.geometry], crs=roi.crs)
    print(f"Processing ROI {idx + 1}/{len(roi)}")

    roi_geometry = gpd.GeoSeries([row.geometry], crs=roi.crs).__geo_interface__

    # Build and submit task for the current ROI
    task_request = build_task_request(product, layers, roi_json, sd, ed)
    task_id = submit_task(headers, task_request)
    print(f"Task ID: {task_id}")
    
    # Construct directory path for saving data
    output_folder = os.path.join(pt, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder created: {output_folder}")
    
    # Store the task_id and its corresponding folder for later use
    task_ids.append((task_id, output_folder))

print(f"All {len(task_ids)} tasks submitted!")

# Phase 2: Check task statuses periodically (every 30 seconds)
completed_tasks = []

while len(completed_tasks) < len(task_ids):
    print("Checking task statuses...")
    for task_id, output_folder in task_ids:
        if task_id in completed_tasks:
            continue
         
        # loop when unprocessed task, doesnt want
        
        # Check task status
        status = check_task_status(task_id, headers)
        
        if status == True:  # Replace "done" with the actual status string for completion
            print(f"Downloading results for Task ID: {task_id}...")
            
            # Download the results for the current ROI
            download_results(task_id, output_folder, headers)
            
            # Process the downloaded rasters
            process_rasters(output_folder)
            print(f"Rasters processed for Task ID: {task_id}")
            
            completed_tasks.append(task_id)
    
    if len(completed_tasks) < len(task_ids):
        print(f"{len(completed_tasks)}/{len(task_ids)} tasks completed. Waiting for 30 seconds...")
        time.sleep(30)  # Wait for 30 seconds before checking statuses again

print("All tasks completed, results downloaded, and rasters processed.")



"""
# Adjust and filter data
def filter_data(bdf):
    # Quality control filter (QC)
    qc_filter_values = [15, 2501, 3525, 65535]
    bdf['LST_filter'] = np.where(bdf['QC'].isin(qc_filter_values), np.nan, bdf['LST'])
    if 'LST_err_filter' not in bdf.columns:
        print("Column 'LST_err_filter' does not exist in the DataFrame.")
    else:
        bdf['LST_err_filter'] = np.where(bdf['QC'].isin(qc_filter_values), np.nan, bdf['LST_err'])
    if 'emis_filter' not in bdf.columns:
        print("Column 'emis_filter' does not exist in the DataFrame.")
    else:
        bdf['emis_filter'] = np.where(bdf['QC'].isin(qc_filter_values), np.nan, bdf['emis'])
    if 'heig_filter' not in bdf.columns:
        print("Column 'heig_filter' does not exist in the DataFrame.")
    else:
        bdf['heig_filter'] = np.where(bdf['QC'].isin(qc_filter_values), np.nan, bdf['height'])

    # Cloud filter
    bdf['LST_filter'] = np.where(bdf['cloud'] == 1, np.nan, bdf['LST_filter'])
    bdf['LST_err_filter'] = np.where(bdf['cloud'] == 1, np.nan, bdf['LST_err_filter'])

    # Water filter
    bdf['LST_filter'] = np.where(bdf['wt'] == 0, np.nan, bdf['LST_filter'])
    print(bdf.columns)
    return bdf

# Save filtered data as GeoTIFF and CSV
def save_filtered_data(bdf, output_path):
    bdf.to_csv(output_path + '_filterequests.csv', index=False)
    # Rebuild raster from filtered data
    with rasterio.open(output_path + '_filterequests.tif', 'w', **meta) as dest:
        dest.write(bdf['LST_filter'].values.reshape(shape), 1)

# Example usage
bdf = pd.DataFrame({
    'LST': [1, 2, 3],
    'QC': [15, 0, 3525],
    'cloud': [0, 1, 0],
    'wt': [1, 1, 0]
})

filtered_bdf = filter_data(bdf)
save_filtered_data(filtered_bdf, "output_path")
"""