import os
import time
import requests
import geopandas as gpd
import rasterio
import json
from rasterio.merge import merge
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# Directory paths
print("Setting Directory Paths")
pt = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors/ECOraw/"
output_path = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors/ECO/"
roi_path = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors/polygon/site_full_ext.shp"

# Define Earthdata login credentials (Replace with your actual credentials)
user = 'JephthaT'
password = '1#Big_Chilli'

# Get token (API login via requests)
def get_token(user, password):

    #Authenticates with Earthdata and retrieves an authentication token.

    try:
        response = requests.post('https://appeears.earthdatacloud.nasa.gov/api/login', auth=(user, password))
        response.raise_for_status()
        token_response = response.json()
        return token_response
    except requests.exceptions.HTTPError as err:
        raise SystemExit(f"Authentication failed: {err}")

token = get_token(user, password)
token_new = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImplcGh0aGF0IiwiZXhwIjoxNzM0MDAyNTMyLCJpYXQiOjE3Mjg4MTg1MzIsImlzcyI6Imh0dHBzOi8vdXJzLmVhcnRoZGF0YS5uYXNhLmdvdiJ9.sly-XS_g6K44wo0JKJ4quriQzVdfPOJsRSavYhww7z7OFttzSdUHeMwDBmezZhLnrk6YiNiSAWtogqRyU8zJSwamMo2ACfTxyoeRZ9EQ_5qtfOptDVUZqww26f95Rrsz58ygLG5tmRlZbUSmXXgLk9fyshuftduyMi6L34LcJrX10HkthgRKUWwVz8NTkoPboHAxGDPQlcKfKeAdN40Q7GWe4sOMeDdYA1AaF0ZeQAxG4aAr0z-7a3rtNwdQa5MvoPIsXMpqaIxM8BLZm82Yu8Wt79PH9Rvt14GnJGZ8LKMpYU8AWxKTmKg7liAw5R6THnOpwsFJxWc2fo5h6eBLNg"

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
    "Authorization": f"Bearer {token['token']}",
    "Content-Type": "application/json"
}
layers = ["LST", "LST_err"]

# Load the area of interest (ROI)
print("Loading Regions of Interest")
roi = gpd.read_file(roi_path)
roi_json = roi.__geo_interface__  # Convert ROI to GeoJSON


def build_task_request(product, layers, roi, sd, ed):
    # Prepare the request payload
    task = {
        "task_type": "area",
        "task_name": "ECOStress_Request",
        "params": {
            "dates": [{"startDate": sd, "endDate": ed}],
            "layers": [{"product": product, "layer": layer} for layer in layers],
            "geo": roi_json,  # Using GeoJSON for the region
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
    print(task_id)
    url = f"https://lpdaacsvc.cr.usgs.gov/appeears/api/task/{task_id}"
    while True:
        response = requests.get(url, headers=headers)
        status = response.json()["status"]
        if status == "done":
            print(f"Task {task_id} is complete!")
            break
        elif status == "running":
            print(f"Task {task_id} is still running. Checking again in 30 seconds...")
            time.sleep(30)
        else:
            raise Exception(f"Task failed with status: {status}")

# Function to download results from AppEEARS
def download_results(task_id, output_path, headers):
    url = f"https://lpdaacsvc.cr.usgs.gov/appeears/api/task/{task_id}/bundle"
    response = requests.get(url, headers=headers)
    files = response.json()['files']
    for file in files:
        download_url = file['fileURL']
        local_filename = os.path.join(output_path, file['fileName'])
        with requests.get(download_url, stream=True) as r:
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
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

# Loop through the ROI
for idx, row in roi.iterrows():
    # Create bounding box for ROI
    roi_bbox = row.geometry.bounds
    roi2 = gpd.GeoSeries([row.geometry], crs=roi.crs)
    print(f"Processing ROI {idx + 1}/{len(roi)}")

    roi_geometry = gpd.GeoSeries([row.geometry], crs=roi.crs).__geo_interface__

    # Build and submit task for the current ROI
    task_request = build_task_request(product, layers, roi_geometry, sd, ed)
    task_id = submit_task(headers, task_request)

    # Check task status and wait for completion
    check_task_status(task_id, headers)
    # Construct directory path for saving data
    output_folder = os.path.join(pt, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder created: {output_folder}")
    # Download the results for the current ROI
    download_results(task_id, output_folder, headers)

    # Process the downloaded rasters
    process_rasters(output_folder)
    print("All ROIs Processed")


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
    bdf.to_csv(output_path + '_filter.csv', index=False)
    # Rebuild raster from filtered data
    with rasterio.open(output_path + '_filter.tif', 'w', **meta) as dest:
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