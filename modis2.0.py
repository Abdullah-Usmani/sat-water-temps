import json
import os
import re
import time
import requests
import pandas as pd
import geopandas as gpd
import rasterio
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# Directory paths
print("Setting Directory Paths")
raw_path = r"/Users/ssj/Desktop/SatelliteRetrievalProject/MODISraw/"
filtered_path = r"/Users/ssj/Desktop/SatelliteRetrievalProject/MODIS/"
roi_path = r"/Users/ssj/Desktop/SatelliteRetrievalProject/polygon/site_full_ext_Corrected.shp"
log_path = r"/Users/ssj/Desktop/SatelliteRetrievalProject/logs/"
download_log = os.path.join(log_path, "downloaded_files.json")
log_file = os.path.join(log_path, f"modis_retrieval_{datetime.now().strftime('%Y%m%d_%H%M')}.log")

# Ensure log directory exists
os.makedirs(log_path, exist_ok=True)
os.makedirs(raw_path, exist_ok=True)
os.makedirs(filtered_path, exist_ok=True)

# Logging function
def log_message(message):
    with open(log_file, "a") as log:
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
    print(message)

# Load previously downloaded files
def load_downloaded_files():
    if os.path.exists(download_log):
        with open(download_log, "r") as f:
            return json.load(f)
    return {}

downloaded_files = load_downloaded_files()

# Save updated download log
def save_downloaded_files():
    with open(download_log, "w") as f:
        json.dump(downloaded_files, f, indent=4)

# Get token (API login via requests)
def get_token(user, password):
    try:
        response = requests.post('https://appeears.earthdatacloud.nasa.gov/api/login', auth=(user, password))
        response.raise_for_status()
        token_response = response.json()
        log_message("Successfully obtained API token.")
        return token_response['token']
    except requests.exceptions.HTTPError as err:
        log_message(f"Authentication failed: {err}")
        raise SystemExit(f"Authentication failed: {err}")

if not os.path.exists(roi_path):
    log_message(f"The ROI shapefile does not exist at {roi_path}")
    raise FileNotFoundError(f"The ROI shapefile does not exist at {roi_path}")

try:
    roi = gpd.read_file(roi_path)
    log_message("Successfully loaded ROI shapefile.")
except Exception as e:
    log_message(f"Could not read the shapefile: {e}")
    raise ValueError(f"Could not read the shapefile: {e}")

# Define Earthdata login credentials (Replace with actual credentials)
user = 'syuk2'
password = 'xopmew-Ciwxug-merzy9'

# Generate a timestamp for uniqueness in filenames
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# Set date range
log_message("Setting Dates")
today_date = datetime.now()
end_date = today_date - timedelta(days=1)
start_date = today_date - timedelta(days=8)
ed = "03-02-2025"
sd = "02-26-2025"

# MODIS-specific settings
token = get_token(user, password)
product = "MYD09A1.061"  # MODIS Surface Reflectance 8-day L3 Global 500m
headers = {
    'Authorization': f'Bearer {token}'
}

# KEY RESULTS TO STORE/LOG
updated_aids = set()
new_files = []
new_dates = []
multi_aids = set()
multi_files = []
aid_folder_mapping = {}

# MODIS Bands for Water Quality Analysis
layers = [
    "sur_refl_b01",  # Red - Turbidity, TSS
    "sur_refl_b02",  # NIR - NDWI, TSS
    "sur_refl_b03",  # Blue - Chlorophyll-a
    "sur_refl_b04",  # Green - Chlorophyll-a
    "sur_refl_b05",  # SWIR - Water masking
    "sur_refl_qc_500m"  # Quality Control for cloud/water masking
]

log_message("Loading Regions of Interest")
roi_json = roi.__geo_interface__  # Convert ROI to GeoJSON

# Function to build the task request
def build_task_request(product, layers, roi_json, sd, ed):
    task = {
        "task_type": "area",
        "task_name": "MODIS_WaterQuality_Request",
        "params": {
            "dates": [{"startDate": sd, "endDate": ed}],
            "layers": [{"product": product, "layer": layer} for layer in layers],
            "geo": roi_json,
            "output": {
                "format": {"type": "geotiff"},
                "projection": "geographic"
            }
        }
    }
    log_message("Task request built successfully.")
    return task

# Function to track and organize downloaded files
def track_downloaded_file(file_name, date):
    if date not in downloaded_files:
        downloaded_files[date] = []
    if file_name not in downloaded_files[date]:
        downloaded_files[date].append(file_name)
    save_downloaded_files()
    log_message(f"Tracked file: {file_name} for date {date}")

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
            log_message(f"Task {task_id} is complete!")
            doneFlag = True
            break
        elif status in ["processing", "queued", "pending"]:
            log_message(f"Task {task_id} is still {status}. Checking again in 30 seconds...")
            time.sleep(30)
        else:
            raise Exception(f"Task failed with status: {status}")
    return doneFlag

# Function to download results from AppEEARS
def download_results(task_id, headers):
    url = f"https://appeears.earthdatacloud.nasa.gov/api/bundle/{task_id}"
    response = requests.get(url, headers=headers)
    files = response.json()['files']

   # Step 1: Download files and group by aid
    for file in files:
        file_id = file['file_id']
        file_name = file['file_name']
        aid_match = re.search(r'aid(\d{4})', file_name)  # Extract aid number from filename

        if aid_match:
            aid_number = extract_metadata(file_name)[0] 
            updated_aids.add(aid_number)  # Track updated aids
            name, location = aid_folder_mapping.get(aid_number, (None, None))
            if name is None or location is None:
                print(f"No mapping found for AID: {aid_number}, skipping...")
                continue
            output_folder = os.path.join(raw_path, name, location)
            
            if output_folder is not None:
                # Ensure output folder exists and strip preceding folder in file_name if present
                os.makedirs(output_folder, exist_ok=True)
                file_name_stripped = file_name.split('/')[-1]
                local_filename = os.path.join(output_folder, file_name_stripped)

                # print(f"Downloading to: {local_filename}")
                download_url = f"{url}/{file_id}"
                download_response = requests.get(download_url, headers=headers, stream=True, allow_redirects=True)
                
                # Save the file locally and add it to the new_files list
                with open(local_filename, 'wb') as f:
                    for chunk in download_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                new_files.append(local_filename)  # Track newly downloaded file
                print(f"Downloaded {local_filename}")

        else:
            # Handle general files without aid numbers (e.g., XML, CSV, JSON)
            base_name, ext = os.path.splitext(file_name)
            new_file_name = f"{base_name}_{timestamp}{ext}"  # Append timestamp before extension
            local_filename = os.path.join(raw_path, new_file_name)  # Save directly to the base folder

            # print(f"Downloading to base folder: {local_filename}")

            download_url = f"{url}/{file_id}"
            download_response = requests.get(download_url, headers=headers, stream=True, allow_redirects=True)

            with open(local_filename, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    f.write(chunk)

            new_files.append(local_filename)  # Track newly downloaded file
            print(f"Downloaded {local_filename}")

# Function to extract aid number and date from filename
def extract_metadata(filename):
    aid_match = re.search(r'aid(\d{4})', filename)
    date_match = re.search(r'doy(\d{13})', filename)

    aid_number = int(aid_match.group(1)) if aid_match else None
    date = date_match.group(1) if date_match else None

    return aid_number, date

# Function to filter only new folders and return unique folders
def get_updated_folders(new_files):
    return {extract_metadata(f)[0] for f in new_files if extract_metadata(f)[0]}

# Function to filter only new files and return unique dates
def get_updated_dates(new_files):
    return {extract_metadata(f)[1] for f in new_files if extract_metadata(f)[1]}

# Function to read a specific raster layer
def read_raster(layer_name, relevant_files):
    matches = [f for f in relevant_files if layer_name in f]
    return rasterio.open(matches[0]) if matches else None

# Function to read raster as NumPy array
def read_array(raster):
    return raster.read(1) if raster else None

# Function to process a single date
def process_rasters(aid_number, date, selected_files):
    print(f"Processing date: {date} for aid: {aid_number}")
    relevant_files = [f for f in selected_files if date in f]
    water_mask_flag = True
    if not relevant_files:
        print(f"No files found for date: {date}")
        return

    # Read raster layers
    Red = read_raster("sur_refl_b01", relevant_files)
    NIR = read_raster("sur_refl_b02", relevant_files)
    Blue = read_raster("sur_refl_b03", relevant_files)
    Green = read_raster("sur_refl_b04", relevant_files)
    SWIR = read_raster("sur_refl_b05", relevant_files)
    QC = read_raster("sur_refl_qc_500m", relevant_files)

    if None in [Red, NIR, Blue, Green, SWIR, QC]:
        print(f"Skipping {date} due to missing layers.")
        return

    # Read raster data into NumPy arrays
    arrays = {key: read_array(layer) for key, layer in {
        "Red": Red, "NIR": NIR, "Blue": Blue, "Green": Green, "SWIR": SWIR, "QC": QC
    }.items()}

     # Get AID number
    name, location = aid_folder_mapping.get(aid_number, (None, None))
    if not name or not location:
        print(f"No mapping found for AID: {aid_number}, skipping...")
        return
    
     # Define destination folder
    dest_folder_raw = os.path.join(raw_path, name, location)
    dest_folder_filtered = os.path.join(filtered_path, name, location)
    os.makedirs(dest_folder_raw, exist_ok=True)
    os.makedirs(dest_folder_filtered, exist_ok=True)

    raw_tif_path = os.path.join(dest_folder_raw, f"{name}_{location}_{date}_raw.tif")
    # Update metadata to match the number of bands
    raw_meta = Red.meta.copy()
    raw_meta.update(dtype=rasterio.float32, count=len(arrays))  # Ensure correct band count

    # Open the new TIF file with multiple bands
    with rasterio.open(raw_tif_path, "w", **raw_meta) as dst:
        for idx, (key, data) in enumerate(arrays.items(), start=1):
         dst.write(data, idx)  # Write each band correctly

    print(f"Saved raw raster: {raw_tif_path}")

    # Convert raster data to DataFrame
    rows, cols = arrays["Red"].shape  # Use an available key like 'Red'
    x, y = np.meshgrid(np.arange(cols), np.arange(rows))

    df = pd.DataFrame({
        "x": x.flatten(),
        "y": y.flatten(),
        **{key: arr.flatten() for key, arr in arrays.items()}
    })

    # Save raw CSV
    raw_csv_path = os.path.join(dest_folder_raw, f"{name}_{location}_{date}_raw.csv")
    df.to_csv(raw_csv_path, index=False)
    print(f"Saved raw CSV: {raw_csv_path}")

    filter_csv_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter.csv")
    filter_tif_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter.tif")

    water_mask_flag = (df["wt"] == 1).any()
    if not water_mask_flag:
        print(f"Skipping {date} due to no water mask.")
        return
    
    # Define valid range for surface reflectance bands
    VALID_MIN = -100
    VALID_MAX = 16000
    SCALE_FACTOR = 0.0001

    # Function to check valid pixel values
    def filter_invalid_values(df, bands):
        for band in bands:
            df[f"{band}_filtered"] = np.where(
                (df[band] >= VALID_MIN) & (df[band] <= VALID_MAX),
                df[band] * SCALE_FACTOR,  # Apply scale factor
                np.nan  # Set invalid values to NaN
            )
        return df

    # Function to filter based on QC flags using bit masking
    def filter_qc_flags(df):
        def is_valid_qc(qc_value):
            qc_bits = format(qc_value, "032b")  # Convert QC flag to 32-bit binary
            MODLAND_QA = qc_bits[-2:]  # Bits 0-1 (ideal quality: '00' or '01')
            return MODLAND_QA in ["00", "01"]  # Keep high-quality pixels

        df["QC_filtered"] = df["sur_refl_qc_500m"].apply(lambda x: x if is_valid_qc(x) else np.nan)
        return df

    # Function to apply cloud masking
    def apply_cloud_mask(df):
        df["cloud_mask"] = df["sur_refl_state_500m"] & 0b11  # Extract bits 0-1
        for band in ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05"]:
            df.loc[df["cloud_mask"] > 0, f"{band}_filtered"] = np.nan  # Mask only reflectance bands
        return df

    # Process dataset
    bands = ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05"]
    df = filter_invalid_values(df, bands)
    df = filter_qc_flags(df)
    df = apply_cloud_mask(df)

    # Save filtered CSV
    filter_csv_path = os.path.join(filtered_path, f"{name}_{location}_{date}_filtered.csv")
    df.to_csv(filter_csv_path, index=False)
    print(f"Saved filtered CSV: {filter_csv_path}")

    # Convert filtered data back to raster
    def create_raster(data, reference_raster):
        """Reshape filtered data into raster format using reference metadata."""
       # rows, cols = reference_raster.shape
        meta = reference_raster.meta.copy()
        meta.update(dtype=rasterio.float32, count=1)  # Single-band raster
        return data.reshape(rows, cols).astype(np.float32), meta

    # Define filtered raster data for MODIS Surface Reflectance Bands
    filtered_rasters = {
        "sur_refl_b01": create_raster(df["sur_refl_b01_filtered"].values, Red),
        "sur_refl_b02": create_raster(df["sur_refl_b02_filtered"].values, NIR),
        "sur_refl_b03": create_raster(df["sur_refl_b03_filtered"].values, Blue),
        "sur_refl_b04": create_raster(df["sur_refl_b04_filtered"].values, Green),
        "sur_refl_b05": create_raster(df["sur_refl_b05_filtered"].values, SWIR),
        "sur_refl_qc_500m": create_raster(df["QC_filtered"].values, QC)
    }

    # Get metadata from one of the rasters
    filter_meta = filtered_rasters["sur_refl_b01"][1].copy()
    filter_meta.update(dtype=rasterio.float32, count=len(filtered_rasters))  # Update band count

    # Save the filtered raster
    output_raster_path = "filtered_MODIS_surface_reflectance.tif"

    with rasterio.open(output_raster_path, "w", **filter_meta) as dst:
        for i, (band_name, (data, _)) in enumerate(filtered_rasters.items(), start=1):
            dst.write(data, i)  # Write each filtered band
    multi_aids.add(aid_number)
    multi_files.append(filter_tif_path)

    print(f"Saved filtered MODIS raster: {output_raster_path}")
    print(f"Finished processing {date}")

   # Function to process a single dataset
def process_single_water_quality(args):
    aid_number, date, specific_date_files = args
    process_rasters(aid_number, date, specific_date_files)

# Main function to process all new water quality files using multiprocessing
def process_all_water_quality(all_new_files):
    updated_aids = get_updated_folders(all_new_files)  # Identify updated folders
    print(updated_aids)

    if not updated_aids:
        print("No new water quality data to process.")
        return

    print(f"Processing {len(updated_aids)} updated water quality datasets...")

    tasks = []

    # Collect processing tasks
    for aid_number in updated_aids:
        aid_folder_files = [file for file in all_new_files if aid_number == extract_metadata(file)[0]]

        new_dates_get = get_updated_dates(aid_folder_files)
        if not new_dates_get:
            print(f"No new water quality files to process for AID {aid_number}.")
            continue

        for date in new_dates_get:
            specific_date_files = [file for file in aid_folder_files if date == extract_metadata(file)[1]]
            tasks.append((aid_number, date, specific_date_files))

    # Use multiprocessing to process tasks in parallel
    num_workers = min(len(tasks), cpu_count())  # Use available CPUs but not exceed task count
    with Pool(processes=num_workers) as pool:
        pool.map(process_single_water_quality, tasks)

    print("Water quality processing complete.")

    ################ LINE 382 Start here ####################






