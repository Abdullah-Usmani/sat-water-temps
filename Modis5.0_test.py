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
raw_path = r"/Users/ssj/Desktop/SatelliteRetrievalProject_test/MODISraw/"
filtered_path = r"/Users/ssj/Desktop/SatelliteRetrievalProject_test/MODIS/"
roi_path = r"/Users/ssj/Desktop/SateliteRetrievalProjec/polygon/test/site_full_ext_Test.shp"
log_path = r"/Users/ssj/Desktop/SatelliteRetrievalProject_test/logs/"
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
ed = "03-05-2025"
sd = "02-26-2025"

# KEY RESULTS TO STORE/LOG
updated_aids = set()
new_files = []
new_dates = []
multi_aids = set()
multi_files = []
aid_folder_mapping = {}
 # Define valid range for surface reflectance bands
INVALID_QC_VALUES = {

    1107297155,  # Corrected product not produced
    1983120245,  # Bands 1-4 have multiple correction out-of-bounds issues
    1983120193,  # Similar issue: bands 1-4 out of bounds + dead detector
    1979712373,  # Bands 1-4 have severe quality issues
    1979712321,  # Similar: correction out of bounds for bands 1-4
    1975518069,  # Bands 1-4 constrained to extreme values
    1975518017,  # Bands 1-4 out of bounds
    1110705013,  # Bands 1-4 dead detector + out of bounds
    1110704181,  # Similar: bands 1-4 invalid
}

# MODIS-specific settings
token = get_token(user, password)
product = "MYD09A1.061"  # MODIS Surface Reflectance 8-day L3 Global 500m
headers = {
    'Authorization': f'Bearer {token}'
}

# MODIS Bands for Water Quality Analysis
layers = [
    "sur_refl_b01",  # Red - Turbidity, TSS
    "sur_refl_b02",  # NIR - NDWI, TSS
    "sur_refl_b03",  # Blue - Chlorophyll-a
    "sur_refl_b04",  # Green - Chlorophyll-a
   # "sur_refl_b05",  # SWIR - Water masking
    "sur_refl_qc_500m",  # Quality Control for cloud/water masking
  # "sur_refl_state_500m"  # State QA for cloud/water masking
]
log_message("Loading Regions of Interest")
roi_json = roi.__geo_interface__  # Convert ROI to GeoJSON

# Function to build the task request
def build_task_request(product, layers, roi_json, sd, ed):
    task = {
        "task_type": "area",
        "task_name": "MODIS_WaterQuality_Request test1",
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
            # Handle general files without aid numbers
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
    date_match = re.search(r'doy(\d{7,14})', filename)

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
    relevant_files = []
    water_mask_flag = True
    for f in selected_files:
        aid, f_date = extract_metadata(f)
        if aid == aid_number and date == f_date:
            relevant_files.append(f)
    if not relevant_files:
        print(f"No files found for date: {date}")
        return

    # Read raster layers
    Red = read_raster("sur_refl_b01", relevant_files)
    NIR = read_raster("sur_refl_b02", relevant_files)
    Blue = read_raster("sur_refl_b03", relevant_files)
    Green = read_raster("sur_refl_b04", relevant_files)
    #SWIR = read_raster("sur_refl_b05", relevant_files)
    QC = read_raster("sur_refl_qc_500m", relevant_files)

    if None in [Red, NIR, Blue, Green, QC]:
        print(f"Skipping {date} due to missing layers.")
        return

    # Read raster data into NumPy arrays
    arrays = {key: read_array(layer) for key, layer in {
        "sur_refl_b01":Red, "sur_refl_b02":NIR, "sur_refl_b03": Blue, "sur_refl_b04": Green, "sur_refl_qc_500m": QC
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
    raw_meta = Blue.meta.copy()
    raw_meta.update(dtype=rasterio.float32, count=len(arrays))  # Ensure correct band count

    # Open the new TIF file with multiple bands
    with rasterio.open(raw_tif_path, "w", **raw_meta) as dst:
        for idx, (key, data) in enumerate(arrays.items(), start=1):
            dst.write(data, idx)  # Write each band correctly

    print(f"Saved raw raster: {raw_tif_path}")

    # Convert raster data to DataFrame
    rows, cols = arrays["sur_refl_b01"].shape  # Use an available key like 'Green'
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


    ###`# QC Filtering and Masking`
    def extract_qc_flags(qc_value):
        """Extract cloud and water flags from the MODIS QC bit field."""
        qc_binary = format(qc_value, '032b')  # Convert QC value to 32-bit binary
        
        cloud_mask = int(qc_binary[-2:], 2)   # Cloud state (last 2 bits)
        water_mask = int(qc_binary[-4:-2], 2) # Water state (bits 2-3)

        return cloud_mask, water_mask
    
    # Function to apply QC mask
    def apply_qc_mask(df):
        """Filter out cloudy and deep water pixels based on MODIS QC flags."""
        
        # Ensure QC values are integer type
        df["sur_refl_qc_500m"] = df["sur_refl_qc_500m"].fillna(0).astype(int)
        
        # Extract flags
        cloud_flags, water_flags = zip(*df["sur_refl_qc_500m"].map(extract_qc_flags))
        
        # Store extracted flags
        df["cloud_mask"] = cloud_flags
        df["water_mask"] = water_flags
        
        return df
    
    # Apply the QC mask extraction function
    df = apply_qc_mask(df)

    # Step 1: Apply QC filtering (remove invalid pixels)
    for col in ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_qc_500m"]:
        df[f"{col}_filter"] = np.where(df["sur_refl_qc_500m"].isin(INVALID_QC_VALUES), np.nan, df[col])

    # Step 2: Remove Cloudy Pixels
    df["modland_qc"] = df["sur_refl_qc_500m"] & 0b11  # Extract bits 0-1

    # Cloud detection: If modland_qc == 3, it's cloudy
    df["cloud_mask"] = np.where(df["modland_qc"] == 3, 1, 0)

    # Set NaN for invalid pixels
    df.loc[df["sur_refl_qc_500m"].isin(INVALID_QC_VALUES), "cloud_mask"] = np.nan

    # Step 3: Water Masking
    df["water_mask"] = np.where(df["modland_qc"] == 0, 1, 0)

    # Set NaN for invalid pixels
    df.loc[df["sur_refl_qc_500m"].isin(INVALID_QC_VALUES), "water_mask"] = np.nan

    # Define output file paths
    filter_csv_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter_wtoff.csv")
    filter_tif_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter_wtoff.tif")

    # Save filtered CSV
    #filter_csv_path = os.path.join(filtered_path, f"{name}_{location}_{date}_filtered.csv")
    df.to_csv(filter_csv_path, index=False)
    multi_files.append(filter_csv_path)
    print(f"Saved filtered CSV: {filter_csv_path}")

    # Convert filtered data back to raster
    def create_raster(data, reference_raster):
        print("""Reshape filtered data into raster format using reference metadata.""")
       # rows, cols = reference_raster.shape
        meta = reference_raster.meta.copy()
        meta.update(dtype=rasterio.float32, count=1)  # Single-band raster
        return data.reshape(rows, cols).astype(np.float32), meta

    # Define filtered raster data for MODIS Surface Reflectance Bands
    filtered_rasters = {
        "sur_refl_b01": create_raster(df["sur_refl_b01_filter"].values, Red),
        "sur_refl_b02": create_raster(df["sur_refl_b02_filter"].values, NIR),
        "sur_refl_b03": create_raster(df["sur_refl_b03_filter"].values, Blue),
        "sur_refl_b04": create_raster(df["sur_refl_b04_filter"].values, Green),
        "sur_refl_qc_500m": create_raster(df["sur_refl_qc_500m_filter"].values, QC)
        #"sur_refl_b05": create_raster(df["sur_refl_b05_filtered"].values, )
    }
    # Get metadata from one of the rasters
    filter_meta = filtered_rasters["sur_refl_b04"][1].copy()
    filter_meta.update(dtype=rasterio.float32, count=len(filtered_rasters))  # Update band count

    with rasterio.open(filter_tif_path, "w", **filter_meta) as dst:
        for idx, (band_name, (data, _)) in enumerate(filtered_rasters.items(), start=1):
            dst.write(data, idx)  # Write each filtered band
    multi_aids.add(aid_number)
    multi_files.append(filter_tif_path)

    print(f"Saved filtered MODIS raster: {filter_tif_path}")
    print(f"Finished processing {date}")

# Main function to process all new water quality files
def process_all_water_quality(all_new_files):
   # updated_aids = get_updated_folders(all_new_files)  # Identify updated folders
    print(updated_aids)
    if not updated_aids:
        print("No new water quality data to process.")
        return

    print(f"Processing {len(updated_aids)} updated water quality datasets...")

    # Iterate through each updated folder
    print("All new files detected:", all_new_files)

    for aid_number in updated_aids:
        aid_folder_files = []
        for file in all_new_files:
            if aid_number == extract_metadata(file)[0]:
                aid_folder_files.append(file)
        new_dates_get = get_updated_dates(aid_folder_files)
        if not new_dates_get:
            print("No new files to process.")
            continue
        specific_date_files = []

        # Process each specific date's water quality files
        for date in new_dates_get:
           for file in aid_folder_files:
                if date == extract_metadata(file)[1]:
                    specific_date_files.append(file)

        process_rasters(aid_number, date, specific_date_files)

    print("Water quality processing complete.")

    ################ LINE 382 Start here ####################

    # Phase 5: Use this cleanup function after the main processing - cleanup_old_files(raw_path, days_old=20)
def cleanup_old_water_quality_files(folder_path, days_old=20):
    """
    Deletes water quality files older than the specified number of days.

    Args:
        folder_path (str): Path to the folder containing water quality files.
        days_old (int): Number of days before files are considered old and deleted.
    """
    # Calculate the cutoff time
    cutoff_time = datetime.now() - timedelta(days=days_old)

    # Iterate through each file in the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        # Ensure it's a file before processing
        if os.path.isfile(file_path):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))

            # Delete file if it's older than the cutoff time
            if file_mod_time < cutoff_time:
                os.remove(file_path)
                print(f"Deleted old water quality file: {filename} (Last modified: {file_mod_time})")

def log_water_quality_updates():

    """
    Logs updates related to water quality processing.

    Args:
        log_path (str): Path to the directory where logs will be stored.
        task_id (dict): Task-related information.
        updated_aids (set): Set of updated water quality AIDs.
        new_files (dict): Dictionary of newly processed files.
        multi_aids (set): Set of multi-aid entries.
        multi_files (dict): Dictionary of multi-aid files.
        aid_folder_mapping (dict): Mapping of AID folders.
        sd (dict): Start date information.
        ed (dict): End date information.
    """

    # Generate timestamp for log filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = f"water_quality_updates_{timestamp}.txt"
    full_path = os.path.join(log_path, file_path)

    # Open the new file and save updates
    with open(full_path, 'w', encoding='utf-8') as file:
        file.write(f"Timestamp: {timestamp}\n\n")

        # Log task information
        file.write("[Task Info]\n")
        file.write(json.dumps(task_id, indent=4) + "\n")
        file.write(json.dumps(sd, indent=4) + "\n")
        file.write(json.dumps(ed, indent=4) + "\n\n")

        # Log updated AIDs
        file.write("[Updated Water Quality AIDs]\n")
        file.write(json.dumps(list(updated_aids), indent=4) + "\n\n")

        # Log new files
        file.write("[New Water Quality Files]\n")
        file.write(json.dumps(new_files, indent=4) + "\n\n")

        # Log multiple AID handling
        file.write("[Multi AIDs]\n")
        file.write(json.dumps(list(multi_aids), indent=4) + "\n\n")

        file.write("[Multi Files]\n")
        file.write(json.dumps(multi_files, indent=4) + "\n\n")

        # Log AID Folder Mapping
        file.write("[AID Folder Mapping]\n")
        file.write(json.dumps(aid_folder_mapping, indent=4) + "\n\n")

    print(f"Water quality processing updates saved to {full_path}.")
# Phase 1: Submit task in one go
task_request = build_task_request(product, layers, roi_json, sd, ed)
task_id = submit_task(headers, task_request)
# task_id = "d0e5be4a-3747-46cb-9f63-ec24872ee799"
print(f"Task ID: {task_id}")

# # Phase 2: Create Directories and Mapping
# aid_folder_mapping = {}  # Initialize mapping outside the loop
for idx, row in roi.iterrows():
    print(f"Processing ROI {idx + 1}/{len(roi)}")
    
    # Construct directory path for saving data
    output_folder = os.path.join(raw_path, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder created: {output_folder}")
    
    # Map aid numbers to output folders
    # aid_number = f'aid{str(idx + 1).zfill(4)}'  # Construct aid number
    aid_number = int(idx + 1)  # Construct aid number
    aid_folder_mapping[int(aid_number)] = (row['name'], row['location'])  # Map aid number to folder

# Phase 3: Check the status of the single task
print("All tasks submitted!")
print("Checking task statuses...")
status = check_task_status(task_id, headers)
if status:
    print(f"Downloading results for Task ID: {task_id}...")
    download_results(task_id, headers)  # Pass the roi DataFrame for dynamic mapping    
print("All tasks completed, results downloaded!")

# Phase 4: Process the downloaded files
process_all_water_quality(new_files)

# Phase 5: Cleanup old files
# cleanup_old_files(raw_path, days_old=20)

# Phase 6: Log updates
log_water_quality_updates()