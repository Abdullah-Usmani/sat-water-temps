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
raw_path = r"C:\Users\Darren\Desktop\segp\Water Temp Sensors\MODISLSTraw"
filtered_path = r"C:\Users\Darren\Desktop\segp\Water Temp Sensors\MODISLST"
roi_path = r"C:\Users\Darren\Desktop\segp\Water Temp Sensors\polygon\new_polygons.shp" 
log_path = r"C:\Users\Darren\Desktop\segp\Water Temp Sensors\logs"
download_log = os.path.join(log_path, "downloaded_files.json")
log_file = os.path.join(log_path, f"modisLST_retrieval_{datetime.now().strftime('%Y%m%d_%H%M')}.log")

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
user = 'darennnnn'
password = 'Darennnnnnnn11$'

# Generate a timestamp for uniqueness in filenames
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# Set date range
log_message("Setting Dates")
today_date = datetime.now()
end_date = today_date - timedelta(days=1)
start_date = today_date - timedelta(days=8)
ed = "03-05-2023"
sd = "02-26-2023"

# KEY RESULTS TO STORE/LOG
updated_aids = set()
new_files = []
new_dates = []
multi_aids = set()
multi_files = []
aid_folder_mapping = {}

# Landsat-specific settings
token = get_token(user, password)
product = "MYD11A1.061"  # MODIS/Aqua Land Surface Temperature/Emissivity Daily L3 Global 1km
headers = {
    'Authorization': f'Bearer {token}'
}

# Landsat Bands for LST Calculation
layers = [
  "LST_Day_1km",      # Daytime Land Surface Temperature
    "LST_Night_1km",    # Nighttime Land Surface Temperature
    "QC_Day",           # Quality Control for Daytime LST
    "QC_Night",         # Quality Control for Nighttime LST
    "Emis_31",          # Emissivity Band 31
    "Emis_32"           # Emissivity Band 32
]
log_message("Loading Regions of Interest")
roi_json = roi.__geo_interface__  # Convert ROI to GeoJSON

# Function to build the task request
def build_task_request(product, layers, roi_json, sd, ed):
    task = {
        "task_type": "area",
        "task_name": "Landsat_LST_Request",
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
            print(f"Task {task_id} is complete!")
            doneFlag = True
            break
        elif status == "processing":
            print(f"Task {task_id} is still processing. Checking again in 30 seconds...")
            time.sleep(30)
        elif status == "queued":
            print(f"Task {task_id} is still queued. Checking again in 30 seconds...")
            time.sleep(30)
        elif status == "pending":
            print(f"Task {task_id} is still pending. Checking again in 30 seconds...")
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
            download_url = f"{url}/{file_id}"
            download_response = requests.get(download_url, headers=headers, stream=True, allow_redirects=True)

            with open(local_filename, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    f.write(chunk)

            new_files.append(local_filename)  # Track newly downloaded file
            print(f"Downloaded {local_filename}")

def extract_metadata(filename):
    aid_match = re.search(r'aid(\d{4})', filename, re.IGNORECASE)
    date_match = re.search(r'(\d{4}_\d{2}_\d{2})', filename)
    
    aid_number = int(aid_match.group(1)) if aid_match else None
    date = date_match.group(1) if date_match else None
    
    print(f"Debug - Filename: {filename} | Extracted AID: {aid_number} | Date: {date}")  # Add this
    return aid_number, date

# Function to filter only new folders and return unique folders
def get_updated_folders(new_files):
    return {extract_metadata(f)[0] for f in new_files if extract_metadata(f)[0]}

# Function to filter only new files and return unique dates
def get_updated_dates(new_files):
    """Extract unique dates from newly downloaded files"""
    dates = set()
    for f in new_files:
        _, date = extract_metadata(f)
        if date:
            dates.add(date)
    return dates

# Function to read a specific raster layer
def read_raster(layer_name, relevant_files):
    matches = [f for f in relevant_files if layer_name in f]
    return rasterio.open(matches[0]) if matches else None


# Function to process MODIS LST data
def process_modis_lst(aid_number, date, selected_files):
    print(f"Debug - Processing AID: {aid_number} | Date: {date}")
    print(f"Debug - Selected files: {selected_files}")
    print(f"Processing date: {date} for aid: {aid_number}")
    relevant_files = []
    for f in selected_files:
        aid, f_date = extract_metadata(f)
        if aid == aid_number and date == f_date:
            relevant_files.append(f)
            print(f"Found files: {relevant_files}")  # Debug: List files being processed

    if not relevant_files:
        print(f"No files found for date: {date}")
        return
    
  
    # Read MODIS LST layers
    lst_day = read_raster("LST_Day_1km", relevant_files)
    lst_night = read_raster("LST_Night_1km", relevant_files)
    qc_day = read_raster("QC_Day", relevant_files)
    qc_night = read_raster("QC_Night", relevant_files)
    emis_31 = read_raster("Emis_31", relevant_files)
    emis_32 = read_raster("Emis_32", relevant_files)

    if None in [lst_day, qc_day]:
        print(f"Skipping {date} due to missing daytime layers.")
        return

    # Get AID number and location info
    name, location = aid_folder_mapping.get(aid_number, (None, None))
    print(f"Debug - Mapping found: {name}/{location} for AID {aid_number}")
    if not name or not location:
        print(f"No mapping found for AID: {aid_number}, skipping...")
        return
    
    # Define destination folders
    dest_folder_raw = os.path.join(raw_path, name, location)
    dest_folder_filtered = os.path.join(filtered_path, name, location)
    os.makedirs(dest_folder_raw, exist_ok=True)
    os.makedirs(dest_folder_filtered, exist_ok=True)

    # Convert MODIS LST from Kelvin to Celsius (scale factor 0.02)
    def process_lst_band(lst_band, qc_band):
        if lst_band is None:
            print("Error: LST band is None!")  # Debug
            return None
        
        # Read data and apply scale factor
        lst_data = lst_band.read(1).astype(float) * 0.02
        qc_data = qc_band.read(1) if qc_band else None
        
        # Apply quality control (bits 0-1: 00=good, 01=other quality)
        if qc_data is not None:
            lst_data[(qc_data & 0x03) != 0] = np.nan  # Mask non-good quality pixels
        
        return lst_data - 273.15  # Convert Kelvin to Celsius

    # Process daytime and nighttime LST
    lst_day_c = process_lst_band(lst_day, qc_day)
    if np.all(np.isnan(lst_day_c)):
        print(f"Warning: All daytime LST values are NaN for {date} (AID {aid_number})")
    lst_night_c = process_lst_band(lst_night, qc_night)

    # Save raw data
    raw_tif_path = os.path.join(dest_folder_raw, f"{name}_{location}_{date}_raw.tif")
    raw_meta = lst_day.meta.copy()
    raw_meta.update(dtype=rasterio.float32, count=6)
    
    with rasterio.open(raw_tif_path, "w", **raw_meta) as dst:
        if lst_day is not None: dst.write(lst_day.read(1), 1)
        if lst_night is not None: dst.write(lst_night.read(1), 2)
        if qc_day is not None: dst.write(qc_day.read(1), 3)
        if qc_night is not None: dst.write(qc_night.read(1), 4)
        if emis_31 is not None: dst.write(emis_31.read(1), 5)
        if emis_32 is not None: dst.write(emis_32.read(1), 6)

    print(f"Saved raw raster: {raw_tif_path}")

    # Save processed LST data
    if lst_day_c is not None:
        lst_day_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_lst_day.tif")
        with rasterio.open(lst_day_path, "w", **lst_day.meta) as dst:
            dst.write(lst_day_c, 1)
        print(f"Saved daytime LST: {lst_day_path}")

    if lst_night_c is not None:
        lst_night_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_lst_night.tif")
        with rasterio.open(lst_night_path, "w", **lst_night.meta) as dst:
            dst.write(lst_night_c, 1)
        print(f"Saved nighttime LST: {lst_night_path}")

# Main function to process all new files
def process_all_files(all_new_files):
    print(updated_aids)
    if not updated_aids:
        print("No new data to process.")
        return

    print(f"Processing {len(updated_aids)} updated datasets...")
    
    # Iterate through each updated folder
    print("All new files detected:", all_new_files)

    for aid_number in updated_aids:
        aid_folder_files = []
        for file in all_new_files:
            if aid_number == extract_metadata(file)[0]:
                aid_folder_files.append(file)
        
        new_dates_get = get_updated_dates(aid_folder_files)
        if not new_dates_get:
            print(f"No new dates found for AID {aid_number}")
            continue

        # Process each specific date's files
        for date in new_dates_get:
            specific_date_files = []
            for file in aid_folder_files:
                if date == extract_metadata(file)[1]:
                    specific_date_files.append(file)
            
            print(f"Processing AID {aid_number}, date {date}")
            process_modis_lst(aid_number, date, specific_date_files)
            print(f"Finished processing {date}")  # Now 'date' is defined here

    print("Processing complete.")

# Move folder creation outside the processing function
for aid, (name, location) in aid_folder_mapping.items():
    os.makedirs(os.path.join(filtered_path, name, location), exist_ok=True)

def log_updates():
    """
    Logs updates related to processing.
    """
    # Generate timestamp for log filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = f"modis_updates_{timestamp}.txt"
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
        file.write("[Updated AIDs]\n")
        file.write(json.dumps(list(updated_aids), indent=4) + "\n\n")

        # Log new files
        file.write("[New Files]\n")
        file.write(json.dumps(new_files, indent=4) + "\n\n")

        # Log multiple AID handling
        file.write("[Multi AIDs]\n")
        file.write(json.dumps(list(multi_aids), indent=4) + "\n\n")

        file.write("[Multi Files]\n")
        file.write(json.dumps(multi_files, indent=4) + "\n\n")

        # Log AID Folder Mapping
        file.write("[AID Folder Mapping]\n")
        file.write(json.dumps(aid_folder_mapping, indent=4) + "\n\n")

    print(f"Processing updates saved to {full_path}.")

# Phase 1: Submit task in one go
task_request = build_task_request(product, layers, roi_json, sd, ed)
task_id = submit_task(headers, task_request)
print(f"Task ID: {task_id}")

# Phase 2: Create Directories and Mapping
for idx, row in roi.iterrows():
    print(f"Processing ROI {idx + 1}/{len(roi)}")
    
    # Construct directory path for saving data
    output_folder = os.path.join(raw_path, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder created: {output_folder}")
    
    # Map aid numbers to output folders
    aid_number = int(idx + 1)  # Construct aid number
    aid_folder_mapping[int(aid_number)] = (row['name'], row['location'])  # Map aid number to folder

    # After defining aid_folder_mapping 
for name, location in aid_folder_mapping.values():
    os.makedirs(os.path.join(filtered_path, name, location), exist_ok=True)
    print(f"Created: {filtered_path}/{name}/{location}")

# Phase 3: Check the status of the single task
print("All tasks submitted!")
print("Checking task statuses...")
status = check_task_status(task_id, headers)
if status:
    print(f"Downloading results for Task ID: {task_id}...")
    download_results(task_id, headers)  # Pass the roi DataFrame for dynamic mapping    
print("All tasks completed, results downloaded!")

print("\nValidation before processing:")
print(f"Updated AIDs: {updated_aids}")
print(f"AID Folder Mapping: {aid_folder_mapping}")
print(f"New files count: {len(new_files)}")
print("Sample new_files paths:", new_files[:3])

# Phase 4: Process the downloaded files
process_all_files(new_files)

# Phase 6: Log updates
log_updates()