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
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
bucket_name = "multitifs"

supabase_folder = f"{SUPABASE_URL}/storage/v1/object/public/{bucket_name}"

# Directory paths
print("Setting Directory Paths")
raw_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\ECOraw\\"
filtered_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\ECO\\"
roi_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\misc\polygon\new_polygons.shp"
log_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\logs\\"

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

if not os.path.exists(roi_path):
    raise FileNotFoundError(f"The ROI shapefile does not exist at {roi_path}")

try:
    roi = gpd.read_file(roi_path)
except Exception as e:
    raise ValueError(f"Could not read the shapefile: {e}")

# Define Earthdata login credentials (Replace with your actual credentials)
user = "darennnnn"
password = "Darennnnnnnn11$"

if not user or not password:
    raise ValueError("Earthdata credentials are not set. Please set APPEEARS_USER and APPEEARS_PASS as environment variables.")

# Generate a timestamp for this run (format: YYYYMMDD_HHMM) for uniqueness in filenames
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# Get Today Date As End Date
print("Setting Dates")
today_date = datetime.now()
today_date_str = today_date.strftime("%m-%d-%Y")
ed = today_date_str
# ed = "06-24-2025"

# Get Yesterday Date as Start Date
yesterday_date = today_date - timedelta(days=1)
yesterday_date_str = yesterday_date.strftime("%m-%d-%Y")
sd = yesterday_date_str
# sd = "06-19-2025"

# KEY RESULTS TO STORE/LOG
updated_aids = set()
new_files = []
multi_aids = set()
multi_files = []
deleted_files = []
aid_folder_mapping = {}
# Invalid QC values
INVALID_QC_VALUES = {15, 2501, 3525, 65535}

token = get_token(user, password)

# Products, Headers and layers
product = "ECO_L2T_LSTE.002"
headers = {
    'Authorization': f'Bearer {token}'
}
layers = ["LST", "LST_err", "QC", "water", "cloud", "EmisWB", "height"]
# Load the area of interest (ROI)
print("Loading Regions of Interest")

roi = gpd.read_file(roi_path)
roi_json = roi.__geo_interface__  # Convert ROI to GeoJSON

# Function to build the task request with the payload
def build_task_request(product, layers, roi_json, sd, ed):
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
        local_filename = None

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
                new_files.append(local_filename)  # Track newly downloaded files
                print(f"Downloaded: {local_filename}")

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

            new_files.append(local_filename)  # Track newly downloaded files
            print(f"Downloaded: {local_filename}")

        file_path = f"updates_{timestamp}.txt"  # Each run creates a new file
        full_path = os.path.join(log_path, file_path)

        # Ensure the log directory exists
        os.makedirs(log_path, exist_ok=True)

        # Open the file in append mode to ensure all writes are preserved
        with open(full_path, 'a', encoding='utf-8') as file:
            file.write(f"Downloaded: {local_filename}\n")  # Log the file path

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
    relevant_files = []
    water_mask_flag = True
    # for f in selected_files if aid_number and date in FILENAME, 
    for f in selected_files:
        aid, f_date = extract_metadata(f)
        if aid == aid_number and date == f_date:
            relevant_files.append(f)
    # relevant_files = [f for f in selected_files if date in f]
    if not relevant_files:
        print(f"No files found for date: {date}")
        return

    # Read raster layers
    LST = read_raster("LST_doy", relevant_files)
    LST_err = read_raster("LST_err", relevant_files)
    QC = read_raster("QC", relevant_files)
    wt = read_raster("water", relevant_files)
    cl = read_raster("cloud", relevant_files)
    EmisWB = read_raster("EmisWB", relevant_files)
    heig = read_raster("height", relevant_files)

    if None in [LST, LST_err, QC, wt, cl, EmisWB, heig]:
        print(f"Skipping {date} due to missing layers.")
        return

    # Read raster data into NumPy arrays
    arrays = {key: read_array(layer) for key, layer in {
        "LST": LST, "LST_err": LST_err, "QC": QC, "wt": wt, "cloud": cl, "EmisWB": EmisWB, "height": heig
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
    raw_meta = LST.meta.copy()
    raw_meta.update(dtype=rasterio.float32, count=len(arrays))  # Ensure correct band count

    # Open the new TIF file with multiple bands
    with rasterio.open(raw_tif_path, "w", **raw_meta) as dst:
        for idx, (key, data) in enumerate(arrays.items(), start=1):
            dst.write(data, idx)  # Ensure it writes within the correct band range

    print(f"Saved raw raster: {raw_tif_path}")

    # Convert raster data to DataFrame
    rows, cols = arrays["LST"].shape
    x, y = np.meshgrid(np.arange(cols), np.arange(rows))

    # Ensure all arrays are not None and have the same shape
    valid_arrays = {key: arr for key, arr in arrays.items() if arr is not None}
    shapes = [arr.shape for arr in valid_arrays.values()]
    if len(set(shapes)) != 1:
        print(f"Array shape mismatch for aid {aid_number}, date {date}: {shapes}")
        return

    df = pd.DataFrame({
        "x": x.flatten(),
        "y": y.flatten(),
        **{key: arr.flatten() for key, arr in valid_arrays.items()}
    })

    # Save raw CSV
    raw_csv_path = os.path.join(dest_folder_raw, f"{name}_{location}_{date}_raw.csv")
    df.to_csv(raw_csv_path, index=False)
    print(f"Saved raw CSV: {raw_csv_path}")

    filter_csv_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter.csv")
    filter_tif_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter.tif")

    # Count total pixels as those with any value (not NaN)
    valid_pixels = df["LST"].notna().sum()
    print(f"Valid raw pixels for {date} (aid {aid_number}): {valid_pixels}")
    # Zero pixels are those with invalid or no values at all (NaN)
    invalid_pixels = df["LST"].isna().sum()
    print(f"Invalid raw pixels for {date} (aid {aid_number}): {invalid_pixels}")

    total_pixels = valid_pixels + invalid_pixels
    print(f"Total raw pixels for {date} (aid {aid_number}): {total_pixels}")

    if total_pixels > 0 and (invalid_pixels / (total_pixels) > 0.8):
        print(f"Skipping {date} for aid {aid_number}: more than 80% of raw pixels are invalid.")
        return

    water_mask_flag = df["wt"].isin([1]).any()

    # Apply filtering
    for col in ["LST", "LST_err", "QC", "EmisWB", "height"]:
        df[f"{col}_filter"] = np.where(df["QC"].isin(INVALID_QC_VALUES), np.nan, df[col])

    for col in ["LST_filter", "LST_err_filter", "QC_filter", "EmisWB_filter", "height_filter"]:
        df[f"{col}"] = np.where(df["cloud"] == 1, np.nan, df[col])

    if not water_mask_flag:
        for col in ["LST_filter", "LST_err_filter", "QC_filter", "EmisWB_filter", "height_filter"]:
            df[f"{col}"] = np.where(df["wt"] == 0, np.nan, df[col])
        filter_csv_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter_wtoff.csv")
        filter_tif_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter_wtoff.tif")

    for col in ["LST", "LST_err", "QC", "EmisWB"]:
        df.drop(columns=[f"{col}"], inplace=True)

    # Count total pixels as those with any value (not NaN)
    valid_pixels = df["LST_filter"].notna().sum()
    print(f"Valid filtered pixels for {date} (aid {aid_number}): {valid_pixels}")
    # Zero pixels are those with invalid or no values at all (NaN)
    invalid_pixels = df["LST_filter"].isna().sum()
    print(f"Invalid filtered pixels for {date} (aid {aid_number}): {invalid_pixels}")

    total_pixels = valid_pixels + invalid_pixels
    print(f"Total filtered pixels for {date} (aid {aid_number}): {total_pixels}")

    if total_pixels > 0 and (invalid_pixels / (total_pixels) > 0.8):
        print(f"Skipping {date} for aid {aid_number}: more than 80% of filtered pixels are invalid.")
        return

    # Convert filtered data back to raster
    def create_raster(data, reference_raster):
        meta = reference_raster.meta.copy()
        meta.update(dtype=rasterio.float32, count=1)
        return data.reshape(rows, cols).astype(np.float32), meta

    filtered_rasters = {
        "LST": create_raster(df["LST_filter"].values, LST),
        "LST_err": create_raster(df["LST_err_filter"].values, LST),
        "QC": create_raster(df["QC_filter"].values, LST),
        "EmisWB": create_raster(df["EmisWB_filter"].values, LST),
        "height": create_raster(df["height_filter"].values, LST),
    }

    # Save filtered raster
    filter_meta = filtered_rasters["LST"][1].copy()
    filter_meta.update(dtype=rasterio.float32, count=len(filtered_rasters))  # Correct band count

    # Save filtered raster
    with rasterio.open(filter_tif_path, "w", **filter_meta) as dst:
        for idx, (key, (data, _)) in enumerate(filtered_rasters.items(), start=1):
            dst.write(data, idx)  # Ensure correct band range
    multi_aids.add(aid_number)
    multi_files.append(filter_tif_path)

    print(f"Saved filtered raster: {filter_tif_path}")

    # Save raster metadata to a .txt file
    metadata_file_path = os.path.join(dest_folder_filtered, f"{name}_{location}_metadata.txt")
    with open(metadata_file_path, 'w') as meta_file:
        meta_file.write(str(filter_meta))
    print(f"Saved raster metadata: {metadata_file_path}")

    # Trouble-making Line -- Drop rows with NaN in the filtered LST column. Messes up when creating a TIF raster if mismatch in size
    df.dropna(subset=["LST_filter"], inplace=True)
            
    # Save filtered CSV
    df.to_csv(filter_csv_path, index=False)
    multi_files.append(filter_csv_path)
    print(f"Saved filtered CSV: {filter_csv_path}")

    file_path = f"updates_{timestamp}.txt"  # Each run creates a new file
    full_path = os.path.join(log_path, file_path)

    upload_to_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, filter_tif_path, name, location)
    upload_to_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, filter_csv_path, name, location)

    # Ensure the log directory exists
    os.makedirs(log_path, exist_ok=True)

    print(f"Finished processing {date}")

    # Open the file in append mode to ensure all writes are preserved
    with open(full_path, 'a', encoding='utf-8') as file:
        file.write(f"Filtered CSV {filter_csv_path}\n")  # Log the r"file path
        file.write(f"Filtered TIF {filter_tif_path}\n")  # Log the r"file path
        file.write(f"Filtered metadata {metadata_file_path}\n")  # Log the r"file path

# Main function to process all new files using multiprocessing
def process_all(all_new_files):
    # updated_aids = get_updated_folders(all_new_files)
    print(updated_aids)
    if not updated_aids:
        print("No new folders to process.")
        return
    
    print(f"Processing {len(updated_aids)} updated folders...")

    # updated_aids = list(updated_aids)

    # Process each updated folder and date
    for aid_number in updated_aids:
        # print(type(aid_number))
        aid_folder_files = []
        for file in all_new_files:
            if aid_number == extract_metadata(file)[0]:
                aid_folder_files.append(file)
        new_dates_get = get_updated_dates(aid_folder_files)
        if not new_dates_get:
            print("No new files to process.")
            continue
        specific_date_files = []

        for date in new_dates_get:
            for file in aid_folder_files:
                if date == extract_metadata(file)[1]:
                    specific_date_files.append(file)
            process_rasters(aid_number, date, specific_date_files)
    print("Processing complete.")

# Function to download TIF & CSV files to supabase bucket - create one to update shape file under Static?
def upload_to_supabase(bucket_name, supabase_url, supabase_key, file_path, name, location):
    log_file_path = f"updates_{timestamp}.txt"  # Each run creates a new file
    full_path = os.path.join(log_path, log_file_path)

    # Ensure the log directory exists
    os.makedirs(log_path, exist_ok=True)
    
    supabase: Client = create_client(supabase_url, supabase_key)
    supabase_path = f"ECO/{name}/{location}/{os.path.basename(file_path)}"
    # Check if file already exists in Supabase
    try:
        existing_files = supabase.storage.from_(bucket_name).list(f"ECO/{name}/{location}")
        if any(f["name"] == os.path.basename(file_path) for f in existing_files):
            print(f"Skipped upload: {file_path} already exists in Supabase bucket {bucket_name}")
            with open(full_path, 'a', encoding='utf-8') as log_file:
                log_file.write(f"Skipped upload: {file_path} already exists in Supabase\n")
            return
    except Exception as e:
        print(f"Error checking existence in Supabase: {e}")

    with open(file_path, 'rb') as file:
        supabase.storage.from_(bucket_name).upload(supabase_path, file)
        print(f"Uploaded {file_path} to Supabase bucket {bucket_name}")
    with open(full_path, 'a', encoding='utf-8') as log_file:
        log_file.write(f"Uploaded {file_path} to Supabase\n")  # Log the uploaded file path
    
# Cleanup old files in local folder and Supabase bucket
# Local folder version for testing
def cleanup_old_files_local(folder_path, specified_doy):
    """
    Deletes files in the specified local folder that were last modified before the cutoff_date.
    Args:
        folder_path (str): The path to the local folder.
        cutoff_date (datetime): The cutoff date. Files older than this will be deleted.
    """
    log_file_path = f"updates_{timestamp}.txt"  # Each run creates a new file
    full_path = os.path.join(log_path, log_file_path)

    # Ensure the log directory exists
    os.makedirs(log_path, exist_ok=True)

    for root, _, files in os.walk(folder_path):
        for filename in files:
            file_path = os.path.join(root, filename)
            # Extract date from filename using regex: expects ..._[YYYYDOYHHMMSS]...
            date_match = re.search(r'_(\d{7,13})', filename)
            if date_match:
                doy_str = date_match.group(1)
                # Extract DOY part (characters 5-7, assuming YYYYDOY...)
                if len(doy_str) >= 7:
                    doy = int(doy_str[4:7])
                    # Calculate current DOY from the timestamp variable
                    current_date = datetime.strptime(timestamp[:8], "%Y%m%d")
                    current_doy = current_date.timetuple().tm_yday
                    if doy < (current_doy - 30):
                        os.remove(file_path)
                        print(f"Deleted {file_path} (DOY {doy})")
                        with open(full_path, 'a', encoding='utf-8') as log_file:
                            log_file.write(f"Deleted {file_path} from Supabase\n")  # Log the r"file path
                            deleted_files.append(file_path)

def cleanup_old_files_supabase(bucket_name, supabase_url, supabase_key, specified_doy, cutoff_date):
    """
    Deletes files in the specified Supabase storage bucket that were last modified before the cutoff_date.
    Args:
        bucket_name (str): The name of the Supabase storage bucket.
        supabase_url (str): The Supabase project URL.
        supabase_key (str): The Supabase service role key.
        cutoff_date (datetime): The cutoff date. Files older than this will be deleted.
    """

    log_file_path = f"updates_{timestamp}.txt"  # Each run creates a new file
    full_path = os.path.join(log_path, log_file_path)

    # Ensure the log directory exists
    os.makedirs(log_path, exist_ok=True)

    supabase: Client = create_client(supabase_url, supabase_key)
    # List all files in the bucket
    folders = supabase.storage.from_(bucket_name).list("ECO/")
    for folder in folders:
        try:
            files = supabase.storage.from_(bucket_name).list(f"ECO/{folder['name']}/lake")
        except Exception as e:
            print(f"Error listing files in {folder['name']}: {e}")
            return

        for file in files:
            # file['updated_at'] is in ISO format, e.g. '2024-06-01T12:34:56.789Z'
            # Extract date from filename using regex: expects ..._[YYYYDOYHHMMSS]...
            date_match = re.search(r'_(\d{7,13})', file['name'])
            if date_match:
                doy_str = date_match.group(1)
                # Extract DOY part (characters 5-7, assuming YYYYDOY...)
                if len(doy_str) >= 7:
                    doy = int(doy_str[4:7])
                    # Calculate current DOY from the timestamp variable
                    current_date = datetime.strptime(timestamp[:8], "%Y%m%d")
                    current_doy = current_date.timetuple().tm_yday
                    if doy < (current_doy - 30):
                        file_path = f"ECO/{folder['name']}/lake/{file['name']}"
                        # print(f"Found old file: {file_path} (DOY {doy})")
                        supabase.storage.from_(bucket_name).remove([file_path])
                        print(f"Deleted {file_path} (DOY {doy})")
                        with open(full_path, 'a', encoding='utf-8') as log_file:
                            log_file.write(f"Deleted {file_path} from Supabase\n")  # Log the r"file path
                            deleted_files.append(file_path)

def cleanup_duplicate_filters_by_doy(folder_path):
    """
    For each DOY, if both _filter and _filter_wtoff files exist, delete the _filter file.
    """
    for root, _, files in os.walk(folder_path):
        # Group files by DOY
        files_by_doy = {}
        for filename in files:
            # Match DOY in filename
            date_match = re.search(r'_(\d{7,13})', filename)
            if date_match and len(date_match.group(1)) >= 7:
                doy = date_match.group(1)[4:7]
                files_by_doy.setdefault(doy, []).append(filename)

        for doy, doy_files in files_by_doy.items():
            # Find _filter and _filter_wtoff files for this DOY
            filter_files = [f for f in doy_files if re.search(r'_filter\.\w+$', f) and not re.search(r'_filter_wtoff\.\w+$', f)]
            wtoff_files = [f for f in doy_files if re.search(r'_filter_wtoff\.\w+$', f)]
            # For each _filter file, check if a corresponding _filter_wtoff exists (same base)
            for filt_file in filter_files:
                base = re.sub(r'_filter\.\w+$', '', filt_file)
                for wtoff_file in wtoff_files:
                    wtoff_base = re.sub(r'_filter_wtoff\.\w+$', '', wtoff_file)
                    if base == wtoff_base:
                        file_path = os.path.join(root, filt_file)
                        print(f"Deleted {file_path} (has _filter_wtoff alternative)")
                        os.remove(file_path)
                        break

# Function to log updates
def log_updates():
    # Open the log file in append mode
    file_path = f"completed_updates_{timestamp}.txt"  # Each run creates a new file
    full_path = os.path.join(log_path, file_path)

    # Open the new file and save updates
    with open(full_path, 'w', encoding='utf-8') as file:
        file.write(f"Timestamp: {timestamp}\n\n")

        # Log task information
        file.write("[Task Info]\n")
        file.write(json.dumps(task_id, indent=4))  # Format JSON output
        file.write(json.dumps(sd, indent=4))  # Format JSON output
        file.write(json.dumps(ed, indent=4))  # Format JSON output
        file.write("\n\n")

        # Log list update
        file.write("[Updated Aids]\n")
        file.write(json.dumps(list(updated_aids), indent=4))  # Format JSON output
        file.write("\n\n")
        
        # Log dictionary update
        file.write("[New Files]\n")
        file.write(json.dumps(new_files, indent=4))  # Format JSON output
        file.write("\n\n")

        # Log list update
        file.write("[New Dates]\n")
        file.write("\n\n")

        # Log list update
        file.write("[Multi Aids]\n")
        file.write(json.dumps(list(multi_aids), indent=4))  # Format JSON output
        file.write("\n\n")

        # Log list update
        file.write("[Multi Files]\n")
        file.write(json.dumps(multi_files, indent=4))  # Format JSON output
        file.write("\n\n")

        # Log list update
        file.write("[Deleted Files]\n")
        file.write(json.dumps(deleted_files, indent=4))  # Format JSON output
        file.write("\n\n")
        
        # Log dictionary update
        file.write("[Aid Folder Mapping]\n")
        file.write(json.dumps(aid_folder_mapping, indent=4))  # Format JSON output
        file.write("\n\n")

    print(f"Updates saved to {full_path}.")

# Phase 1: Submit task in one go
# task_request = build_task_request(product, layers, roi_json, sd, ed)
# task_id = submit_task(headers, task_request)
# print(f"Task ID: {task_id}")

# Phase 2: Create Directories and Mapping
aid_folder_mapping = {}  # Initialize mapping outside the loop
for idx, row in roi.iterrows():
    print(f"Processing ROI {idx + 1}/{len(roi)}")
    
    # Construct directory path for saving data
    output_folder = os.path.join(raw_path, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder created: {output_folder}")

    # Map aid numbers to output folders
#    aid_number = f'aid{str(idx + 1).zfill(4)}'  # Construct aid number
    aid_number = int(idx + 1)  # Construct aid number
    aid_folder_mapping[int(aid_number)] = (row['name'], row['location'])  # Map aid number to folder

# # Phase 3: Check the status of the single task
# print("All tasks submitted!")
# print("Checking task statuses...")
# status = check_task_status(task_id, headers)
# if status:
#     print(f"Downloading results for Task ID: {task_id}...")
#     download_results(task_id, headers)  # Pass the roi DataFrame for dynamic mapping    
# print("All tasks completed, results downloaded!")

# # Phase 4: Process the raster files
process_all(new_files)

# Phase 5: Cleanup old files
# test_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\misc\2606minus30\ECO\\"
# cleanup_old_files_local(test_path, 32)
# cleanup_old_files_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, 32, datetime(2025, 2, 1))

# Phase 6: Log updates
# log_updates()
