# === Import Libraries ===

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
from pathlib import Path

# === Directory & Environment Setup ===

# Define directory paths
eco_raw_path = Path(r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\ECOraw")
eco_filtered_path = Path(r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\ECO")
myd_raw_path = Path(r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\MYD11A1raw")
myd_filtered_path = Path(r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\MYD11A1")
raw_download_path = Path(r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\downloads")
roi_path = Path(r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\misc\polygon\new_polygons.shp")
log_path = Path(r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\logs")

# Ensure ROI shapefile exists and load it
if not roi_path.exists():
    raise FileNotFoundError(f"ROI shapefile not found: {roi_path}")
try:
    roi = gpd.read_file(str(roi_path))
except Exception as e:
    raise ValueError(f"Failed to read ROI shapefile: {e}")

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials in environment variables.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
bucket_name = "multitifs"
supabase_folder = f"{SUPABASE_URL}/storage/v1/object/public/{bucket_name}"

# === Global State for Logging/Tracking ===
# Load updated_aids and new_files from text.txt
updated_aids = set()
new_files = []
multi_aids = set()
multi_files = []
deleted_files = []
aid_folder_mapping = {}
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

### === 1. TASK SUBMISSION FUNCTIONS & PAYLOADS ===

# Get token (API login via requests)
def get_token(user, password):
    #Authenticates with Earthdata and retrieves an authentication token.
    try:
        response = requests.post('https://appeears.earthdatacloud.nasa.gov/api/login', auth=(user, password))
        response.raise_for_status()
        token_response = response.json()
        return token_response['token']
    except requests.exceptions.HTTPError as err:
        raise SystemExit(f"Authentication failed: {err}")

# Define Earthdata login credentials (Replace with your actual credentials)
user = os.getenv("APPEEARS_USER")
password = os.getenv("APPEEARS_PASS")

if not user or not password:
    raise ValueError("Earthdata credentials are not set. Please set APPEEARS_USER and APPEEARS_PASS as environment variables.")

# Parameters for the API task submission
token = get_token(user, password)

# Define the product and layers to request
product = "ECO_L2T_LSTE.002"
headers = { 'Authorization': f'Bearer {token}' }
layers = ["LST", "LST_err", "QC", "water", "cloud", "EmisWB", "height"]

# Landsat-specific settings
product_MODIS = "MYD11A1.061"  # MODIS/Aqua Land Surface Temperature/Emissivity Daily L3 Global 1km
# Landsat Bands for LST Calculation
layers_MODIS = [
  "LST_Day_1km",      # Daytime Land Surface Temperature
    "LST_Night_1km",    # Nighttime Land Surface Temperature
    "QC_Day",           # Quality Control for Daytime LST
    "QC_Night",         # Quality Control for Nighttime LST
    "Emis_31",          # Emissivity Band 31
    "Emis_32"           # Emissivity Band 32
]


# Define the date range for the task
end_date = datetime.now().strftime("%m-%d-%Y")
end_date = "07-05-2025"
start_date = (datetime.now() - timedelta(days=1)).strftime("%m-%d-%Y")
start_date = "06-30-2025"

# Load the area of interest (ROI)
roi = gpd.read_file(roi_path)
roi_json = roi.__geo_interface__  # Convert ROI to GeoJSON

# Function to build the task request with the payload
def build_task_request(product, layers, roi_json, start_date, end_date):
    task = {
        "task_type": "area",
        "task_name": product,
        "params": {
            "dates": [{"startDate": start_date, "endDate": end_date}],
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

## === 2. CREATE AID FOLDER MAPPING FUNCTION(S) ===

def create_aid_folder_mapping(roi, eco_raw_path):
    """
    Creates a mapping from aid numbers to (name, location) tuples and ensures output folders exist.
    Returns the mapping as a dictionary.
    """
    mapping = {}
    for idx, row in roi.iterrows():
        print(f"Processing ROI {idx + 1}/{len(roi)}")
        output_folder = os.path.join(eco_raw_path, row['name'], row['location'])
        os.makedirs(output_folder, exist_ok=True)
        print(f"Output folder created: {output_folder}")
        aid_number = int(idx + 1)
        mapping[aid_number] = (row['name'], row['location'])
    return mapping

## === 3. DOWNLOAD RESULTS FUNCTION(S) ===

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
            output_folder = os.path.join(raw_download_path, name, location)
            
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
                print(f","r"{local_filename}")

        else:
            # Handle general files without aid numbers (e.g., XML, CSV, JSON)
            base_name, ext = os.path.splitext(file_name)
            new_file_name = f"{base_name}_{timestamp}{ext}"  # Append timestamp before extension
            local_filename = os.path.join(raw_download_path, new_file_name)  # Save directly to the base folder

            # print(f"Downloading to base folder: {local_filename}")

            download_url = f"{url}/{file_id}"
            download_response = requests.get(download_url, headers=headers, stream=True, allow_redirects=True)

            with open(local_filename, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    f.write(chunk)

            new_files.append(local_filename)  # Track newly downloaded files
            print(f","r"{local_filename}")

        file_path = f"updates_{timestamp}.txt"  # Each run creates a new file
        full_path = os.path.join(log_path, file_path)

        # Ensure the log directory exists
        os.makedirs(log_path, exist_ok=True)

        # Open the file in append mode to ensure all writes are preserved
        with open(full_path, 'a', encoding='utf-8') as file:
            file.write(f","r"{local_filename}\n")  # Log the file path

### === 4. PROCESSING FUNCTIONS ===

# Function to extract aid number and date from filename
def extract_metadata(filename):
    aid_match = re.search(r'aid(\d{4})', filename)
    date_match = re.search(r'doy(\d{13})', filename)

    aid_number = int(aid_match.group(1)) if aid_match else None
    date = date_match.group(1) if date_match else None

    return aid_number, date


# Function to process a single date for either ECO or MODIS product
def process_rasters(aid_number, date, selected_files, product_type="ECO"):
    print(f"Processing date: {date} for aid: {aid_number} (Product: {product_type})")

    # Filter files for this aid and date
    relevant_files = [
        f for f in selected_files
        if extract_metadata(f)[0] == aid_number and extract_metadata(f)[1] == date
    ]
    if not relevant_files:
        print(f"No files found for date: {date}")
        return

    # Get folder info
    name, location = aid_folder_mapping.get(aid_number, (None, None))
    if not name or not location:
        print(f"No mapping found for AID: {aid_number}, skipping...")
        return

    def open_raster(layer):
        for f in relevant_files:
            if layer in f:
                try:
                    return rasterio.open(f)
                except rasterio.errors.RasterioIOError as e:
                    print(f"Error opening raster '{f}': {e}")
                    if "not recognized as being in a supported file format" in str(e):
                        print(f"Skipping file due to unsupported format: {f}")
                    return None
        return None

    if product_type == "ECO":
        dest_raw = os.path.join(eco_raw_path, name, location)
        dest_filtered = os.path.join(eco_filtered_path, name, location)
        os.makedirs(dest_raw, exist_ok=True)
        os.makedirs(dest_filtered, exist_ok=True)

        # Read all required layers
        layer_names = ["LST", "LST_err", "QC", "water", "cloud", "EmisWB", "height"]
        rasters = {name: open_raster(name) for name in layer_names}
        if any(r is None for r in rasters.values()):
            print(f"Skipping {date} due to missing or unsupported layers.")
            return

        arrays = {k: v.read(1) for k, v in rasters.items()}
        raw_tif_path = os.path.join(dest_raw, f"{name}_{location}_{date}_raw.tif")
        meta = rasters["LST"].meta.copy()
        meta.update(dtype=rasterio.float32, count=len(arrays))
        with rasterio.open(raw_tif_path, "w", **meta) as dst:
            for idx, arr in enumerate(arrays.values(), 1):
                dst.write(arr, idx)
        print(f"Saved raw raster: {raw_tif_path}")

        rows, cols = arrays["LST"].shape
        x, y = np.meshgrid(np.arange(cols), np.arange(rows))
        df = pd.DataFrame({
            "x": x.flatten(),
            "y": y.flatten(),
            **{k: arr.flatten() for k, arr in arrays.items()}
        })

        raw_csv_path = os.path.join(dest_raw, f"{name}_{location}_{date}_raw.csv")
        df.to_csv(raw_csv_path, index=False)
        print(f"Saved raw CSV: {raw_csv_path}")

        valid = df["LST"].notna().sum()
        invalid = df["LST"].isna().sum()
        total = valid + invalid
        print(f"Valid raw pixels: {valid}, Invalid: {invalid}, Total: {total}")
        if total > 0 and invalid / total > 0.9:
            print(f"Skipping {date} for aid {aid_number}: >90% invalid raw pixels.")
            return

        INVALID_QC = {15, 2501, 3525, 65535}
        water_mask = df["water"].isin([1]).any()
        for col in ["LST", "LST_err", "QC", "EmisWB", "height"]:
            df[f"{col}_f"] = np.where(df["QC"].isin(INVALID_QC), np.nan, df[col])
        for col in ["LST_f", "LST_err_f", "QC_f", "EmisWB_f", "height_f"]:
            df[col] = np.where(df["cloud"] == 1, np.nan, df[col])
        if not water_mask:
            for col in ["LST_f", "LST_err_f", "QC_f", "EmisWB_f", "height_f"]:
                df[col] = np.where(df["water"] == 0, np.nan, df[col])
            filter_csv_path = os.path.join(dest_filtered, f"{name}_{location}_{date}_filter_wtoff.csv")
            filter_tif_path = os.path.join(dest_filtered, f"{name}_{location}_{date}_filter_wtoff.tif")
        else:
            filter_csv_path = os.path.join(dest_filtered, f"{name}_{location}_{date}_filter.csv")
            filter_tif_path = os.path.join(dest_filtered, f"{name}_{location}_{date}_filter.tif")

        df.drop(columns=["LST", "LST_err", "QC", "EmisWB", "height"], inplace=True)

        valid = df["LST_f"].notna().sum()
        invalid = df["LST_f"].isna().sum()
        total = valid + invalid
        print(f"Valid filtered pixels: {valid}, Invalid: {invalid}, Total: {total}")
        if total > 0 and invalid / total > 0.9:
            print(f"Skipping {date} for aid {aid_number}: >90% invalid filtered pixels.")
            return

        def arr_to_raster(data, ref_raster):
            meta = ref_raster.meta.copy()
            meta.update(dtype=rasterio.float32, count=1)
            return data.reshape(rows, cols).astype(np.float32), meta

        filtered_layers = ["LST_f", "LST_err_f", "QC_f", "EmisWB_f", "height_f"]
        filtered_rasters = {k: arr_to_raster(df[k].values, rasters["LST"]) for k in filtered_layers}
        filter_meta = filtered_rasters["LST_f"][1].copy()
        filter_meta.update(count=len(filtered_rasters))
        with rasterio.open(filter_tif_path, "w", **filter_meta) as dst:
            for idx, (k, (data, _)) in enumerate(filtered_rasters.items(), 1):
                dst.write(data, idx)
        print(f"Saved filtered raster: {filter_tif_path}")

        meta_path = os.path.join(dest_filtered, f"{name}_{location}_metadata.txt")
        with open(meta_path, 'w') as f:
            f.write(str(filter_meta))
        print(f"Saved raster metadata: {meta_path}")

        df.dropna(subset=["LST_f"], inplace=True)
        df.to_csv(filter_csv_path, index=False)
        print(f"Saved filtered CSV: {filter_csv_path}")

        multi_aids.add(aid_number)
        multi_files.extend([filter_tif_path, filter_csv_path])

        upload_to_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, filter_tif_path, name, location)
        upload_to_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, filter_csv_path, name, location)

        log_file = os.path.join(log_path, f"updates_{timestamp}.txt")
        os.makedirs(log_path, exist_ok=True)
        with open(log_file, 'a', encoding='utf-8') as file:
            file.write(f"Filtered CSV {filter_csv_path}\n")
            file.write(f"Filtered TIF {filter_tif_path}\n")
            file.write(f"Filtered metadata {meta_path}\n")

        print(f"Finished processing {date}")

    elif product_type == "MODIS":
        dest_raw = os.path.join(myd_raw_path, name, location)
        dest_filtered = os.path.join(myd_filtered_path, name, location)
        os.makedirs(dest_raw, exist_ok=True)
        os.makedirs(dest_filtered, exist_ok=True)

        # Open MODIS rasters by layer name (similar to ECO)
        layer_names = [
            "LST_Day_1km", "LST_Night_1km",
            "QC_Day", "QC_Night",
            "Emis_31", "Emis_32"
        ]
        rasters = {name: open_raster(name) for name in layer_names}
        if rasters["LST_Day_1km"] is None or rasters["QC_Day"] is None:
            print(f"Skipping {date} due to missing or unsupported daytime layers.")
            return

        # Convert MODIS LST from Kelvin to Celsius (scale factor 0.02) // Not doing celsius conversion right now
        def process_lst_band(lst_band, qc_band):
            if lst_band is None:
                print("Error: LST band is None!")
                return None
            lst_data = lst_band.read(1).astype(float)*(0.02)
            qc_data = qc_band.read(1) if qc_band else None
            if qc_data is not None:
                lst_data[(qc_data & 0x03) != 0] = np.nan  # Mask non-good quality pixels
                return lst_data

        lst_day_c = process_lst_band(rasters["LST_Day_1km"], rasters["QC_Day"])
        lst_night_c = process_lst_band(rasters["LST_Night_1km"], rasters["QC_Night"])

        # Save raw raster
        raw_tif_path = os.path.join(dest_raw, f"{name}_{location}_{date}_raw.tif")
        raw_meta = rasters["LST_Day_1km"].meta.copy()
        raw_meta.update(dtype=rasterio.float32, count=len(layer_names))
        with rasterio.open(raw_tif_path, "w", **raw_meta) as dst:
            for idx, layer in enumerate(layer_names, 1):
                if rasters[layer] is not None:
                    dst.write(rasters[layer].read(1), idx)
        print(f"Saved raw raster: {raw_tif_path}")

        # Save raw CSV
        rows, cols = rasters["LST_Day_1km"].read(1).shape
        x, y = np.meshgrid(np.arange(cols), np.arange(rows))
        df = pd.DataFrame({
            "x": x.flatten(),
            "y": y.flatten(),
            "LST_Day_1km": rasters["LST_Day_1km"].read(1).flatten() if rasters["LST_Day_1km"] is not None else np.nan,
            "LST_Night_1km": rasters["LST_Night_1km"].read(1).flatten() if rasters["LST_Night_1km"] is not None else np.nan,
            "QC_Day": rasters["QC_Day"].read(1).flatten() if rasters["QC_Day"] is not None else np.nan,
            "QC_Night": rasters["QC_Night"].read(1).flatten() if rasters["QC_Night"] is not None else np.nan,
            "Emis_31": rasters["Emis_31"].read(1).flatten() if rasters["Emis_31"] is not None else np.nan,
            "Emis_32": rasters["Emis_32"].read(1).flatten() if rasters["Emis_32"] is not None else np.nan,
        })
        raw_csv_path = os.path.join(dest_raw, f"{name}_{location}_{date}_MODISraw.csv")
        df.to_csv(raw_csv_path, index=False)
        print(f"Saved raw CSV: {raw_csv_path}")

        valid = np.isfinite(df["LST_Day_1km"]).sum()
        invalid = (~np.isfinite(df["LST_Day_1km"])).sum()
        total = valid + invalid
        print(f"Valid raw pixels: {valid}, Invalid: {invalid}, Total: {total}")
        if total > 0 and invalid / total > 0.9:
            print(f"Skipping {date} for aid {aid_number}: >90% invalid raw pixels.")
            return


        # Filtered CSV (masking by QC)
        df["LST_Day_1km_f"] = lst_day_c.flatten() if lst_day_c is not None else np.nan
        df["LST_Night_1km_f"] = lst_night_c.flatten() if lst_night_c is not None else np.nan

        df.drop(columns=["LST_Day_1km", "LST_Night_1km", "QC_Day", "QC_Night", "Emis_31", "Emis_32"], inplace=True)

        valid_f = np.isfinite(df["LST_Day_1km_f"]).sum()
        invalid_f = (~np.isfinite(df["LST_Day_1km_f"])).sum()
        total_f = valid_f + invalid_f
        print(f"Valid filtered pixels: {valid_f}, Invalid: {invalid_f}, Total: {total_f}")
        if total_f > 0 and invalid_f / total_f > 0.9:
            print(f"Skipping {date} for aid {aid_number}: >90% invalid filtered pixels.")
            return

        filter_csv_path = os.path.join(dest_filtered, f"{name}_{location}_{date}_MODISfilter.csv")
        df.to_csv(filter_csv_path, index=False)
        print(f"Saved filtered CSV: {filter_csv_path}")

        # Save processed LST data
        if lst_day_c is not None:
            lst_day_path = os.path.join(dest_filtered, f"{name}_{location}_{date}_lst_day.tif")
            meta = rasters["LST_Day_1km"].meta.copy()
            meta.update(dtype=rasterio.float32, count=1)
            with rasterio.open(lst_day_path, "w", **meta) as dst:
                dst.write(lst_day_c, 1)
                print(f"Saved daytime LST: {lst_day_path}")
                upload_to_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, lst_day_path, name, location)
                multi_files.append(lst_day_path)
        if lst_night_c is not None:
            lst_night_path = os.path.join(dest_filtered, f"{name}_{location}_{date}_lst_night.tif")
            meta = rasters["LST_Night_1km"].meta.copy()
            meta.update(dtype=rasterio.float32, count=1)
            with rasterio.open(lst_night_path, "w", **meta) as dst:
                dst.write(lst_night_c, 1)
                print(f"Saved nighttime LST: {lst_night_path}")
                upload_to_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, lst_night_path, name, location)
                multi_files.append(lst_night_path)

        multi_aids.add(aid_number)
        multi_files.extend([filter_csv_path, raw_csv_path])

        upload_to_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, filter_csv_path, name, location)
        upload_to_supabase(bucket_name, SUPABASE_URL, SUPABASE_KEY, raw_csv_path, name, location)

        log_file = os.path.join(log_path, f"updates_{timestamp}.txt")
        os.makedirs(log_path, exist_ok=True)
        with open(log_file, 'a', encoding='utf-8') as file:
            if lst_day_c is not None:
                file.write(f"Daytime LST {lst_day_path}\n")
            if lst_night_c is not None:
                file.write(f"Nighttime LST {lst_night_path}\n")
                file.write(f"Raw raster {raw_tif_path}\n")
                file.write(f"Raw CSV {raw_csv_path}\n")
                file.write(f"Filtered CSV {filter_csv_path}\n")

        print(f"Finished processing {date} (MODIS)")

# Usage: process_rasters(aid_number, date, selected_files, product_type="ECO" or "MODIS")

# Main function to process all new files using multiprocessing
def process_all(all_new_files):
    print(updated_aids)
    if not updated_aids:
        print("No new folders to process.")
        return
    
    print(f"Processing {len(updated_aids)} updated folders...")
            
    # Process each updated aid and its unique dates
    for aid_number in updated_aids:
        # Gather all files for this aid
        aid_files = [f for f in all_new_files if extract_metadata(f)[0] == aid_number]
        # Get unique dates for this aid
        unique_dates = set(extract_metadata(f)[1] for f in aid_files if extract_metadata(f)[1] is not None)
        if not unique_dates:
            print(f"No new files to process for aid {aid_number}.")
            continue
        for date in unique_dates:
            # Select files for this aid and date
            date_files = [f for f in aid_files if extract_metadata(f)[1] == date]
            # process_rasters(aid_number, date, date_files)
            process_rasters(aid_number, date, date_files, product_type="MODIS" if "MYD11A1" in date_files[0] else "ECO")
    print("Processing complete.")

### === 5. UPLOAD & CLEANUP FUNCTIONS ===

# Function to download TIF & CSV files to supabase bucket - create one to update shape file under Static?
def upload_to_supabase(bucket_name, supabase_url, supabase_key, file_path, name, location):
    log_file_path = f"updates_{timestamp}.txt"  # Each run creates a new file
    full_path = os.path.join(log_path, log_file_path)

    # Ensure the log directory exists
    os.makedirs(log_path, exist_ok=True)
    
    supabase: Client = create_client(supabase_url, supabase_key)
    supabase_dir = f"ECO/{name}/{location}"
    supabase_path = f"{supabase_dir}/{os.path.basename(file_path)}"

    # Ensure subdirectories exist in Supabase (Supabase creates folders implicitly on upload, but we can ensure by uploading a .keep file if needed)
    # Supabase creates folders implicitly on upload; no need to pre-create directories or .keep files.
    # Check if file already exists in Supabase
    try:
        existing_files = supabase.storage.from_(bucket_name).list(supabase_dir)
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

def cleanup_old_files(source, specified_doy, bucket_name=None, supabase_url=None, supabase_key=None):
    """
    Deletes files older than specified_doy (relative to today) from a local folder or Supabase bucket.
    Args:
        source (str): Local folder path or 'supabase' to use Supabase.
        specified_doy (int): Number of days of year to keep (files older than this will be deleted).
        cutoff_date (datetime, optional): Not used for local, used for Supabase as a cutoff date.
        bucket_name (str, optional): Supabase bucket name if using Supabase.
        supabase_url (str, optional): Supabase URL if using Supabase.
        supabase_key (str, optional): Supabase key if using Supabase.
    """
    log_file_path = f"updates_{timestamp}.txt"
    full_path = os.path.join(log_path, log_file_path)
    os.makedirs(log_path, exist_ok=True)
    current_date = datetime.strptime(timestamp[:8], "%Y%m%d")
    current_doy = current_date.timetuple().tm_yday

    if source == "supabase":
        if not all([bucket_name, supabase_url, supabase_key]):
            print("Supabase credentials missing.")
            return
        supabase: Client = create_client(supabase_url, supabase_key)
        folders = supabase.storage.from_(bucket_name).list("ECO/")
        for folder in folders:
            try:
                files = supabase.storage.from_(bucket_name).list(f"ECO/{folder['name']}/lake")
            except Exception as e:
                print(f"Error listing files in {folder['name']}: {e}")
                continue
            for file in files:
                _, date_str = extract_metadata(file['name'])
                if date_str and len(date_str) >= 7:
                    doy = int(date_str[4:7])
                    if doy < (current_doy - specified_doy):
                        file_path = f"ECO/{folder['name']}/lake/{file['name']}"
                        supabase.storage.from_(bucket_name).remove([file_path])
                        print(f"Deleted {file_path} (DOY {doy})")
                        with open(full_path, 'a', encoding='utf-8') as log_file:
                            log_file.write(f"Deleted {file_path} from Supabase\n")
                        deleted_files.append(file_path)
    else:
        # Assume source is a local folder path
        for root, _, files in os.walk(source):
            for filename in files:
                file_path = os.path.join(root, filename)
                _, date_str = extract_metadata(filename)
                if date_str and len(date_str) >= 7:
                    doy = int(date_str[4:7])
                    if doy < (current_doy - specified_doy):
                        os.remove(file_path)
                        print(f"Deleted {file_path} (DOY {doy})")
                        with open(full_path, 'a', encoding='utf-8') as log_file:
                            log_file.write(f"Deleted {file_path} from local\n")
                        deleted_files.append(file_path)

### === 6. LOGGING FUNCTION(S) ===

# Function to log updates concisely
def log_updates():
    file_path = f"completed_updates_{timestamp}.txt"
    full_path = os.path.join(log_path, file_path)
    log_data = {
        "timestamp": timestamp,
        "task_id": task_id,
        "start_date": start_date,
        "end_date": end_date,
        "updated_aids": list(updated_aids),
        "new_files": new_files,
        "multi_aids": list(multi_aids),
        "multi_files": multi_files,
        "deleted_files": deleted_files,
        "aid_folder_mapping": aid_folder_mapping,
    }
    with open(full_path, 'w', encoding='utf-8') as file:
        json.dump(log_data, file, indent=2)
    print(f"Updates saved to {full_path}.")


### *MAIN EXECUTION PHASES

# Phase 1: Submit task in one go
task_request = build_task_request(product_MODIS, layers_MODIS, roi_json, start_date, end_date)
task_id = submit_task(headers, task_request)
print("All tasks submitted!")
print(f"Task ID: {task_id}")

# Phase 2: Create Directories and Mapping
aid_folder_mapping = create_aid_folder_mapping(roi, eco_raw_path)

# Phase 3: Check the status of the single task
print("Checking task statuses...")
status = check_task_status(task_id, headers)
if status:
    print(f"Downloading results for Task ID: {task_id}...")
    download_results(task_id, headers)  # Pass the roi DataFrame for dynamic mapping    
print("All tasks completed, results downloaded!")

# Phase 4: Process the raster files
process_all(new_files)

# Phase 5: Cleanup old files
cleanup_old_files(source="supabase", specified_doy=90, bucket_name=bucket_name, supabase_url=SUPABASE_URL, supabase_key=SUPABASE_KEY)

# Phase 6: Log updates
log_updates()
