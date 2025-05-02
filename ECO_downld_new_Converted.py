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
from dotenv import load_dotenv
load_dotenv()
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Directory paths
print("Setting Directory Paths")

base_dir = os.path.dirname(os.path.abspath(__file__))
roi_test_path = os.path.join(base_dir, "polygon/test/site_full_ext_Test.shp")
raw_path = os.path.join(base_dir, "ECOraw")
filtered_path = os.path.join(base_dir, "ECO")
roi_path = os.path.join(base_dir, "polygon/new_polygons.shp")
log_path = os.path.join(base_dir, "logs")

#Verify File Paths
if not os.path.exists(roi_test_path):
    raise FileNotFoundError(f"The ROI shapefile does not exist at {roi_test_path}")

try:
    roi = gpd.read_file(roi_test_path)
except Exception as e:
    raise ValueError(f"Could not read the shapefile: {e}")

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
user = 'JephthaT'
password = '1#Big_Chilli'

if not user or not password:
    raise ValueError("Earthdata credentials are not set. Please set APPEEARS_USER and APPEEARS_PASS as environment variables.")

# Generate a timestamp for this run (format: YYYYMMDD_HHMM) for uniqueness in filenames
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# Get Today Date As End Date
print("Setting Dates")
today_date = datetime.now()
today_date_str = today_date.strftime("%m-%d-%Y")
ed = today_date_str
# ed = "04-01-2025"


# Get Yesterday Date as Start Date
yesterday_date = today_date - timedelta(days=1)
yesterday_date_str = yesterday_date.strftime("%m-%d-%Y")
sd = yesterday_date_str
# sd = "03-26-2025"
# sd = "08-01-2023"

# KEY RESULTS TO STORE/LOG
updated_aids = set()
new_files = []
new_dates = []
multi_aids = set()
multi_files = []
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

        try:
            response_json = response.json()
            if "status" not in response_json:
                print(f"Unexpected response format: {response_json}")
                raise KeyError("Missing 'status' in response JSON.")
            status = response_json["status"]
        except json.JSONDecodeError:
            print(f"Non-JSON response received: {response.text}")
            raise
        except KeyError as e:
            raise Exception(f"Invalid response for task status: {response.text}") from e

        if status == "done":
            print(f"Task {task_id} is complete!")
            return True
        elif status in ["processing", "queued", "pending"]:
            print(f"Task {task_id} is {status}. Checking again in 30 seconds...")
            time.sleep(30)
        else:
            raise Exception(f"Task failed or returned unknown status: {status}")

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

    # Water Mask Flag might be Tripping. UbolRatana, NamNgum for example, has water mask = 1 all over, still returning _wtoff

    if not df["wt"].isin([1]).any():
        # print("Water not detected.")
        water_mask_flag = False
    else:
        water_mask_flag = True

    # Apply filtering
    for col in ["LST", "LST_err", "QC", "EmisWB", "height"]:
        df[f"{col}_filter"] = np.where(df["QC"].isin(INVALID_QC_VALUES), np.nan, df[col])

    for col in ["LST_filter", "LST_err_filter", "QC_filter", "EmisWB_filter", "height_filter"]:
        df[f"{col}"] = np.where(df["cloud"] == 1, np.nan, df[col])

    if water_mask_flag:
        for col in ["LST_filter", "LST_err_filter", "QC_filter", "EmisWB_filter", "height_filter"]:
            df[f"{col}"] = np.where(df["wt"] == 0, np.nan, df[col])
        filter_csv_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter_wtoff.csv")
        filter_tif_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter_wtoff.tif")

    for col in ["LST", "LST_err", "QC", "EmisWB"]:
        df.drop(columns=[f"{col}"], inplace=True)

    df.dropna(subset=["LST_filter"], inplace=True)

    # Save filtered CSV
    df.to_csv(filter_csv_path, index=False)
    multi_files.append(filter_csv_path)
    print(f"Saved filtered CSV: {filter_csv_path}")

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

    # Save raster metadata to a .txt file
    metadata_file_path = os.path.join(dest_folder_filtered, f"{name}_{location}_metadata.txt")
    with open(metadata_file_path, 'w') as meta_file:
        meta_file.write(str(filter_meta))
    print(f"Saved raster metadata: {metadata_file_path}")

    print(f"Saved filtered raster: {filter_tif_path}")
    print(f"Finished processing {date}")

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

# Phase 5: Use this cleanup function after the main processing - cleanup_old_files(raw_path, days_old=20)
def cleanup_old_files(folder_path, days_old=20):

    # Calculate the cutoff time
    cutoff_time = datetime.now() - timedelta(days=days_old)

    # Iterate through each file in the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        # Only proceed if it's a file
        if os.path.isfile(file_path):

            # Get the file's modification time
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))

            # Delete file if it's older than the cutoff time
            if file_mod_time < cutoff_time:
                os.remove(file_path)
                print(f"Deleted {filename} (last modified on {file_mod_time})")

def log_updates():
    # Open the log file in append mode
    file_path = f"updates_{timestamp}.txt"  # Each run creates a new file
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
        
        # Log dictionary update
        file.write("[Aid Folder Mapping]\n")
        file.write(json.dumps(aid_folder_mapping, indent=4))  # Format JSON output
        file.write("\n\n")

    print(f"Updates saved to {full_path}.")

# Phase 1: Submit task in one go
task_request = build_task_request(product, layers, roi_json, sd, ed)
task_id = submit_task(headers, task_request)
#task_id = "1b73ef44-9c05-4635-8daa-0fd4fe015b9f" # 03/03/2025 - 03/08/2025
print(f"Task ID: {task_id}")

# # Phase 2: Create Directories and Mapping
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

# Phase 3: Check the status of the single task
print("All tasks submitted!")
print("Checking task statuses...")
status = check_task_status(task_id, headers)
if status:
    print(f"Downloading results for Task ID: {task_id}...")
    download_results(task_id, headers)  # Pass the roi DataFrame for dynamic mapping    
print("All tasks completed, results downloaded!")

# Phase 4: Process the downloaded files
process_all(new_files)

# Phase 5: Cleanup old files
# cleanup_old_files(raw_path, days_old=20)

# Phase 6: Log updates
log_updates()

def clean_filtered_csvs(filtered_path):
    for root, _, files in os.walk(filtered_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_csv(file_path)
                    for col in ["LST", "LST_err", "QC", "EmisWB"]:
                        df.drop(columns=[f"{col}"], inplace=True)
                    df.dropna(subset=["LST_filter"], inplace=True)
                    df.to_csv(file_path, index=False)
                    print(f"Cleaned file: {file_path}")
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")
                    
def clean_filtered_tifs(filtered_path):
    for root, _, files in os.walk(filtered_path):
        for file in files:
            if file.endswith(".tif"):
                file_path = os.path.join(root, file)
                try:
                    with rasterio.open(file_path, "r+") as src:
                        data = src.read(1)  # Read the first band
                        data[data == src.nodata] = np.nan  # Replace nodata values with NaN
                        data = np.where(np.isnan(data), src.nodata, data)  # Remove pixels with NaN
                        src.write(data, 1)  # Write the cleaned data back to the file
                    print(f"Cleaned file: {file_path}")
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

# def convert_csv_to_tif():
    # """
    # Converts a single CSV file to a GeoTIFF file using a reference raster for geospatial metadata.

    # Args:
    #     csv_path (str): Path to the CSV file.
    #     output_tif_path (str): Path to the output GeoTIFF file.
    #     reference_raster_path (str): Path to a reference raster file for geospatial metadata.
    # """

    # csv_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\ECO\Ambuclao\lake\Ambuclao_lake_2025047192336_filter.csv"
    # output_tif_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\ECO\Ambuclao\lake\testtest.tif"
    # reference_raster_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors\ECO\Ambuclao\lake\Ambuclao_lake_2025047192336_filter.tif"
    # # Load reference raster for metadata
    # with rasterio.open(reference_raster_path) as ref_raster:
    #     ref_meta = ref_raster.meta.copy()
    #     rows, cols = ref_meta['height'], ref_meta['width']

    #     print(f"Reference Metadata: {ref_meta}")
    # try:
    #     # Read CSV data
    #     df = pd.read_csv(csv_path)
    #     if "x" not in df.columns or "y" not in df.columns:
    #         print(f"Skipping {csv_path}: Missing 'x' or 'y' columns.")
    #         return

    #     # Extract raster data from DataFrame
    #     raster_data = np.full((rows, cols), np.nan, dtype=np.float32)
    #     raster_data[df["y"], df["x"]] = df["LST_filter"]

    #     # Update metadata for single-band raster
    #     ref_meta.update(dtype=rasterio.float32, count=1)

    #     # Save to GeoTIFF
    #     with rasterio.open(output_tif_path, "w", **ref_meta) as dst:
    #         dst.write(raster_data, 1)

    #     print(f"Converted {csv_path} to {output_tif_path}")
    # except Exception as e:
    #     print(f"Error processing {csv_path}: {e}")

# # Phase 7: Reconstruct the filtered CSVs
# clean_filtered_csvs(filtered_path)