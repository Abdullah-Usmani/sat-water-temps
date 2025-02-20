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
raw_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/ECOraw/"
filtered_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/ECO/"
roi_path = r"C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\Water Temp Sensors/polygon/corrected/site_full_ext_corrected.shp"

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
user = 'abdullahusmani1'
password = 'haziqLOVERS123!'

# Generate a timestamp for this run (format: YYYYMMDD_HHMM) for uniqueness in filenames
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# Get Today Date As End Date
print("Setting Dates")
today_date = datetime.now()
today_date_str = today_date.strftime("%m-%d-%Y")
ed = today_date_str
# ed = "02-04-2025"

# Get Yesterday Date as Start Date
yesterday_date = today_date - timedelta(days=1)
yesterday_date_str = yesterday_date.strftime("%m-%d-%Y")
# sd = yesterday_date_str
sd = "02-13-2025"

token = get_token(user, password)
print(token)

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
    new_files = []  # List to keep track of just-downloaded files

    # Step 1: Download files and group by aid
    for file in files:
        file_id = file['file_id']
        file_name = file['file_name']
        aid_match = re.search(r'aid(\d{4})', file_name)  # Extract aid number from filename

        if aid_match:
            aid_number = aid_match.group(0)  # Get the full aid number string (e.g., "aid0001")
            name, location = aid_folder_mapping.get(aid_number, (None, None))
            output_folder = os.path.join(raw_path, name, location)
            
            if output_folder is not None:
                # Ensure output folder exists and strip preceding folder in file_name if present
                os.makedirs(output_folder, exist_ok=True)
                file_name_stripped = file_name.split('/')[-1]
                local_filename = os.path.join(output_folder, file_name_stripped)

                print(f"Downloading to: {local_filename}")
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

            print(f"Downloading to base folder: {local_filename}")

            download_url = f"{url}/{file_id}"
            download_response = requests.get(download_url, headers=headers, stream=True, allow_redirects=True)

            with open(local_filename, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    f.write(chunk)

            new_files.append(local_filename)  # Track newly downloaded file
            print(f"Downloaded {local_filename}")

    # Step 2: Process each aid_folder once all files are downloaded
    print("Processing rasters")
    process_rasters(new_files)  # Pass only newly downloaded files to process


# Updated process_rasters function to handle only newly downloaded files, and apply filters according to Dr Matteo's R model
# Invalid QC values
INVALID_QC_VALUES = {15, 2501, 3525, 65535}

# Function to extract aid number and date from filename
def extract_metadata(filename):
    aid_match = re.search(r'aid\d{4}', filename)
    date_match = re.search(r'doy\d{7}', filename)

    aid_number = aid_match.group(0) if aid_match else None
    date = date_match.group(0) if date_match else None

    return aid_number, date

# Function to filter only new files and return unique dates
def get_new_dates(new_files):
    return {extract_metadata(f)[1] for f in new_files if extract_metadata(f)[1]}

# Function to read a specific raster layer
def read_raster(layer_name, relevant_files):
    matches = [f for f in relevant_files if layer_name in f]
    return rasterio.open(matches[0]) if matches else None

# Function to read raster as NumPy array
def read_array(raster):
    return raster.read(1) if raster else None

# Function to process a single date
def process_date(date, new_files):
    print(f"Processing date: {date}")

    relevant_files = [f for f in new_files if date in f]
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
    aid_number = extract_metadata(relevant_files[0])[0]
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

    # Apply filtering
    for col in ["LST", "LST_err", "QC", "EmisWB", "height"]:
        df[f"{col}_filter"] = np.where(df["QC"].isin(INVALID_QC_VALUES), np.nan, df[col])

    for col in ["LST_filter", "LST_err_filter", "QC_filter", "EmisWB_filter", "height_filter"]:
        df[f"{col}"] = np.where(df["cloud"] == 1, np.nan, df[col])
        # df[f"{col}"] = np.where(df["wt"] == 0, np.nan, df[col])

    # Save filtered CSV
    filter_csv_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter.csv")
    df.to_csv(filter_csv_path, index=False)
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
    filter_tif_path = os.path.join(dest_folder_filtered, f"{name}_{location}_{date}_filter.tif")
    filter_meta = filtered_rasters["LST"][1].copy()
    filter_meta.update(dtype=rasterio.float32, count=len(filtered_rasters))  # Correct band count

    # Save filtered raster
    with rasterio.open(filter_tif_path, "w", **filter_meta) as dst:
        for idx, (key, (data, _)) in enumerate(filtered_rasters.items(), start=1):
            dst.write(data, idx)  # Ensure correct band range
    print(f"Saved filtered raster: {filter_tif_path}")

    print(f"Finished processing {date}")

# Main function to process all new files using multiprocessing
def process_rasters(new_files):
    new_dates = get_new_dates(new_files)
    for date in new_dates:
        print(f"{date} is a new date")
    if not new_dates:
        print("No new files to process.")
        return

    print(f"Processing {len(new_dates)} new dates...")
    for date in new_dates:
        process_date(date, new_files)

    print("Processing complete.")

# Run the processing
print("Processing complete.")

# Phase 1: Submit task in one go
# task_id = "abbc9d8c-a439-44f6-8756-723cb54129e9"
task_id = "fedc736e-523c-43c7-9d88-484d70e4156a"
# task_request = build_task_request(product, layers, roi_json, sd, ed)
# task_id = submit_task(headers, task_request)
print(f"Task ID: {task_id}")

# Phase 2: Create Directories and Mapping
aid_folder_mapping = {}  # Initialize mapping outside the loop
for idx, row in roi.iterrows():
    print(f"Processing ROI {idx + 1}/{len(roi)}")
    
    # Construct directory path for saving data
    output_folder = os.path.join(raw_path, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder created: {output_folder}")
    
    # Map aid numbers to output folders
    aid_number = f'aid{str(idx + 1).zfill(4)}'  # Construct aid number
    aid_folder_mapping[aid_number] = (row['name'], row['location'])  # Map aid number to folder

# Phase 3: Check the status of the single task
print("All tasks submitted!")
print("Checking task statuses...")
status = check_task_status(task_id, headers)
if status:
    print(f"Downloading results for Task ID: {task_id}...")
    download_results(task_id, headers)  # Pass the roi DataFrame for dynamic mapping    
print("All tasks completed, results downloaded, and rasters processed.")

# Phase 4: Use this cleanup function after the main processing - cleanup_old_files(raw_path, days_old=20)
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
    if 'EmisWB_filter' not in bdf.columns:
        print("Column 'EmisWB_filter' does not exist in the DataFrame.")
    else:
        bdf['EmisWB_filter'] = np.where(bdf['QC'].isin(qc_filter_values), np.nan, bdf['EmisWB'])
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
def save_filtered_data(bdf, filtered_path):
    bdf.to_csv(filtered_path + '_filterequests.csv', index=False)
    # Rebuild raster from filtered data
    with rasterio.open(filtered_path + '_filterequests.tif', 'w', **meta) as dest:
        dest.write(bdf['LST_filter'].values.reshape(shape), 1)

# Example usage
bdf = pd.DataFrame({
    'LST': [1, 2, 3],
    'QC': [15, 0, 3525],
    'cloud': [0, 1, 0],
    'wt': [1, 1, 0]
})

filtered_bdf = filter_data(bdf)
save_filtered_data(filtered_bdf, "filtered_path")
"""


"""
            if os.path.isdir(dr_sub_path):
                fl = [os.path.join(dr_sub_path, f) for f in os.listdir(dr_sub_path) if f.endswith(".tif")]
                
                # Extract metadata
                dt = list(set([f.split("doy")[-1].split("_")[0] for f in fl]))
                ly = list(set([f.split("002_")[-1].split("_doy")[0] for f in fl]))
                prj = list(set([f.split("01_")[-1].split(".tif")[0] for f in fl]))
                
                for date in dt:
                    matching_files = [f for f in fl if date in f]
                    bands = {}
                    
                    # Read relevant raster bands
                    for f in matching_files:
                        with rasterio.open(f) as src:
                            if "LST" in f:
                                bands["LST"] = src.read(1)
                            elif "LST_err" in f:
                                bands["LST_err"] = src.read(1)
                            elif "QC" in f:
                                bands["QC"] = src.read(1)
                            elif "water" in f:
                                bands["water"] = src.read(1)
                            elif "cloud" in f:
                                bands["cloud"] = src.read(1)
                            elif "EmisWB" in f:
                                bands["EmisWB"] = src.read(1)
                            elif "height" in f:
                                bands["height"] = src.read(1)
                            meta = src.meta
                    
                    # Convert raster to DataFrame
                    bdf = pd.DataFrame({
                        "x": np.repeat(np.arange(bands["LST"].shape[1]), bands["LST"].shape[0]),
                        "y": np.tile(np.arange(bands["LST"].shape[0]), bands["LST"].shape[1]),
                        **bands
                    })
                    
                    # Apply filters
                    qc_mask = bdf["QC"].isin([15, 2501, 3525, 65535])
                    cloud_mask = bdf["cloud"] == 1
                    water_mask = bdf["water"] == 0
                    
                    for key in ["LST", "LST_err", "QC", "EmisWB", "height"]:
                        bdf[f"{key}_filter"] = bdf[key]
                        bdf.loc[qc_mask | cloud_mask | water_mask, f"{key}_filter"] = np.nan
                    
                    # Save filtered CSV
                    csv_filename = f"{ptout}{dr}/{dr_sub}/{dr}_{dr_sub}_{date}_{prj[0]}_filter.csv"
                    os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
                    bdf.to_csv(csv_filename, index=False)
                    
                    # Rebuild filtered raster
                    filtered_rasters = [bdf[f"{key}_filter"].values.reshape(meta['height'], meta['width']) for key in ["LST", "LST_err", "QC", "EmisWB", "height"]]
                    meta.update({"count": len(filtered_rasters)})
                    tif_filename = f"{ptout}{dr}/{dr_sub}/{dr}_{dr_sub}_{date}_{prj[0]}_filter.tif"
                    
                    with rasterio.open(tif_filename, "w", **meta) as dst:
                        for idx, layer in enumerate(filtered_rasters, start=1):
                            dst.write(layer, idx)

# Save final multilayer GeoTIFF
final_tif = os.path.join(ptout, "multily_filt.tif")
with rasterio.open(final_tif, "w", **meta) as dst:
    for idx, layer in enumerate(filtered_rasters, start=1):
        dst.write(layer, idx)
"""