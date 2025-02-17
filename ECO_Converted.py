import os
import re
import time
import requests
import pandas as pd
import geopandas as gpd
import rasterio
import numpy as np
from datetime import datetime, timedelta

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
sd = "02-14-2025"

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
def process_rasters(new_files):
    for tif_file in new_files:
        if tif_file.endswith('.tif'):
            print(f"Processing file: {tif_file}")
            # Extract the aid number from the file path
            aid_match = re.search(r'aid\d{4}', tif_file)
            if aid_match:
                aid_number = aid_match.group(0)
                print(f"Processing raster for aid number: {aid_number}")
                name, location = aid_folder_mapping.get(aid_number, (None, None))
            
                if name is None or location is None:
                    print(f"No mapping found for aid: {aid_number}")
                    continue

                # Define the destination directory structure in the ECO folder
                dest_folder_raw = os.path.join(raw_path, name, location)
                dest_folder_filtered = os.path.join(filtered_path, name, location)
                os.makedirs(dest_folder_filtered, exist_ok=True)
                print(f"Destination folder created: {dest_folder_filtered}")

                # NEWNEWNEWNEW

                # Initialize raster variables as None to ensure they exist
                LST = LST_err = QC = wt = cl = emis = heig = None

                # Define a function to read raster or return NaN if not found
                def read_raster_or_nan(tif_file, raster_name):
                    if raster_name in tif_file:
                        with rasterio.open(tif_file) as src:
                            return src.read(1)  # Read the first band
                    else:
                        return None  # Return None if raster is not found

                # Iterate through the tif files (assuming tif_files is a list of file paths)
                if "LST_doy" in tif_file:
                    LST = read_raster_or_nan(tif_file, "LST_doy")
                if "LST_err" in tif_file:
                    LST_err = read_raster_or_nan(tif_file, "LST_err")
                if "QC" in tif_file:
                    QC = read_raster_or_nan(tif_file, "QC")
                if "water" in tif_file:
                    wt = read_raster_or_nan(tif_file, "water")
                if "cloud" in tif_file:
                    cl = read_raster_or_nan(tif_file, "cloud")
                if "Emis" in tif_file:
                    emis = read_raster_or_nan(tif_file, "Emis")
                if "height" in tif_file:
                    heig = read_raster_or_nan(tif_file, "height")

                # Ensure the data is read or replaced with NaN
                # Get shape from the first raster (if any)
                if LST is not None:
                    raster_shape = LST.shape
                else:
                    raster_shape = (0, 0)  # Default shape if no rasters were read

                # Replace None rasters with NaN arrays of the correct shape
                LST_data = LST if LST is not None else np.full(raster_shape, np.nan)
                LST_err_data = LST_err if LST_err is not None else np.full(raster_shape, np.nan)
                QC_data = QC if QC is not None else np.full(raster_shape, np.nan)
                wt_data = wt if wt is not None else np.full(raster_shape, np.nan)
                cl_data = cl if cl is not None else np.full(raster_shape, np.nan)
                emis_data = emis if emis is not None else np.full(raster_shape, np.nan)
                heig_data = heig if heig is not None else np.full(raster_shape, np.nan)

                # Now, ensure all rasters are the same shape
                def ensure_same_shape(*rasters):
                    # Get the max shape (height, width)
                    max_shape = max([raster.shape for raster in rasters if raster is not None], key=lambda x: x)
                    reshaped_rasters = []
                    for raster in rasters:
                        if raster is not None:
                            # Resize if shape doesn't match the max shape
                            if raster.shape != max_shape:
                                raster_resized = np.full(max_shape, np.nan)  # Create a new array with the max shape
                                raster_resized[:raster.shape[0], :raster.shape[1]] = raster  # Fill with original data
                                reshaped_rasters.append(raster_resized)
                            else:
                                reshaped_rasters.append(raster)
                        else:
                            reshaped_rasters.append(np.full(max_shape, np.nan))  # If None, fill with NaN
                    return reshaped_rasters

                # Ensure all rasters are the same shape
                rasters = ensure_same_shape(LST_data, LST_err_data, QC_data, wt_data, cl_data, emis_data, heig_data)

                # Stack the rasters into a single array
                raster_stack = np.stack(rasters, axis=-1)

                # Save the stacked raster as a new file
                with rasterio.open(os.path.join(dest_folder_raw, os.path.basename(tif_file).replace(".tif", "_raw.tif")), 'w',
                                driver='GTiff', 
                                count=raster_stack.shape[-1], 
                                dtype=raster_stack.dtype, 
                                width=raster_stack.shape[1], 
                                height=raster_stack.shape[0], 
                                crs=LST.crs if LST else None, 
                                transform=LST.transform if LST else None) as dst:
                    for i in range(raster_stack.shape[-1]):
                        dst.write(raster_stack[:, :, i], i + 1)

                # Convert raster to dataframe
                data = []
                for y in range(raster_shape[0]):
                    for x in range(raster_shape[1]):
                        data.append([x, y, LST_data[y, x], LST_err_data[y, x], QC_data[y, x], 
                                    wt_data[y, x], cl_data[y, x], emis_data[y, x], heig_data[y, x]])

                bdf = pd.DataFrame(data, columns=["x", "y", "LST", "LST_err", "QC", "wt", "cloud", "emis", "height"])

                # Save to CSV
                bdf.to_csv(os.path.join(dest_folder_raw, os.path.basename(tif_file).replace(".tif", "_raw.csv")))

                # Filtering step
                bdf['LST_filter'] = np.where(bdf['QC'].isin([15, 2501, 3525, 65535]), np.nan, bdf['LST'])
                bdf['LST_err_filter'] = np.where(bdf['QC'].isin([15, 2501, 3525, 65535]), np.nan, bdf['LST_err'])
                bdf['QC_filter'] = np.where(bdf['QC'].isin([15, 2501, 3525, 65535]), np.nan, bdf['QC'])
                bdf['emis_filter'] = np.where(bdf['QC'].isin([15, 2501, 3525, 65535]), np.nan, bdf['emis'])
                bdf['heig_filter'] = np.where(bdf['QC'].isin([15, 2501, 3525, 65535]), np.nan, bdf['height'])

                # Cloud filtering
                bdf['LST_filter'] = np.where(bdf['cloud'] == 1, np.nan, bdf['LST_filter'])
                bdf['LST_err_filter'] = np.where(bdf['cloud'] == 1, np.nan, bdf['LST_err_filter'])
                bdf['QC_filter'] = np.where(bdf['cloud'] == 1, np.nan, bdf['QC_filter'])
                bdf['emis_filter'] = np.where(bdf['cloud'] == 1, np.nan, bdf['emis_filter'])
                bdf['heig_filter'] = np.where(bdf['cloud'] == 1, np.nan, bdf['heig_filter'])

                # Water filtering
                bdf['LST_filter'] = np.where(bdf['wt'] == 0, np.nan, bdf['LST_filter'])
                bdf['LST_err_filter'] = np.where(bdf['wt'] == 0, np.nan, bdf['LST_err_filter'])
                bdf['QC_filter'] = np.where(bdf['wt'] == 0, np.nan, bdf['QC_filter'])
                bdf['emis_filter'] = np.where(bdf['wt'] == 0, np.nan, bdf['emis_filter'])
                bdf['heig_filter'] = np.where(bdf['wt'] == 0, np.nan, bdf['heig_filter'])

                # Save filtered dataframe
                bdf.to_csv(os.path.join(dest_folder_filtered, os.path.basename(tif_file).replace(".tif", "_filtered.csv")))

                # Rebuild .tif files with filtered data
                LST_filt = np.array(bdf['LST_filter'].values).reshape(raster_shape)
                LST_err_filt = np.array(bdf['LST_err_filter'].values).reshape(raster_shape)
                LST_QC_filt = np.array(bdf['QC_filter'].values).reshape(raster_shape)
                LST_emis_filt = np.array(bdf['emis_filter'].values).reshape(raster_shape)
                LST_heig_filt = np.array(bdf['heig_filter'].values).reshape(raster_shape)

                # Create a new raster with filtered data
                """
                File "C:\Users\abdul\Documents\Uni\y2\2019 (SEGP)\sat-water-temps\ECO_Converted.py", line 330, in process_rasters
                    with rasterio.open(os.path.join(dest_folder_filtered, os.path.basename(tif_file).replace(".tif", "_filtered.tif")), 'w',
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                File "C:\Users\abdul\AppData\Local\Programs\Python\Python312\Lib\site-packages\rasterio\env.py", line 463, in wrapper
                    return f(*args, **kwds)
                        ^^^^^^^^^^^^^^^^
                File "C:\Users\abdul\AppData\Local\Programs\Python\Python312\Lib\site-packages\rasterio\__init__.py", line 378, in open
                    dataset = writer(
                            ^^^^^^^
                File "rasterio\\_io.pyx", line 1540, in rasterio._io.DatasetWriterBase.__init__
                    rasterio.errors.RasterioIOError: Attempt to create 0x0 dataset is illegal,sizes must be larger than zero.
                """
                with rasterio.open(os.path.join(dest_folder_filtered, os.path.basename(tif_file).replace(".tif", "_filtered.tif")), 'w', 
                                driver='GTiff', 
                                count=5, 
                                dtype=LST_data.dtype, 
                                crs=LST.crs if LST else None, 
                                transform=LST.transform if LST else None, 
                                width=raster_shape[1], 
                                height=raster_shape[0]) as out_raster:
                    out_raster.write(LST_filt, 1)
                    out_raster.write(LST_err_filt, 2)
                    out_raster.write(LST_QC_filt, 3)
                    out_raster.write(LST_emis_filt, 4)
                    out_raster.write(LST_heig_filt, 5)

                # NEWNEWNEWNEW

                # # Open the original TIFF file
                # with rasterio.open(tif_file) as src:
                #     print(f"Opened TIFF file: {tif_file}")
                #     # Extract metadata
                #     meta = src.meta
                #     bands = {}
                #     if "LST" in tif_file:
                #         bands["LST"] = src.read(1)
                #         print(f"Shape of LST band: {bands["LST"]}")  # Should be (height, width)
                #     elif "LST_err" in tif_file:
                #         bands["LST_err"] = src.read(1)
                #         print(f"Shape of LST_err band: {bands["LST_err"]}")  # Should be (height, width)
                #     elif "QC" in tif_file:
                #         bands["QC"] = src.read(1)
                #         print(f"Shape of QC band: {bands["QC"]}")  # Should be (height, width)
                #     elif "water" in tif_file:
                #         bands["water"] = src.read(1)
                #         print(f"Shape of water band: {bands["water"]}")  # Should be (height, width)
                #     elif "cloud" in tif_file:
                #         bands["cloud"] = src.read(1)
                #         print(f"Shape of cloud band: {bands["cloud"]}")  # Should be (height, width)
                #     elif "EmisWB" in tif_file:
                #         bands["EmisWB"] = src.read(1)
                #         print(f"Shape of EmisWB band: {bands["EmisWB"]}")  # Should be (height, width)
                #     elif "height" in tif_file:
                #         bands["height"] = src.read(1)
                #         print(f"Shape of height band: {bands["height"]}")  # Should be (height, width)

                #     # Convert raster to DataFrame
                #     bdf = pd.DataFrame({
                #         "x": np.repeat(np.arange(bands["LST"].shape[1]), bands["LST"].shape[0]),
                #         "y": np.tile(np.arange(bands["LST"].shape[0]), bands["LST"].shape[1]),
                #         **bands
                #     })
                #     print(f"Converted raster to DataFrame for file: {tif_file}")

                #     # Apply filters
                #     qc_mask = bdf["QC"].isin([15, 2501, 3525, 65535])
                #     cloud_mask = bdf["cloud"] == 1
                #     water_mask = bdf["water"] == 0

                #     for key in ["LST", "LST_err", "QC", "EmisWB", "height"]:
                #         bdf[f"{key}_filter"] = bdf[key]
                #         bdf.loc[qc_mask | cloud_mask | water_mask, f"{key}_filter"] = np.nan
                        
                #     print(f"Applied filters for file: {tif_file}")

                    # Save filtered CSV
                    # csv_filename = os.path.join(dest_folder_filtered, os.path.basename(tif_file).replace(".tif", "_filtered.csv"))
                    # os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
                    # bdf.to_csv(csv_filename, index=False)
                    # print(f"Saved filtered CSV: {csv_filename}")
                        
                    # # Rebuild filtered raster
                    # filtered_rasters = [bdf[f"{key}_filter"].values.reshape(meta['height'], meta['width']) for key in ["LST", "LST_err", "QC", "EmisWB", "height"]]
                    # meta.update({"count": len(filtered_rasters)})
                    # filtered_file = os.path.join(dest_folder_filtered, os.path.basename(tif_file).replace(".tif", "_filtered.tif"))
                        
                    # with rasterio.open(filtered_file, "w", **meta) as dst:
                    #     for idx, layer in enumerate(filtered_rasters, start=1):
                    #         dst.write(layer, idx)
                    # print(f"Saved filtered raster: {filtered_file}")

                    # final_tif = os.path.join(dest_folder_filtered, "multily_filt.tif")
                    # with rasterio.open(final_tif, "w", **meta) as dst:
                    #     for idx, layer in enumerate(filtered_rasters, start=1):
                    #         dst.write(layer, idx)
                    # print(f"Saved final multilayer GeoTIFF: {final_tif}")

                    # lst = src.read(1)  # Load LST layer
                    # lst_filtered = np.where(lst == -9999, np.nan, lst)  # Replace NoData with NaN

                    # # Define path for the filtered file
                    # filtered_file = os.path.join(dest_folder_filtered, os.path.basename(tif_file).replace(".tif", "_filtered.tif"))

                    # # Save the filtered data to the new path
                    # with rasterio.open(
                    #     filtered_file,
                    #     'w',
                    #     driver='GTiff',
                    #     height=lst_filtered.shape[0],
                    #     width=lst_filtered.shape[1],
                    #     count=1,
                    #     dtype='float32',
                    #     crs=src.crs,
                    #     transform=src.transform
                    # ) as dst:
                    #     dst.write(lst_filtered, 1)
                    #     print(f"Filtered raster saved: {filtered_file}")


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
                            if "LST_doy" in f:
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