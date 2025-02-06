import os
import time
import requests
import geopandas as gpd
import rasterio
import json
import re
from rasterio.merge import merge
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import mysql.connector
from mysql.connector import Error

# Directory paths
print("Setting Directory Paths")
pt = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors_Jephtha/ECOraw/"
output_path = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors_Jephtha/ECO/"
roi_path = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors_Jephtha/polygon/test/site_full_ext_Test.shp"

# Database Credentials
MYSQL_HOST = "localhost"
MYSQL_USER = "Jephtha_T"
MYSQL_PASSWORD = "1#Big_Chilli"
MYSQL_DATABASE = "ecodata"

# Azure Blob Storage credentials
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=ecodatasat;AccountKey=y4YZhr/kS10qLPGhXff3rrcydXD960w5yrxPsaWdGlcZkOACDm+YJyS79YCCkwFTdAWu/PrdC4y7+AStXFsUrA==;EndpointSuffix=core.windows.net"  # From Azure Portal
CONTAINER_NAME = "ecostress-data"  # Name of your container

# Define Earthdata login credentials (Replace with your actual credentials)
user = 'JephthaT'
password = '1#Big_Chilli'

if not os.path.exists(roi_path):
    raise FileNotFoundError(f"The ROI shapefile does not exist at {roi_path}")

try:
    roi = gpd.read_file(roi_path)
except Exception as e:
    raise ValueError(f"Could not read the shapefile: {e}")

# Function to Create Database Connection
def create_db_connection():
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

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
# ed = "10-01-2024"

# Get Yesterday Date as Start Date
yesterday_date = today_date - timedelta(days=1)
yesterday_date_str = yesterday_date.strftime("%m-%d-%Y")
# sd = yesterday_date_str
sd = "11-01-2024"

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
def download_results(task_id, headers):
    url = f"https://appeears.earthdatacloud.nasa.gov/api/bundle/{task_id}"
    response = requests.get(url, headers=headers)
    files = response.json()['files']
    
    # Dictionary to group files by aid folder
    aid_files = {}

    # Step 1: Download files and group by aid
    for file in files:
        file_id = file['file_id']
        file_name = file['file_name']
        aid_match = re.search(r'aid(\d{4})', file_name)  # Extract aid number from filename

        if aid_match:
            aid_number = aid_match.group(0)  # Get the full aid number string (e.g., "aid0001")
            output_folder = aid_folder_mapping.get(aid_number)  # Get corresponding output folder

            if output_folder is not None:
                # Ensure output folder exists and strip preceding folder in file_name if present
                os.makedirs(output_folder, exist_ok=True)
                file_name_stripped = file_name.split('/')[-1]
                local_filename = os.path.join(output_folder, file_name_stripped)

                print(f"Downloading to: {local_filename}")
                download_url = f"{url}/{file_id}"
                download_response = requests.get(download_url, headers=headers, stream=True, allow_redirects=True)

                # Save the file locally temporarily
                with open(local_filename, 'wb') as f:
                    for chunk in download_response.iter_content(chunk_size=8192):
                        f.write(chunk)

                print(f"Downloaded {local_filename}")

                processed_file = process_rasters(local_filename)

                # Upload the file to Azure Blob Storage
                blob_name = f"{aid_number}/{os.path.basename(processed_file)}"  # Use aid_number as folder in Azure
                file_url = upload_to_azure_blob(processed_file, blob_name)

                # Insert file metadata into MySQL
                region_id = int(aid_number.replace("aid", ""))  # Extract region_id from aid_number
                file_type = "GeoTIFF" if processed_file.endswith(".tif") else "CSV"  # Adjust based on file type
                insert_file(task_id, region_id, os.path.basename(processed_file), file_url, file_type)

                # Add the file to the aid_files dictionary for later processing
                if aid_number not in aid_files:
                    aid_files[aid_number] = output_folder  # Track the folder for each aid


        else:
            # Handle general files without aid numbers (e.g., XML, CSV, JSON)
            local_filename = os.path.join(pt, file_name)  # Save directly to the base folder
            print(f"Downloading to base folder: {local_filename}")

            download_url = f"{url}/{file_id}"
            download_response = requests.get(download_url, headers=headers, stream=True, allow_redirects=True)

            with open(local_filename, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded {local_filename}")
            # Upload the file to Azure Blob Storage
            blob_name = f"{aid_number}/{file_name}"  # Use aid_number as folder in Azure
            file_url = upload_to_azure_blob(local_filename, blob_name)

            # Insert file metadata into MySQL
            region_id = int(aid_number.replace("aid", ""))  # Extract region_id from aid_number
            if file_name.endswith(".csv"):
                file_type = "CSV"
            elif file_name.endswith(".xml"):
                file_type = "XML"
            elif file_name.endswith(".json"):
                file_type = "JSON"
            insert_file(task_id, region_id, file_name, file_url, file_type)

    # Step 2: Process each aid_folder once all files are downloaded
    for aid_number, folder in aid_files.items():
        print(f"Processing rasters in folder: {folder} for aid number: {aid_number}")
        process_rasters(folder)
        print(f"Rasters processed for aid number: {aid_number}")


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
            return filtered_file

# Function to Insert Task into MySQL
def insert_task(task_id, task_name, start_date, end_date):
    connection = create_db_connection()
    if connection:
        cursor = connection.cursor()
        query = """
        INSERT INTO tasks (task_id, task_name, start_date, end_date, status, submission_time)
        VALUES (%s, %s, %s, %s, 'queued', NOW())
        ON DUPLICATE KEY UPDATE status='queued', submission_time=NOW();
        """
        cursor.execute(query, (task_id, task_name, start_date, end_date))
        connection.commit()
        cursor.close()
        connection.close()
        print("Task inserted into MySQL")

# Function to Insert Downloaded File Info into MySQL
def insert_file(task_id, region_id, file_name, file_path, file_type):
    connection = create_db_connection()
    if connection:
        cursor = connection.cursor()
        query = """
        INSERT INTO files (task_id, region_id, file_name, file_path, file_type, download_time)
        VALUES (%s, %s, %s, %s, %s, NOW());
        """
        cursor.execute(query, (task_id, region_id, file_name, file_path, file_type))
        connection.commit()
        cursor.close()
        connection.close()
        print(f"File {file_name} inserted into MySQL")

"""
# Function to Insert Processed Raster Info into MySQL
def insert_raster(file_id, raster_name, raster_path, raster_type, filtered_path):
    connection = create_db_connection()
    if connection:
        cursor = connection.cursor()
        query = "
        INSERT INTO rasters (file_id, raster_name, raster_path, raster_type, filtered_path, processing_time)
        VALUES (%s, %s, %s, %s, %s, NOW());
        "
        cursor.execute(query, (file_id, raster_name, raster_path, raster_type, filtered_path))
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Raster {raster_name} inserted into MySQL")"""

# Function to Upload to Azure Blob Storage
def upload_to_azure_blob(file_path, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"File uploaded to Azure Blob: {blob_name}")
    return blob_client.url

# Submit task in one go
task_request = build_task_request(product, layers, roi_json, sd, ed)
task_id = submit_task(headers, task_request)
print(f"Task ID: {task_id}")

# Phase 1: Create Directories and Mapping
aid_folder_mapping = {}  # Initialize mapping outside the loop
for idx, row in roi.iterrows():
    print(f"Processing ROI {idx + 1}/{len(roi)}")
    
    # Construct directory path for saving data
    output_folder = os.path.join(pt, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder created: {output_folder}")
    
    # Map aid numbers to output folders
    aid_number = f'aid{str(idx + 1).zfill(4)}'  # Construct aid number
    aid_folder_mapping[aid_number] = output_folder


print("All tasks submitted!")

# Phase 2: Check task statuses periodically (every 30 seconds)
completed_tasks = []

while len(completed_tasks) < 1:  # Change to 1 since we only have one task
    print("Checking task statuses...")
    
    # Check the status of the single task
    status = check_task_status(task_id, headers)
    
    if status:  # Replace True with the actual status string for completion
        print(f"Downloading results for Task ID: {task_id}...")
        download_results(task_id, headers)  # Pass the roi DataFrame for dynamic mapping
        # Process the downloaded rasters
        process_rasters(pt)  # Assuming you want to process all data in the base output directory
        print(f"Rasters processed for Task ID: {task_id}")
        completed_tasks.append(task_id)
    
    if len(completed_tasks) < 1:
        print("Task not completed yet. Waiting for 30 seconds...")
        time.sleep(30)  # Wait for 30 seconds before checking statuses again

print("All tasks completed, results downloaded, and rasters processed.")