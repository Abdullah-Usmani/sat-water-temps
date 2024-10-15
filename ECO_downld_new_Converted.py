import os
import requests
import geopandas as gpd
import rasterio
from rasterio.merge import merge
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# Directory paths
print("Setting Directory Paths")
pt = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors/ECOraw/"
output_path = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors/ECO/"
roi_path = "C:/Users/jepht/OneDrive/Desktop/Water Temp Sensors/polygon/site_full_ext.shp"

# Get the current date
print("Setting Dates")
today_date = datetime.now()
today_date_str = today_date.strftime("%Y-%m-%d")
# Subtract one day to get yesterday's date
yesterday_date = today_date - timedelta(days=1)

# Format yesterday's date as 'yyyy-mm-dd'
yesterday_date_str = yesterday_date.strftime("%Y-%m-%d")

# Define start and end dates
sd = yesterday_date_str
ed = today_date_str

# Define Earthdata login credentials (Replace with your actual credentials)
user = "jephthat"
password = "1#Big_Chilli!"

# Get token (API login via requests)
'''print("Getting Token & Logging In")
def get_token(user, password):
    login_url = "https://urs.earthdata.nasa.gov/api/users/token"
    response = requests.post(login_url, auth=(user, password))
    if response.status_code == 200:
        return response.json()['token']
    else:
        raise Exception("Failed to authenticate with Earthdata")

token = get_token(user, password)'''
token_new = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImplcGh0aGF0IiwiZXhwIjoxNzM0MDAyNTMyLCJpYXQiOjE3Mjg4MTg1MzIsImlzcyI6Imh0dHBzOi8vdXJzLmVhcnRoZGF0YS5uYXNhLmdvdiJ9.sly-XS_g6K44wo0JKJ4quriQzVdfPOJsRSavYhww7z7OFttzSdUHeMwDBmezZhLnrk6YiNiSAWtogqRyU8zJSwamMo2ACfTxyoeRZ9EQ_5qtfOptDVUZqww26f95Rrsz58ygLG5tmRlZbUSmXXgLk9fyshuftduyMi6L34LcJrX10HkthgRKUWwVz8NTkoPboHAxGDPQlcKfKeAdN40Q7GWe4sOMeDdYA1AaF0ZeQAxG4aAr0z-7a3rtNwdQa5MvoPIsXMpqaIxM8BLZm82Yu8Wt79PH9Rvt14GnJGZ8LKMpYU8AWxKTmKg7liAw5R6THnOpwsFJxWc2fo5h6eBLNg"

# Products and layers
product = "ECO_L2T_LSTE.002"

def get_layers(product):
    # Mockup function, use API to get actual layers
    return ["LST", "LST_err", "cloud", "Quality_flags"]

layers = get_layers(product)

# Load the area of interest (ROI)
roi = gpd.read_file(roi_path)
print("Loading Regions of Interest")
# Loop through the ROI
for idx, row in roi.iterrows():
    # Create bounding box for ROI
    roi_bbox = row.geometry.bounds
    roi2 = gpd.GeoSeries([row.geometry], crs="EPSG:4326")
    
    # Prepare the task dataframe
    task_df = pd.DataFrame({
        "task": ["polygon"] * len(layers),
        "subtask": ["subtask"] * len(layers),
        "start": [sd] * len(layers),
        "end": [ed] * len(layers),
        "product": [product] * len(layers),
        "layer": layers
    })
    
    # Construct directory path for saving data
    output_folder = os.path.join(pt, row['name'], row['location'])
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder created: {output_folder}")
    # Download the data (mockup, replace with actual API calls)
    print(f"Downloading data to {output_folder}")
    def download_data(output_folder):
        # Assuming data download via API
        pass

    download_data(output_folder)

    # Process raster files
    raster_files = [os.path.join(output_folder, f) for f in os.listdir(output_folder) if f.endswith('.tif')]
    
    for tif_file in raster_files:
        with rasterio.open(tif_file) as src:
            # Perform operations like stacking and filtering based on layers
            pass

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
