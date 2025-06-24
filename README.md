# Satellite-Based Water Temperature Monitoring Platform
Prepared by **Group 14**:



| Name | Student ID | Email |
| ----------- | ----------- | ----------- |
| Abdullah Usmani | 20615297 | hcyau1@nottingham.edu.my |
| Darren Raj Manuel Raj | 20491070 | hydm2@nottingham.edu.my |
| Jeptha Ashter Tandri | 20600677 | hcyjt6@nottingham.edu.my |
| Muhammad Ahmad Suhail | 20607733 | hcyms5@nottingham.edu.my |
| Muhammad Syukran Shabaruddin | 20512078 | hcyms4@nottingham.edu.my |


This project is a web-based platform for monitoring real-time water temperature data from Ecostress and Sentinel 2 satellites. The platform visualizes temperature trends across major water bodies in Southeast Asia, allowing users to track water temperature, view historical data, and see predictions for various locations.

## Features

- Real-time temperature monitoring: View water temperature data for dams and rivers in Southeast Asia.
- Interactive Charts: Line charts for historical temperature data and doughnut charts for future temperature predictions.
- Location-Based Data: Select from various water body locations (e.g., Baleh Dam, Murum Dam, Bakun Dam) and see relevant data.
- Map Visualization: Interactive map displaying dam locations with hoverable markers showing dam names.

## Running
Ensure all required libraries are installed before running the code:

***Python***
pip install rasterio
pip install geopandas
pip install pandas
pip install numpy
pip install python-dotenv
pip install requests

***R***
install.packages("terra", dependencies = TRUE)
install.packages("sf", dependencies = TRUE)
install.packages("mgcv", dependencies = TRUE)

The folder of this program should have the following structure
```shell
│   Shape_to_GeoJSON.py
│   GAM4water_0.0.4.R
│   ECO_Converted.py
│   ...
├───polygon
│   │   testbed.qgz
│   ├───test
│   │       site_full_ext_Test.shp
│   │       ...
│   └───corrected
│           site_full_ext_corrected.shp
│           ...
├───tests
│       test_local.py
│       test_script.py
├───ECO
└────ECOraw
```

## User Manual
Before the script can be run, user credentials must be entered. Open the core processing script (ECO_Converted.py) and do the following:
- Navigate to code line 22 and ensure all directory paths are correct
``` shell
roi_test_path = os.path.join(base_dir, "polygon/test/site_full_ext_Test.shp")
raw_path = os.path.join(base_dir, "ECOraw")
filtered_path = os.path.join(base_dir, "ECO")
roi_path = os.path.join(base_dir, "polygon/corrected/site_full_ext_corrected.shp")
log_path = os.path.join(base_dir, "logs")
R_path = os.path.join(base_dir, "GAM4water_0.0.4.R")
```
- Navigate to code line 63 and enter EarthData login credentials
``` shell
# Define Earthdata login credentials (Replace with your actual credentials)
user = ''
password = ''
```
- (Optional) Navigate to code line 76 & 83 to change time frame of data download
``` shell
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
```


## Website Functionality:
Live URL:  https://sat-water-temps-c4590981374e.herokuapp.com


**To find temperature data for a specific lake:**
1. Scroll down in the home page to the world map
2. Pan through the map to the location of specified lake by holding left click and dragging
3. As you hover the cursor above the lake it should highlight green, click it once
4. It will now direct you to the lake page


**Features of the lake page:**
- Temperature statistics & distribution shown on the bottom left
    - To alternate graphs simply use the drop down selector on the top left of the graph tab
    - To alternate temperature units between Kelvin, Celsius, and Fahrenheit click either of the three buttons on the top right of the screen
- Temperature Visualization shown across the water body
    - To alternate color spaces between Relative, Fixed, and Grayscale use the drop down selector in the color scale tab
- Temperature Data across multiple dates
    - To alternate dates of data observation, use the drop down selector on the top center of the screen
- Archive Data, to access the archive data screen simply click the blue “Download All Data” button

**Features of the Archive Page:**
- There is an archive page for each lake containing all temperature data held in our system
- To download data for a certain date, navigate to it’s record and simply click the “Download TIF” or “Download CSV” file depending on the format you would like to use
