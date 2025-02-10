import os
import shutil
import rasterio
import numpy as np
from unittest.mock import patch

# Mock input directory structure
TEST_DATA_DIR = "test_data"
MOCK_AID_NUMBERS = ["aid0001", "aid0002"]
MOCK_FOLDERS = {
    "aid0001": os.path.join(TEST_DATA_DIR, "Magat/lake"),
    "aid0002": os.path.join(TEST_DATA_DIR, "Magat/river"),
}

# Create mock .tif files
def create_mock_tif(file_path, shape=(100, 100)):
    """Creates a dummy .tif raster file for testing."""
    data = np.random.randint(0, 255, shape, dtype=np.uint8)
    with rasterio.open(
        file_path, "w", driver="GTiff",
        height=shape[0], width=shape[1], count=1,
        dtype=np.uint8, crs="+proj=latlong"
    ) as dst:
        dst.write(data, 1)

# Setup test environment
def setup_mock_environment():
    """Sets up the test directories and files."""
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)
    
    for aid, folder in MOCK_FOLDERS.items():
        os.makedirs(folder, exist_ok=True)
        create_mock_tif(os.path.join(folder, "test.tif"))

setup_mock_environment()

# Mock the submit_task and download functions
@patch("ECO_Converted.submit_task", return_value="mock_task_id")
@patch("ECO_Converted.download", return_value=None)
def test_process_rasters(mock_submit, mock_download):
    """Test process_rasters without actually downloading or submitting."""
    from ECO_Converted import process_rasters  # Import your function

    # Run the function with the mock directory and aid-folder mapping
    process_rasters(TEST_DATA_DIR, MOCK_FOLDERS)

    # Assertions or prints to confirm behavior
    print("process_rasters ran successfully with mock data.")

# Run the test
test_process_rasters()
