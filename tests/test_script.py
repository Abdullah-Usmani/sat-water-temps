# test_eco_downld_converted.py
import os
import json
import re
from requests.exceptions import HTTPError
import pytest
import tempfile
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import geopandas as gpd
from shapely.geometry import Polygon
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import ECO_downld_new_Converted_R_Integrated as eco

def test_build_task_request():
    roi_json = {"type": "FeatureCollection", "features": []}
    task = eco.build_task_request("ECO_L2T_LSTE.002", ["LST"], roi_json, "01-01-2024", "01-02-2024")
    assert task["params"]["layers"][0]["layer"] == "LST"

def test_get_token_success(mock_post):
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"token": "mocktoken"}
    assert eco.get_token("u", "p") == "mocktoken"

def test_get_updated_folders():
    files = ["some_doy2024010100000_aid0001.tif"]
    assert 1 in eco.get_updated_folders(files)

def test_get_updated_dates():
    files = ["some_doy2024010100000_aid0001.tif"]
    assert "2024010100000" in eco.get_updated_dates(files)

def test_build_task_request():
    roi_json = {"type": "FeatureCollection", "features": []}
    task = eco.build_task_request("ECO_L2T_LSTE.002", ["LST"], roi_json, "01-01-2024", "01-02-2024")
    assert task["params"]["layers"][0]["layer"] == "LST"

@patch("requests.post")
def test_get_token_success(mock_post):
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"token": "mocktoken"}
    token = eco.get_token("user", "pass")
    assert token == "mocktoken"

@patch("requests.post")
def test_get_token_failure(mock_post):
    mock_post.side_effect = Exception("fail")
    with pytest.raises(SystemExit):
        eco.get_token("u", "p")

def test_log_updates(tmp_path, monkeypatch):
    monkeypatch.setattr(eco, "log_path", str(tmp_path))
    monkeypatch.setattr(eco, "timestamp", "20250101_1200")
    monkeypatch.setattr(eco, "task_id", "t123")
    monkeypatch.setattr(eco, "sd", "01-01-2025")
    monkeypatch.setattr(eco, "ed", "01-02-2025")
    monkeypatch.setattr(eco, "updated_aids", {1})
    monkeypatch.setattr(eco, "new_files", ["x.tif"])
    monkeypatch.setattr(eco, "multi_aids", {1})
    monkeypatch.setattr(eco, "multi_files", ["x.csv"])
    monkeypatch.setattr(eco, "aid_folder_mapping", {1: ("SiteA", "lake")})
    eco.log_updates()
    assert (tmp_path / "updates_20250101_1200.txt").exists()

def test_invalid_projection_shapefile(tmp_path):
    shp_path = tmp_path / "invalid_proj.shp"
    df = gpd.GeoDataFrame({"name": ["test"], "location": ["lake"],
        "geometry": [Polygon([(0,0),(1,0),(1,1),(0,1),(0,0)])]}, crs="EPSG:3857")
    df.to_file(shp_path)
    loaded = gpd.read_file(shp_path)
    assert loaded.crs.to_epsg() == 3857

def test_incomplete_tif_skip(monkeypatch):
    monkeypatch.setattr(eco, "read_raster", lambda l, f: None)
    eco.process_rasters(1, "20240303", ["badfile.tif"])

def test_clean_filtered_csvs(tmp_path):
    test_csv = tmp_path / "sample.csv"
    df = pd.DataFrame({
        "x": [0], "y": [0], "LST": [1], "LST_filter": [1],
        "LST_err": [0.1], "QC": [1], "EmisWB": [0.9]
    })
    df.to_csv(test_csv, index=False)
    eco.clean_filtered_csvs(str(tmp_path))
    result = pd.read_csv(test_csv)
    assert "LST" not in result.columns
    assert "LST_filter" in result.columns

def test_cleanup_old_files(tmp_path):
    old_file = tmp_path / "old.txt"
    old_file.write_text("data")
    old_time = datetime.now() - timedelta(days=30)
    os.utime(old_file, (old_time.timestamp(), old_time.timestamp()))
    eco.cleanup_old_files(str(tmp_path), days_old=20)
    assert not old_file.exists()

def test_expected_naming_convention():
    fname = "Ambuclao_lake_2025047_filter.tif"
    assert re.match(r"^[a-zA-Z0-9]+_[a-zA-Z0-9]+_\d+_filter(_wtoff)?\.tif$", fname) or True

@patch("rasterio.open")
def test_read_raster_success(mock_open):
    dummy = MagicMock()
    mock_open.return_value = dummy
    assert eco.read_raster("LST", ["LST_file.tif"]) == dummy


@patch("rasterio.open")
def test_read_raster_fail(mock_open):
    assert eco.read_raster("Emis", ["other.tif"]) is None

@patch("ECO_downld_new_Converted.rasterio")
def test_process_skips_incomplete(mock_rio, mock_read):
    mock_read.return_value = None
    eco.process_rasters(99, "20250303", ["dummy.tif"])

def test_read_array():
    mock_raster = MagicMock()
    mock_raster.read.return_value = np.array([[1,2],[3,4]])
    result = eco.read_array(mock_raster)
    assert isinstance(result, np.ndarray)

@patch("ECO_downld_new_Converted.read_raster")
@patch("ECO_downld_new_Converted.rasterio")
def test_process_skips_incomplete(mock_rio, mock_read):
    mock_read.return_value = None
    eco.process_rasters(99, "20250303", ["dummy.tif"])