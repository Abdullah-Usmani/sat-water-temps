import os
import pytest
import json
from flask import url_for
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app as flask_app

@pytest.fixture
def client():
    flask_app.config['TESTING'] = True
    with flask_app.test_client() as client:
        yield client

def test_index(client):
    res = client.get('/')
    assert res.status_code == 200
    assert b"<html" in res.data

def test_feature_not_found(client):
    res = client.get('/feature/NonExistentFeature')
    assert res.status_code == 404

def test_archive_404(client):
    res = client.get('/feature/NonExistentFeature/archive')
    assert res.status_code == 404

def test_temperature_no_csv(client):
    res = client.get('/feature/NonExistentFeature/temperature')
    assert res.status_code == 404

def test_get_doys_invalid_folder(client):
    res = client.get('/feature/NonExistentFeature/get_dates')
    assert res.status_code == 404

def test_download_tif_invalid(client):
    res = client.get('/download_tif/NonExistentFeature/fake.tif')
    assert res.status_code == 404

def test_download_csv_invalid(client):
    res = client.get('/download_csv/NonExistentFeature/fake.tif')
    assert res.status_code == 404

def test_check_wtoff_invalid(client):
    res = client.get('/feature/NonExistentFeature/check_wtoff/20240101')
    assert res.status_code == 404
