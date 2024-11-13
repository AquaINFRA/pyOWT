import logging
import subprocess
import json
import os
import requests
import pandas as pd
from pathlib import Path
import geopandas as gpd
from urllib.parse import urlparse
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import zipfile

'''
Output file name: "data_merged_with_regions-xyz.csv"

For Helcom data, you will need to accepts the conditions first:
https://maps.helcom.fi/website/MADS/download/?id=67d653b1-aad1-4af4-920e-0683af3c4a48

Long/lat are optional

curl --location 'http://localhost:5000/processes/points-att-polygon/execution' \
--header 'Content-Type: application/json' \
--data '{ 
    "inputs": {
        "regions": "https://maps.helcom.fi/arcgis/rest/directories/arcgisoutput/MADS/tools_GPServer/_ags_HELCOM_subbasin_with_coastal_WFD_waterbodies_or_wa.zip",
        "colname_long": "",
        "colname_lat": "",
        "input_data": "https://vm4072.kaj.pouta.csc.fi/ddas/oapif/collections/lva_secchi/items?f=json&limit=3000"
    } 
}'
'''

LOGGER = logging.getLogger(__name__)

script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))

class PointsAttPolygonProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.my_job_id = 'nothing-yet'

    def set_job_id(self, job_id: str):
        self.my_job_id = job_id

    def __repr__(self):
        return f'<PointsAttPolygonProcessor> {self.name}'

    def execute(self, data, outputs=None):

        # Get config
        config_file_path = os.environ.get('DAUGAVA_CONFIG_FILE', "./pygeoapi/process/config.json")
        with open(config_file_path, 'r') as configFile:
            configJSON = json.load(configFile)

        download_dir = configJSON["download_dir"]
        own_url = configJSON["own_url"]
        r_script_dir = configJSON["r_script_dir"]

        # Get user inputs
        in_regions_url = data.get('regions')
        in_dpoints_url = data.get('input_data')
        in_long_col_name = data.get('colname_long')
        in_lat_col_name = data.get('colname_lat')

        # Check:
        if in_regions_url is None:
            raise ProcessorExecuteError('Missing parameter "regions". Please provide a URL to your input study area (as zipped shapefile).')
        if in_dpoints_url is None:
            raise ProcessorExecuteError('Missing parameter "input_data". Please provide a URL to your input table.')
        if in_long_col_name is None:
            raise ProcessorExecuteError('Missing parameter "colname_long". Please provide a column name.')
        if in_lat_col_name is None:
            raise ProcessorExecuteError('Missing parameter "colname_lat". Please provide a column name.')

        shp_file_name = in_regions_url.split("/")[-1]
        shp_dir_zipped = os.path.join(download_dir, "in/shp/")
        shp_file_path = os.path.join(shp_dir_zipped, shp_file_name)

        print(f'Checking whether this file exists: {shp_file_path}')
        if not os.path.exists(shp_dir_zipped):
            try:
                os.makedirs(shp_dir_zipped, exist_ok=True)
                print(f"Directory {shp_dir_zipped} created.")
            except Exception as e:
                raise RuntimeError(f"Directory {shp_dir_zipped} not created (failed): {str(e)}")

        # Download shapefile if it doesn't exist
        if os.path.exists(shp_file_path):
            print(f"File {shp_file_path} already exists. Skipping download.")
        else:
            try:
                # Download the file from the provided URL
                response = requests.get(in_regions_url, stream=True)
                response.raise_for_status()  # Raises an error for failed requests
                with open(shp_file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                print(f"File {shp_file_path} downloaded.")
            except requests.exceptions.RequestException as e:
                raise RuntimeError(f"Download of shapefile failed, reason: {str(e)}")

        # Define the directory to unzip the shapefile
        shp_dir_unzipped = os.path.join(shp_dir_zipped, shp_file_name.replace(".zip", ""))
        
        # Unzip shapefile if it is not already unzipped
        if os.path.isdir(shp_dir_unzipped):
            print(f"Directory {shp_dir_unzipped} already exists. Skipping unzip.")
        else:
            try:
                with zipfile.ZipFile(shp_file_path, 'r') as zip_ref:
                    zip_ref.extractall(shp_dir_unzipped)
                print(f"Unzipped to directory {shp_dir_unzipped}")
            except zipfile.BadZipFile as e:
                print(f"Unzipping {shp_file_path} failed, reason: {str(e)}")
            except Exception as e:
                print(f"Unzipping {shp_file_path} failed, reason: {str(e)}")

        # Define the directory and file paths for in situ data
        in_situ_directory = os.path.join(download_dir, "in/in_situ_data/")
        url_parts_table = in_dpoints_url.split("/")
        table_file_name = url_parts_table[-1]
        table_file_path = os.path.join(in_situ_directory, table_file_name)

        # Ensure the in_situ_data directory exists, create if not
        if not os.path.exists(in_situ_directory):
            try:
                os.makedirs(in_situ_directory)
                print(f"Directory {in_situ_directory} created.")
            except Exception as e:
                print(f"Directory {in_situ_directory} not created: {e}")

        import urllib.request

        # Download Excel/CSV if it doesn't exist
        print(f'Checking if input table file exists: {table_file_path}')
        if not os.path.exists(table_file_path):
            try:
                urllib.request.urlretrieve(in_dpoints_url, table_file_path)
                print(f"File {table_file_path} downloaded.")
            except urllib.error.URLError as e:
                print(f"Download of input table failed, reason: {e}")
        else:
            print(f"File {table_file_path} already exists. Skipping download.")

        # Where to store output data
        

        #LOGGER.debug('R args: %s' % r_args)
        #returncode, stdout, stderr = call_r_script(LOGGER, r_file_name, r_script_dir, r_args)
        #print(stdout)

        #if not returncode == 0:
        #    err_msg = 'R script "%s" failed.' % r_file_name
        #    for line in stderr.split('\n'):
        #        if line.startswith('Error'):
        #            err_msg = 'R script "%s" failed: %s' % (r_file_name, line)
        #    raise ProcessorExecuteError(user_msg = err_msg)

        #else:
        #    # Create download link:
        #    downloadlink = own_url.rstrip('/')+os.sep+downloadfilename

            # Return link to file:
        #    response_object = {
        #        "outputs": {
        ##            "data_merged_with_regions": {
        #                "title": self.metadata['outputs']['data_merged_with_regions']['title'],
        #                "description": self.metadata['outputs']['data_merged_with_regions']['description'],
        #                "href": downloadlink
        #            }
        #        }
        #    }

        #    return 'application/json', response_object
        # Run the Docker container
        downloadfilename = 'data_merged_with_regions-%s.csv' % self.my_job_id
        returncode, stdout, stderr = run_docker_container(shp_dir_unzipped, table_file_path, in_long_col_name, in_lat_col_name, download_dir, downloadfilename)
        if not returncode == 0:
            err_msg = 'R script "%s" failed.' % r_file_name
            for line in stderr.split('\n'):
                if line.startswith('Error'):
                    err_msg = 'R script "%s" failed: %s' % (r_file_name, line)
            raise ProcessorExecuteError(user_msg = err_msg)

        else:
            # Create download link:
            downloadlink = own_url.rstrip('/')+os.sep+downloadfilename

            # Return link to file:
            response_object = {
                "outputs": {
                    "data_merged_with_regions": {
                        "title": self.metadata['outputs']['data_merged_with_regions']['title'],
                        "description": self.metadata['outputs']['data_merged_with_regions']['description'],
                        "href": downloadlink
                    }
                }
            }

            return 'application/json', response_object

def run_docker_container(shp_dir_unzipped, table_file_path, in_long_col_name, in_lat_col_name, download_dir, outputFilename):
    LOGGER.debug('Start running docker container')
    container_name = f'daugava-workflow_{os.urandom(5).hex()}'
    image_name = 'daugava-workflow'
    print(outputFilename)

    # Prepare container command
    # Define paths inside the container
    container_in = '/in'
    container_out = '/out'

    # Define local paths
    local_in = os.path.join(download_dir, "./in")
    local_out = os.path.join(download_dir, "./out")

    # Ensure directories exist
    os.makedirs(local_in, exist_ok=True)
    os.makedirs(local_out, exist_ok=True)

    # Mount volumes and set command
    docker_command = [
        "sudo", "docker", "run", "--rm", "--name", container_name,
        "-v", f"{local_in}:{container_in}",
        "-v", f"{local_out}:{container_out}",
        image_name,
        f"{container_in}/shp/{os.path.basename(shp_dir_unzipped)}",
        f"{container_in}/in_situ_data/{os.path.basename(table_file_path)}",
        in_long_col_name,
        in_lat_col_name,
        f"{container_out}/{outputFilename}"
    ]
    print(docker_command)
    
    # Run container
    try:
        result = subprocess.run(docker_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = result.stdout.decode()
        stderr = result.stderr.decode()
        return result.returncode, stdout, stderr

    except subprocess.CalledProcessError as e:
        return e.returncode, e.stdout.decode(), e.stderr.decode()