import logging
import subprocess
import json
import os
from pathlib import Path
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

'''
How to call this process:

curl -X POST "http://localhost:5000/processes/tordera-gloria/execution" \
  --header "Content-Type: application/json" \
  --data '{
  "inputs":{
        "file": "channel_sd_day", 
        "variable": ["flo_out", "water_temp"],
        "unit": 1, 
        "start_date": 20160101,
        "end_date": 20201231,
        "start_date_print": 20190601
    }
}'

'''

LOGGER = logging.getLogger(__name__)

script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))

class SwatMitgcmConnectionProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.my_job_id = 'nothing-yet'

    def set_job_id(self, job_id: str):
        self.my_job_id = job_id

    def __repr__(self):
        return f'<SwatMitgcmConnectionProcessor> {self.name}'

    def execute(self, data, outputs=None):

        # Get config
        config_file_path = os.environ.get('AQUAINFRA_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as configFile:
            configJSON = json.load(configFile)

        download_dir = "./pygeoapi/process/"
        own_url = "./pygeoapi/process/"
        docker_executable = configJSON.get("docker_executable", "docker")

        # Get user inputs
        in_file1 = data.get('flow_file')
        in_file2 = data.get('temp_file') 

        downloadfilename = 'joinedFile-%s.csv' % self.my_job_id

        returncode, stdout, stderr = run_docker_container(
            docker_executable,
            in_file1,
            in_file2,
            download_dir,
            downloadfilename
        )

        # print R stderr/stdout to debug log:
        for line in stdout.split("\n"):
            if not len(line.strip()) == 0:
                LOGGER.debug('R stdout: %s' % line)

        for line in stderr.split("\n"):
            if not len(line.strip()) == 0:
                LOGGER.debug('R stderr: %s' % line)

        if not returncode == 0:
            very_debug = True # TODO: This is only a temporary solution!
            if very_debug:
                # TODO: This prints all the content to the response. Remove this after debug period!
                err_msg = 'Running docker container failed. Stderr: ' + ' - '.join(stderr.split('\n'))
            else:
                err_msg = 'Running docker container failed.'
                for line in stderr.split('\n'):
                    if line.startswith('Error'): # TODO: Sometimes error messages span several lines.
                        err_msg = 'Running docker container failed: %s' % (line)
                raise ProcessorExecuteError(user_msg = err_msg)

        else:
            downloadlink = own_url.rstrip('/')+os.sep+"out"+os.sep+downloadfilename
            response_object = {
                "outputs": {
                    "joined_file": {
                        "title": self.metadata['outputs']['output_file']['title'],
                        "description": self.metadata['outputs']['output_file']['description'],
                        "href": downloadlink
                    }
                }
            }

            return 'application/json', response_object

def run_docker_container(
        docker_executable,
        in_file1,
        in_file2,
        download_dir, 
        outputFilename
    ):
    LOGGER.debug('Prepare running docker container')
    container_name = f'catalunya-tordera-image_{os.urandom(5).hex()}'
    image_name = 'catalunya-tordera-image'

    # Prepare container command

    # Define paths inside the container
    container_in = '/in'
    container_out = '/out'

    # Define local paths
    local_in = os.path.join(download_dir, "in")
    local_out = os.path.join(download_dir, "out")

    # Ensure directories exist
    os.makedirs(local_in, exist_ok=True)
    os.makedirs(local_out, exist_ok=True)

    script = 'swat_mitgcm_connection.R'

    # Mount volumes and set command
    docker_command = [
        "sudo", "docker", "run", "--rm", "--name", container_name,
        "-v", f"{local_in}:{container_in}",
        "-v", f"{local_out}:{container_out}",
        "-e", f"R_SCRIPT={script}",  # Set the R_SCRIPT environment variable
        image_name,
        "--",  # Indicates the end of Docker's internal arguments and the start of the user's arguments
        in_file1,
        in_file2,
        f"{container_out}/{outputFilename}"
    ]

    LOGGER.debug('Docker command: %s' % docker_command)
    print(docker_command)
    
    # Run container
    try:
        LOGGER.debug('Start running docker container')
        result = subprocess.run(docker_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = result.stdout.decode()
        stderr = result.stderr.decode()
        LOGGER.debug('Finished running docker container')
        return result.returncode, stdout, stderr

    except subprocess.CalledProcessError as e:
        LOGGER.debug('Failed running docker container')
        return e.returncode, e.stdout.decode(), e.stderr.decode()
