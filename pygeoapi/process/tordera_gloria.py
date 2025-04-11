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
        "variable": "flo_out", "water_temp",
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

class TorderaGloriaProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.my_job_id = 'nothing-yet'

    def set_job_id(self, job_id: str):
        self.my_job_id = job_id

    def __repr__(self):
        return f'<TorderaGloriaProcessor> {self.name}'

    def execute(self, data, outputs=None):

        # Get config
        config_file_path = os.environ.get('AQUAINFRA_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as configFile:
            configJSON = json.load(configFile)

        download_dir = "./pygeoapi/process/"
        own_url = "./pygeoapi/process/"
        docker_executable = configJSON.get("docker_executable", "docker")

        # Get user inputs
        in_project = data.get('TextInOut_URL') or "https://raw.githubusercontent.com/AmandaBatlle/AquaINFRA_CaseUse_MedInlandModel/refs/heads/main/example_inputs/project.zip"
        in_parameter_cal = data.get('par_cal') or "https://raw.githubusercontent.com/AmandaBatlle/AquaINFRA_CaseUse_MedInlandModel/refs/heads/main/example_inputs/par_cal.json"
        in_swat_file = data.get('file') or "channel_sd_day"
        in_variable = data.get('variable') or "flo_out,water_temp" 
        in_unit = data.get('unit') or 1
        in_start_date = data.get('start_date') or 20160101
        in_end_date = data.get('end_date') or 20160228
        in_start_date_print = data.get('start_date_print') or 20160115

        print("----------------------------")
        print(in_project)
        print(in_parameter_cal)
        print(in_swat_file)
        print(in_variable)
        print(in_unit)
        print(in_start_date)
        print(in_end_date)
        print(in_start_date_print)
        print("----------------------------------------")

        variables = [var.strip() for var in in_variable.split(',')]
        if isinstance(variables, str):
            variables = [variables]  # Convert to list

        # Create an array of filenames
        downloadfilenames = [
            f'swat_output_file-{self.my_job_id}-{var}.csv' for var in variables
        ]        
        downloadfilenames_string = ','.join(downloadfilenames)

        returncode, stdout, stderr = run_docker_container(
            docker_executable,
            in_project,
            in_parameter_cal,
            in_swat_file,
            in_variable.replace(" ", ""),
            str(in_unit),
            str(in_start_date),
            str(in_end_date),
            str(in_start_date_print),
            download_dir, 
            downloadfilenames_string
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
            outputs = {}
            for var in variables:
                downloadfilename = f'swat_output_file-{self.my_job_id}-{var}.csv'
                downloadlink = own_url.rstrip('/') + os.sep + "out" + os.sep + downloadfilename

                outputs[f"swat_output_file_{var}"] = {
                    "title": self.metadata['outputs']['swat_output_file']['title'],
                    "description": self.metadata['outputs']['swat_output_file']['description'],
                    "href": downloadlink
                }

            response_object = {
                "outputs": outputs
            }

            return 'application/json', response_object

def run_docker_container(
        docker_executable,
        in_project_folder,
        in_calibration_parameter,
        in_swat_file,
        in_variable,
        in_unit,
        in_start_date,
        in_end_date,
        in_start_date_print,
        download_dir, 
        downloadfilenames
    ):
    LOGGER.debug('Prepare running docker container')
    container_name = f'catalunya-tordera-image_{os.urandom(5).hex()}'
    image_name = 'catalunya-tordera-image'

    # Prepare container command

    # Define paths inside the container
    container_in = '/in'
    container_out = '/out/'

    # Define local paths
    local_in = os.path.join(download_dir, "in")
    local_out = os.path.join(download_dir, "out")

    # Ensure directories exist
    os.makedirs(local_in, exist_ok=True)
    os.makedirs(local_out, exist_ok=True)

    script = 'swat_tordera_gloria.R'

    # Mount volumes and set command
    docker_command = [
        "sudo", "docker", "run", "--rm", "--name", container_name,
        "-v", f"{local_in}:{container_in}",
        "-v", f"{local_out}:{container_out}",
        "-e", f"R_SCRIPT={script}",  # Set the R_SCRIPT environment variable
        image_name,
        "--",  # Indicates the end of Docker's internal arguments and the start of the user's arguments
        in_project_folder,
        in_calibration_parameter,
        in_swat_file,
        in_variable,
        in_unit,
        in_start_date,
        in_end_date,
        in_start_date_print,
        container_out,
        downloadfilenames
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
