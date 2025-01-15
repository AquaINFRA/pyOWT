import logging
import subprocess
import json
import os
from pathlib import Path
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


LOGGER = logging.getLogger(__name__)


'''

### Reading input csv from any URL:
# Example 1: HYPER, Rrs_demo_AquaINFRA_hyper.csv
curl -X POST "http://localhost:5000/processes/hereon-pyowt/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"input_data_url\": \"https://raw.githubusercontent.com/bishun945/pyOWT/refs/heads/AquaINFRA/data/Rrs_demo_AquaINFRA_hyper.csv\", \"input_option\":\"csv\", \"sensor\":\"HYPER\", \"output_option\": 1}}"

# Example 2: MSI_S2A, Rrs_demo_AquaINFRA_msi.csv
curl -X POST "http://localhost:5000/processes/hereon-pyowt/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"input_data_url\": \"https://raw.githubusercontent.com/bishun945/pyOWT/refs/heads/AquaINFRA/data/Rrs_demo_AquaINFRA_msi.csv\", \"input_option\":\"csv\", \"sensor\":\"MSI_S2A\", \"output_option\": 1}}"

# Example 3: OLCI_S3A, Rrs_demo_AquaINFRA_olci.csv
curl -X POST "http://localhost:5000/processes/hereon-pyowt/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"input_data_url\": \"https://raw.githubusercontent.com/bishun945/pyOWT/refs/heads/AquaINFRA/data/Rrs_demo_AquaINFRA_olci.csv\", \"input_option\":\"csv\", \"sensor\":\"OLCI_S3A\", \"output_option\": 1}}"

### Reading the example files from server:
# Example 1: HYPER, Rrs_demo_AquaINFRA_hyper.csv
curl -X POST "http://localhost:5000/processes/hereon-pyowt/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"input_data_url\": \"Rrs_demo_AquaINFRA_hyper.csv\", \"input_option\":\"csv\", \"sensor\":\"HYPER\", \"output_option\": 1}}"

# Example 2: MSI_S2A, Rrs_demo_AquaINFRA_msi.csv
curl -X POST "http://localhost:5000/processes/hereon-pyowt/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"input_data_url\": \"Rrs_demo_AquaINFRA_msi.csv\", \"input_option\":\"csv\", \"sensor\":\"MSI_S2A\", \"output_option\": 1}}"

# Example 3: OLCI_S3A, Rrs_demo_AquaINFRA_olci.csv
curl -X POST "http://localhost:5000/processes/hereon-pyowt/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"input_data_url\": \"Rrs_demo_AquaINFRA_olci.csv\", \"input_option\":\"csv\", \"sensor\":\"OLCI_S3A\", \"output_option\": 1}}"

### Extensive output:
curl -X POST "http://localhost:5000/processes/hereon-pyowt/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"input_data_url\": \"Rrs_demo_AquaINFRA_hyper.csv\", \"input_option\":\"csv\", \"sensor\":\"HYPER\", \"output_option\": 2}}"

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir!
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class OwtClassificationProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None

    def __repr__(self):
        return f'<OwtClassificationProcessor> {self.name}'

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def execute(self, data, outputs=None):
        config_file_path = os.environ.get('PYOWT_CONFIG_FILE', "./config.json")
        with open(config_file_path) as configFile:
            configJSON = json.load(configFile)

        download_dir = configJSON["download_dir"]
        docker_executable = configJSON.get("docker_executable", "docker")

        input_data_url = data.get('input_data_url', 'Rrs_demo_AquaINFRA_hyper.csv')
        input_option = data.get('input_option')
        sensor = data.get('sensor')
        output_option = data.get('output_option')

        downloadfilename = 'owt_classification_output_%s-%s.txt' % (sensor.lower(), self.job_id)
        #downloadfilepath = configJSON['download_dir']+downloadfilename

        returncode, stdout, stderr = run_docker_container(
            docker_executable,
            input_data_url, 
            input_option, 
            sensor, 
            output_option, 
            download_dir, 
            downloadfilename
        )

        # print Python stderr/stdout to debug log:
        for line in stdout.split("\n"):
            if not len(line.strip()) == 0:
                LOGGER.debug('Python stdout: %s' % line)
        for line in stderr.split("\n"):
            if not len(line.strip()) == 0:
                LOGGER.debug('Python stderr: %s' % line)

        if not returncode == 0:
            err_msg = 'Running docker container failed.'
            for line in stderr.split('\n'):
                if line.startswith('Error'):
                    err_msg = 'Running docker container failed: %s' % (line)
            raise ProcessorExecuteError(user_msg = err_msg)

        # Create download link:
        downloadlink = configJSON['download_dir'] +os.sep+"out"+os.sep+ downloadfilename

        # Build response containing the link
        # TODO Better naming
        response_object = {
            "outputs": {
                "owt_classification": {
                    'title': self.metadata['outputs']["owt_classification"]['title'],
                    'description': self.metadata['outputs']["owt_classification"]['description'],
                    "href": downloadlink
                }
            }
        }
        LOGGER.debug('Built response including link: %s' % response_object)

        return 'application/json', response_object


def run_docker_container(
        docker_executable,
        input_data_url, 
        input_option, 
        sensor, 
        output_option, 
        download_dir, 
        outputFilename
    ):
    LOGGER.debug('Prepare running docker container')
    container_name = f'owt-classification-image_{os.urandom(5).hex()}'
    image_name = 'owt-classification-image'

    # Prepare container command
    container_out = '/app/projects/AquaINFRA/out'

    # Define local paths
    local_out = os.path.join(download_dir, "out")

    # Ensure directories exist
    os.makedirs(local_out, exist_ok=True)

    # Mount volumes and set command
    docker_command = [
        docker_executable, "run", "--rm", "--name", container_name,
        "-v", f"{local_out}:{container_out}",  # Mount the volume for output
        image_name,  # Docker image name
        "--input", input_data_url,  # Input URL
        "--input_option", input_option,  # Input option (e.g., "csv")
        "--sensor", sensor,  # Sensor name (e.g., "HYPER")
        "--output_option", str(output_option),  # Output option (1 or 2)
        "--output", f"{container_out}/{outputFilename}"  # Output file path
    ]

    LOGGER.debug('Docker command: %s' % docker_command)
    
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