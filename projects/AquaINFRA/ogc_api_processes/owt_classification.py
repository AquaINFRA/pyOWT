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
curl -X POST https://${PYSERVER}/processes/owt-classification/execution \
  --header "Content-Type: application/json" \
  --data '{
    "inputs": {
        "input_data_url": "https://raw.githubusercontent.com/bishun945/pyOWT/refs/heads/main/projects/AquaINFRA/data/Rrs_demo_AquaINFRA_hyper.csv",
        "input_option": "csv",
        "sensor": "HYPER",
        "output_option": 1
    }
}'

# Example 2: MSI_S2A, Rrs_demo_AquaINFRA_msi.csv
curl -X POST https://${PYSERVER}/processes/owt-classification/execution \
  --header "Content-Type: application/json" \
  --data '{
    "inputs": {
        "input_data_url": "https://raw.githubusercontent.com/bishun945/pyOWT/refs/heads/main/projects/AquaINFRA/data/Rrs_demo_AquaINFRA_msi.csv",
        "input_option":"csv",
        "sensor":"MSI_S2A",
        "output_option": 1
    }
}'

# Example 3: OLCI_S3A, Rrs_demo_AquaINFRA_olci.csv
curl -X POST https://${PYSERVER}/processes/owt-classification/execution \
  --header "Content-Type: application/json" \
  --data '{
    "inputs": {
        "input_data_url": "https://raw.githubusercontent.com/bishun945/pyOWT/refs/heads/main/projects/AquaINFRA/data/Rrs_demo_AquaINFRA_olci.csv",
        "input_option":"csv",
        "sensor":"OLCI_S3A",
        "output_option": 1
    }
}'

### Reading the example files from server:
# Example 1: HYPER, Rrs_demo_AquaINFRA_hyper.csv
curl -X POST https://${PYSERVER}/processes/owt-classification/execution \
  --header "Content-Type: application/json" \
  --data '{
    "inputs": {
        "input_data_url": "Rrs_demo_AquaINFRA_hyper.csv",
        "input_option":"csv",
        "sensor":"HYPER",
        "output_option": 1
    }
}'

# Example 2: MSI_S2A, Rrs_demo_AquaINFRA_msi.csv
curl -X POST https://${PYSERVER}/processes/owt-classification/execution \
  --header "Content-Type: application/json" \
  --data '{
    "inputs": {
        "input_data_url": "Rrs_demo_AquaINFRA_msi.csv",
        "input_option":"csv",
        "sensor":"MSI_S2A",
        "output_option": 1
    }
}'

# Example 3: OLCI_S3A, Rrs_demo_AquaINFRA_olci.csv
curl -X POST https://${PYSERVER}/processes/owt-classification/execution \
  --header "Content-Type: application/json" \
  --data '{
    "inputs": {
        "input_data_url": "Rrs_demo_AquaINFRA_olci.csv",
        "input_option":"csv",
        "sensor":"OLCI_S3A",
        "output_option": 1
    }
}'

### Extensive output:
curl -X POST https://${PYSERVER}/processes/owt-classification/execution \
  --header "Content-Type: application/json" \
  --data '{
    "inputs": {
        "input_data_url": "Rrs_demo_AquaINFRA_hyper.csv",
        "input_option":"csv",
        "sensor":"HYPER",
        "output_option": 2
    }
}'

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
        self.process_id = self.metadata["id"]
        self.image_name = 'owt-classification-image:20251122'
        config_file_path = os.environ.get('AQUAINFRA_CONFIG_FILE', "./config.json")
        with open(config_file_path) as config_file:
            config = json.load(config_file)
            self.download_dir = config["download_dir"]
            self.download_url = config["download_url"]
            self.docker_executable = config.get("docker_executable", "docker")

    def __repr__(self):
        return f'<OwtClassificationProcessor> {self.name}'

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def execute(self, data, outputs=None):

        #################################
        ### Get user inputs and check ###
        #################################

        input_data_url = data.get('input_data_url', 'Rrs_demo_AquaINFRA_hyper.csv')
        input_option = data.get('input_option')
        sensor = data.get('sensor')
        output_option = data.get('output_option')

        #################################
        ### Input and output          ###
        ### storage/download location ###
        #################################

        # Where to store output data
        output_dir = f'{self.download_dir}/out/{self.process_id}/job_{self.job_id}'
        output_url = f'{self.download_url}/out/{self.process_id}/job_{self.job_id}'
        os.makedirs(output_dir, exist_ok=True)
        LOGGER.debug(f'All results will be stored     in: {output_dir}')
        LOGGER.debug(f'All results will be accessible in: {output_url}')
        downloadfilename = 'owt_classification_output_%s-%s.txt' % (sensor.lower(), self.job_id)
        downloadlink = f'{output_url}/{downloadfilename}'

        ############################
        ### Run docker container ###
        ############################

        returncode, stdout, stderr, user_err_msg = run_docker_container(
            self.docker_executable,
            self.image_name,
            self.job_id,
            input_data_url, 
            input_option, 
            sensor, 
            output_option, 
            output_dir,
            downloadfilename
        )

        if not returncode == 0:
            user_err_msg = "no message" if len(user_err_msg) == 0 else user_err_msg
            err_msg = 'Running docker container failed: %s' % user_err_msg
            raise ProcessorExecuteError(user_msg = err_msg)

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
        image_name,
        job_id,
        input_data_url, 
        input_option, 
        sensor, 
        output_option, 
        output_dir,
        outputFilename
    ):

    LOGGER.debug('Will use this image: %s' % image_name)

    # Create container name
    # Note: Only [a-zA-Z0-9][a-zA-Z0-9_.-] are allowed
    #container_name = "%s_%s" % (image_name.split(':')[0], os.urandom(5).hex())
    container_name = "%s_%s" % (image_name.split(':')[0], job_id)
    LOGGER.debug(f'Prepare running docker (image {image_name}, container: {container_name})')


    # Prepare container command
    container_out = '/app/projects/AquaINFRA/out'

    # Mount volumes and set command
    docker_command = [
        docker_executable, "run", "--rm", "--name", container_name,
        "-v", f"{output_dir}:{container_out}",  # Mount the volume for output
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
        log_docker_outputs(stdout, stderr)
        return result.returncode, stdout, stderr, "no error"

    except subprocess.CalledProcessError as e:
        returncode = e.returncode
        stdout = e.stdout.decode()
        stderr = e.stderr.decode()
        LOGGER.error('Failed running docker container (exit code %s)' % returncode)
        log_docker_outputs(stdout, stderr)
        user_err_msg = get_error_message_from_docker_stderr(stderr)
        return returncode, stdout, stderr, user_err_msg


def log_docker_outputs(stdout, stderr):
    for line in stdout.split('\n'):
        if line:
            LOGGER.debug('Docker stdout: %s' % line)
    for line in stderr.split('\n'):
        if line:
            LOGGER.debug('Docker stderr: %s' % line)


def get_error_message_from_docker_stderr(stderr):
    '''
    We would like to return meaningful messages to users.
    '''
    user_err_msg = ""
    for line in stderr.split('\n'):

        # Skip empty lines:
        if not line:
            continue

        # R error messages may start with the word "Error"
        if "ERROR" in line or "Error" in line:
            #LOGGER.debug('### Found explicit error line: %s' % line.strip())
            if "raise" in line:
                # The traceback contains the line that raises the error, so we
                # would return that to the user...
                # TODO: Do we have to add this to other run_docker too?
                pass
            else:
                user_err_msg += line.strip()

        else:
            #LOGGER.debug('### Do not pass back to user: %s' % line.strip())
            pass

    LOGGER.info('USER ERROR MESSAGE: %s' % user_err_msg)
    return user_err_msg

